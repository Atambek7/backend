import asyncio
import os
import re
from uuid import uuid4

import aiohttp
from attr import dataclass
from config import JV_CONTACT_DATA, JV_MANUFACTURER, XL_CONTACT_DATA, XL_MANUFACTURER
from adrf.views import APIView
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError

from main_api.serializers import RetrieveProductSerializer
from main_api.external_serializer import (
    DeleteDataSerializer, 
    ProductDataSerializer,
    PutDataSerializer,
    ActivateProductSerializer
    )
from main_api.services.aftercool_service import (
    AftercoolAuthError,
    AftercoolTransportError,
    AftercoolUpstreamError,
)
from main_api.services.aftercool_price_sync_service import AftercoolPriceSyncService
from main_api.src.controller.kaufland_controller import KauflandController, format_si_dimension
from main_api.src.controller.rest_api_controller import RestApiController
from main_api.src.gpt.gpt_helper import generate_seo
from main_api.src.job_registry import clear_cancel, is_cancelled, register_running_job, unregister_running_job
from main_api.src.logger import log
from config import storefronts

from main_api.models import Unit


try:
    MAX_CONCURRENT_JOBS = max(1, int(os.getenv("MAX_CONCURRENT_JOBS", "3")))
except ValueError:
    MAX_CONCURRENT_JOBS = 3
JOB_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_JOBS)

def get_product_safety_contact(controller):
    jv = {
        "address": "Am Flugplatz 28, 88483 Burgrieden",
        "email_address": "info@jvmoebel.de",
        "name": "AEA GmbH & Co. KG",
        "phone_number": "07392 - 93 78 44 0",
        "url": "https://www.jvmoebel.de/Infos/Kontakt.htm",
    }
    
    xl = {
        "address": "Am Flugplatz 26, 88483 Burgrieden",
        "email_address": "info@xlmoebel.de",
        "name": "XL MOEBEL GmbH",
        "phone_number": "07392 - 93 78 44 5",
        "url": "https://www.xlmoebel.de/xlmoebel-kontakt-zu-unserem-luxusmoebel-store"
    }

    return jv if controller == "jv" else xl


async def _send_aftercool_sync_progress(
    controller: KauflandController,
    job_id: str,
    event: str,
    payload: dict | None = None,
    info: str | None = None,
) -> None:
    payload_with_task = dict(payload or {})
    payload_with_task.setdefault("task", "price_sync")
    await controller._ws_message_send(
        group_name=f"{job_id}_checker",
        job_id=job_id,
        event=event,
        payload=payload_with_task,
        info=info,
    )


async def _run_aftercool_price_sync_job(
    controller_name: str,
    job_id: str,
    username: str | None,
) -> None:
    async with JOB_SEMAPHORE:
        async with aiohttp.ClientSession() as session:
            controllers_by_account = {
                "jv": KauflandController(session, "jv"),
                "xl": KauflandController(session, "xl"),
            }
            controller = (
                controllers_by_account.get(controller_name)
                or controllers_by_account["jv"]
            )
            sync_service = AftercoolPriceSyncService(
                controller=controller,
                controllers_by_account=controllers_by_account,
            )

            async def progress_sender(
                event: str,
                payload: dict | None = None,
                info: str | None = None,
            ) -> None:
                await _send_aftercool_sync_progress(
                    controller,
                    job_id,
                    event,
                    payload=payload,
                    info=info,
                )

            async def cancelled() -> bool:
                return await is_cancelled(job_id)

            await clear_cancel(job_id)
            await register_running_job(job_id)
            try:
                await sync_service.run(
                    job_id=job_id,
                    progress_sender=progress_sender,
                    is_cancelled=cancelled,
                )
            except (
                AftercoolAuthError,
                AftercoolTransportError,
                AftercoolUpstreamError,
            ) as exc:
                await _send_aftercool_sync_progress(
                    controller,
                    job_id,
                    "job_failed",
                    payload={
                        "total": 0,
                        "processed": 0,
                        "success": 0,
                        "failed": 1,
                        "error": 1,
                        "updated": 0,
                        "skipped": 0,
                        "not_found": 0,
                        "failed_eans": [],
                    },
                    info=str(exc),
                )
                log(
                    f"aftercool price sync failed job_id={job_id} controller={controller_name} error={exc}",
                    save=True,
                    level="error",
                )
            except Exception as exc:
                await _send_aftercool_sync_progress(
                    controller,
                    job_id,
                    "job_failed",
                    payload={
                        "total": 0,
                        "processed": 0,
                        "success": 0,
                        "failed": 1,
                        "error": 1,
                        "updated": 0,
                        "skipped": 0,
                        "not_found": 0,
                        "failed_eans": [],
                    },
                    info="unexpected error",
                )
                log(
                    f"aftercool price sync unexpected failure job_id={job_id} controller={controller_name} error={exc}",
                    save=True,
                    level="error",
                )
            finally:
                await unregister_running_job(job_id)


class AftercoolLoginView(APIView):
    permission_classes = [AllowAny]

    async def get(self, request):
        controller_name = os.getenv("AFTERCOOL_TARGET_CONTROLLER", "jv").strip().lower()
        if controller_name not in {"jv", "xl"}:
            controller_name = "jv"

        username = (
            request.user.get_username()
            if getattr(request.user, "is_authenticated", False)
            else None
        )
        job_id = uuid4().hex
        asyncio.create_task(
            _run_aftercool_price_sync_job(
                controller_name=controller_name,
                job_id=job_id,
                username=username,
            )
        )
        return Response(
            {
                "message": "aftercool price sync job started",
                "job_id": job_id,
                "controller": controller_name,
                "ws_task": "checker",
                "progress_ws": f"/ws/checker-progress/{job_id}/",
                "change_log_file": "logs/aftercool_price_changes.csv",
            },
            status=status.HTTP_202_ACCEPTED,
        )


class RetreiveProductView(APIView):
    serializer_class = RetrieveProductSerializer
    permission_classes = [AllowAny]

    async def get(self, request):
        serializer = self.serializer_class(data=request.query_params)
        serializer.is_valid(raise_exception=True)

        ean = serializer.validated_data["ean"]
        controller = serializer.validated_data["controller"]

        async with aiohttp.ClientSession() as session:
            kaufland_controller = KauflandController(session, controller)
            result = await kaufland_controller.get_product_by_ean(ean)

        return Response(result, status=status.HTTP_200_OK)


class PatchProductView(APIView):
    serializer_class = ProductDataSerializer
    permission_classes = [AllowAny]

    async def get(self, request):
        return Response({"message": "ok"}, status=status.HTTP_200_OK)

    async def put(self, request):
        items = request.data if isinstance(request.data, list) else [request.data]
        results = []

        for item in items:
            serializer = self.serializer_class(data=item)
            serializer.is_valid(raise_exception=True)
            validated_data = serializer.validated_data

            ean = validated_data["ean"]
            controller = validated_data["controller"]
            storefront = (
                str(validated_data.get("storefront"))
                if validated_data.get("storefront")
                else "de"
            )

            attributes = {"ean": [ean]}
            if "title" in validated_data:
                attributes["title"] = [validated_data["title"]]
            if "description" in validated_data:
                attributes["description"] = [validated_data["description"]]
            if "picture_urls" in validated_data:
                attributes["picture"] = validated_data["picture_urls"]

            if controller == "jv":
                attributes["manufacturer"] = JV_MANUFACTURER
                attributes["product_safety_contact"] = JV_CONTACT_DATA
            else:
                attributes["manufacturer"] = XL_MANUFACTURER
                attributes["product_safety_contact"] = XL_CONTACT_DATA

            payload = {"ean": [ean], "attributes": attributes}
            response_item = {
                "received_data": validated_data,
                "product_data_put": False,
                "unit_created_or_updated": False,
            }

            async with aiohttp.ClientSession() as client:
                rac = RestApiController(controller=controller, session=client)
                put_response = await rac.send_request(
                    method="PUT",
                    endpoint=f"/product-data?locale={self.to_locale_value(storefront)}",
                    json=payload,
                )
                response_item["product_data_put"] = True
                response_item["product_data_response"] = put_response

                if validated_data.get("price") is not None:
                    raw_price = float(validated_data["price"])
                    price_payload = rac.make_price_payload(
                        storefront=storefront,
                        price=raw_price,
                    )
                    product_response = await rac.send_request(
                        method="GET",
                        endpoint=f"/products/ean/{ean}?storefront={storefront}&embedded=units",
                    )

                    data = product_response.get("data") or {}
                    units = data.get("units") or []
                    unit_id = units[0].get("id_unit") if units else None

                    if unit_id:
                        unit_response = await rac.send_request(
                            method="PATCH",
                            endpoint=f"/units/{unit_id}?storefront={storefront}",
                            json=price_payload,
                        )
                    else:
                        create_payload = {
                            "amount": 20,
                            "handling_time": 28,
                            "listing_price": price_payload["listing_price"],
                            "ean": str(ean),
                            "id_offer": f"{ean}",
                            "condition": "NEW",
                        }
                        unit_response = await rac.send_request(
                            method="POST",
                            endpoint=f"/units?storefront={storefront}&embedded=eco_participation",
                            json=create_payload,
                        )

                    response_item["unit_created_or_updated"] = True
                    response_item["unit_response"] = unit_response

            results.append(response_item)

        if isinstance(request.data, list):
            return Response({"results": results}, status=status.HTTP_200_OK)
        return Response(results[0], status=status.HTTP_200_OK)
    
    async def patch(self, request):
        items: list = request.data if isinstance(request.data, list) else [request.data]
        results = []

        query_controller = request.query_params.get("controller")
        query_storefront = request.query_params.get("storefront")

        for raw_item in items:
            item = dict(raw_item)
            if not item.get("controller") and query_controller:
                item["controller"] = query_controller
            if not item.get("storefront") and query_storefront:
                item["storefront"] = query_storefront
            elif not item.get("storefront"):
                item["storefront"] = "de"

            serializer = self.serializer_class(data=item)
            serializer.is_valid(raise_exception=True)

            validated_data = serializer.validated_data
            ean = validated_data["ean"]
            controller = validated_data["controller"]
            storefront = str(validated_data.get("storefront") or "de")

            attributes = {}
            if "title" in validated_data:
                attributes["title"] = [validated_data["title"]]
                
            if "description" in validated_data:
                attributes["description"] = [validated_data["description"]]
                
            if "picture_urls" in validated_data:
                attributes["picture"] = validated_data["picture_urls"]

            payload = {"ean": [ean], "attributes": attributes}

            response_item = {
                "ean": ean,
                "controller": controller,
                "storefront": storefront,
                "received_data": validated_data,
                "product_data_updated": False,
                "price_updated": False,
            }

            async with aiohttp.ClientSession() as client:
                rac = RestApiController(controller=controller, session=client)

                if attributes:
                    api_response = await rac.send_request(
                        method="PATCH",
                        endpoint=f"/product-data?locale={self.to_locale_value(storefront)}",
                        json=payload,
                    )
                    response_item["product_data_updated"] = True
                    response_item["product_data_response"] = api_response

                price = validated_data.get("price")
                if price is not None:
                    unit_id = validated_data.get("unit_id")
                    if not unit_id:
                        try:
                            res = await rac.send_request(
                                method="GET",
                                endpoint=f"/units?storefront={storefront}&id_offer={ean}",
                            )
                            units = res.get("data") or []
                            if not units:
                                res = await rac.send_request(
                                    method="GET",
                                    endpoint=f"/units?storefront={storefront}&ean={ean}",
                                )
                                units = res.get("data") or []
                            if not units:
                                res = await rac.send_request(
                                    method="GET",
                                    endpoint=f"/products/ean/{ean}?storefront={storefront}&embedded=units",
                                )
                                units = (res.get("data") or {}).get("units") or []
                            if units:
                                unit_id = units[0].get("id_unit")
                        except Exception as e:
                            response_item["unit_lookup_error"] = str(e)

                    if unit_id:
                        unit_endpoint = f"/units/{unit_id}?storefront={storefront}"
                        price_payload = rac.make_price_payload(
                            storefront=storefront,
                            price=float(price),
                        )
                        try:
                            price_response = await rac.send_request(
                                method="PATCH",
                                endpoint=unit_endpoint,
                                json=price_payload,
                            )
                            response_item["unit_id"] = unit_id
                            response_item["price_updated"] = True
                            response_item["price_response"] = price_response
                            response_item["applied_price"] = price_payload
                        except Exception as exc:
                            return Response(
                                f"Failed to update price: {exc}, json = {price_payload}",
                                status=status.HTTP_400_BAD_REQUEST,
                            )
                    else:
                        response_item["price_updated"] = False
                        response_item["error"] = f"Unit not found for EAN {ean} on storefront {storefront}"

            results.append(response_item)

        if isinstance(request.data, list):
            return Response({"results": results}, status=status.HTTP_200_OK)
        return Response({"results": results, **(results[0] if results else {})}, status=status.HTTP_200_OK)
        

    
    def to_locale_value(self, storefront: str) -> str:
        if storefront == "de":
            return "de-DE"
        if storefront == "cz":
            return "cs-CZ"
        if storefront == "sk":
            return "sk-SK"
        if storefront == "at":
            return "de-AT"
        if storefront == "pl":
            return "pl-PL"
        if storefront == "fr":
            return "fr-FR"
        if storefront == "it":
            return "it-IT"
        if storefront == "nl":
            return "nl-NL"
        if storefront == "es":
            return "es-ES"
        return "de-DE"


class DeleteProductView(APIView):
    serializer_class = DeleteDataSerializer
    permission_classes = [AllowAny]
    
    async def delete(self, request, ean: str):
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        validated_data = serializer.validated_data
        
        controller = validated_data["controller"]
        
        locales = [
            "de-DE",
            "cs-CZ",
            "sk-SK",
            "de-AT",
            "pl-PL",
            "fr-FR",
            "it-IT",
            "nl-NL",
            "es-ES",
        ]
        
        async with aiohttp.ClientSession() as client:
            rac = RestApiController(controller=controller, session=client)
            result = {"product_data": {}, "units": {}}
            for locale in locales:
                delete_response = {}
                try:
                    delete_response = await rac.send_request(
                        method="DELETE",
                        endpoint=f"/product-data/{ean}?locale={locale}",
                    )
                    result["product_data"][locale] = delete_response.get("data")
                except Exception as exc:
                    result["product_data"][locale] = f"Failed to delete: {exc}"

            for storefront in storefronts:
                try:
                    inv_res = await rac.send_request(
                        method="GET",
                        endpoint=f"/units?storefront={storefront}&id_offer={ean}",
                    )
                    unit_list = inv_res.get("data") if isinstance(inv_res, dict) else None
                    if not unit_list:
                        inv_res = await rac.send_request(
                            method="GET",
                            endpoint=f"/units?storefront={storefront}&ean={ean}",
                        )
                        unit_list = inv_res.get("data") if isinstance(inv_res, dict) else None

                    if unit_list and isinstance(unit_list, list):
                        for u in unit_list:
                            u_id = u.get("id_unit")
                            if u_id:
                                try:
                                    await rac.send_request(
                                        method="DELETE",
                                        endpoint=f"/units/{u_id}?storefront={storefront}",
                                    )
                                    result["units"][f"{storefront}:{u_id}"] = "deleted"
                                except Exception as u_err:
                                    result["units"][f"{storefront}:{u_id}"] = f"Failed to delete: {u_err}"
                except Exception as exc:
                    result["units"][storefront] = f"Lookup failed: {exc}"

            return Response({"result": result}, status=status.HTTP_200_OK)
        

class PutProductView(APIView):
    serializer_class = PutDataSerializer
    permission_classes = [AllowAny]
    
    async def get(self, request):
        return Response({"method": "GET", "message": "ok"}, status=status.HTTP_200_OK)

    @staticmethod
    def _material_value(material: str) -> str:
        value = str(material).strip()
        if value in ["Stoff", "Textil"]:
            return "Synthetic"
        return value

    @staticmethod
    def _parse_existing_offer_error(error: Exception) -> bool:
        return "already exists" in str(error).lower()

    async def _select_category(
        self,
        rac: RestApiController,
        *,
        title: str,
        description: str,
        manufacturer: str,
        price: float,
    ) -> tuple[str, str, dict]:
        try:
            numeric_price = float(price or 0)
            price_cents = int(round(numeric_price * 100)) if numeric_price > 0 else 10000
        except (ValueError, TypeError):
            price_cents = 10000

        category_selector_data = {
            "item": {
                "title": title,
                "description": description,
                "manufacturer": manufacturer,
            },
            "price": price_cents,
        }
        response = await rac.send_request(
            "POST",
            "/categories/decide?storefront=de&locale=de-DE",
            json=category_selector_data,
        )
        categories = response.get("data") if isinstance(response, dict) else None
        if not categories:
            raise ValueError("Kaufland did not return category suggestions")

        category = categories[0]
        category_id = category.get("id_category")
        category_name = category.get("name")
        if not category_id or not category_name:
            raise ValueError(f"Invalid Kaufland category response: {response}")

        return str(category_id), str(category_name), response

    async def _fetch_unit_id(
        self,
        rac: RestApiController,
        *,
        ean: str,
        storefront: str,
        id_offer: str | None = None,
    ) -> str | None:
        lookup_id = id_offer or ean
        try:
            inv_res = await rac.send_request(
                method="GET",
                endpoint=f"/units?storefront={storefront}&id_offer={lookup_id}",
            )
            data = inv_res.get("data") if isinstance(inv_res, dict) else None
            if data and isinstance(data, list) and data[0].get("id_unit"):
                return str(data[0]["id_unit"])
        except Exception:
            pass

        try:
            inv_res_ean = await rac.send_request(
                method="GET",
                endpoint=f"/units?storefront={storefront}&ean={ean}",
            )
            data_ean = inv_res_ean.get("data") if isinstance(inv_res_ean, dict) else None
            if data_ean and isinstance(data_ean, list) and data_ean[0].get("id_unit"):
                return str(data_ean[0]["id_unit"])
        except Exception:
            pass

        try:
            response = await rac.send_request(
                method="GET",
                endpoint=f"/products/ean/{ean}?storefront={storefront}&embedded=units",
            )
            data = response.get("data") if isinstance(response, dict) else None
            units = data.get("units") if isinstance(data, dict) else None
            if units:
                return str(units[0].get("id_unit"))
        except Exception:
            pass

        return None

    async def _create_or_update_unit(
        self,
        rac: RestApiController,
        *,
        ean: str,
        storefront: str,
        price: float,
        delivery: int,
        amount: int,
        id_offer: str,
    ) -> dict:
        price_payload = rac.make_price_payload(storefront=storefront, price=price)
        unit_data = {
            "amount": amount,
            "handling_time": delivery,
            "listing_price": price_payload["listing_price"],
            "minimum_price": price_payload["minimum_price"],
            "ean": str(ean),
            "id_offer": str(id_offer),
            "condition": "NEW",
        }

        try:
            response = await rac.send_request(
                "POST",
                f"/units?storefront={storefront}&embedded=eco_participation",
                json=unit_data,
            )
            return {
                "status": "created",
                "request": unit_data,
                "response": response,
            }
        except Exception as exc:
            if not self._parse_existing_offer_error(exc):
                return {
                    "status": "failed",
                    "request": unit_data,
                    "error": str(exc),
                }

            unit_id = await self._fetch_unit_id(
                rac,
                ean=ean,
                storefront=storefront,
                id_offer=id_offer,
            )
            if not unit_id:
                return {
                    "status": "failed",
                    "request": unit_data,
                    "error": "Offer already exists, but unit id was not found",
                }

            patch_response = await rac.send_request(
                "PATCH",
                f"/units/{unit_id}?storefront={storefront}",
                json=price_payload,
            )
            return {
                "status": "updated",
                "unit_id": unit_id,
                "request": price_payload,
                "response": patch_response,
            }

    async def _upsert_product(self, request):
        serializer = self.serializer_class(data=request.data)
        if not serializer.is_valid():
            return Response(
                {
                    "success": False,
                    "message": "Product validation failed before Kaufland submission",
                    "errors": serializer.errors,
                    "ean": request.data.get("ean"),
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validated_data = serializer.validated_data
        controller = validated_data["controller"]
        pictures = validated_data["picture"]
        upload_item = {
            "article": validated_data["title"],
            "price": float(validated_data["price"]),
            "I_stammartikel": "",
            "pic_main": pictures[0],
            "pics": pictures[1:],
            "ean": str(validated_data["ean"]),
            "fabric": "",
            "size": validated_data["size"],
            "color": validated_data["color"],
            "height": validated_data["height"],
            "length": validated_data["length"],
            "width": validated_data["width"],
            "material": validated_data["material"],
            "material_composition": validated_data.get("material_composition", ""),
            "number_of_units": "",
            "amount": validated_data["amount"],
            "id_offer": validated_data["id_offer"],
            "storefronts": validated_data["storefronts"],
        }

        async with aiohttp.ClientSession() as session:
            kaufland_controller = KauflandController(session, controller)
            uploaded = await kaufland_controller.upload_single_product(upload_item)

        if uploaded:
            return Response(
                {
                    "success": True,
                    "message": "product uploaded",
                    "ean": upload_item["ean"],
                    "controller": controller,
                },
                status=status.HTTP_201_CREATED,
            )

        return Response(
            {
                "success": False,
                "message": "product upload failed",
                "reason": kaufland_controller.last_upload_error,
                "ean": upload_item["ean"],
                "controller": controller,
            },
            status=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )

    async def post(self, request):
        return await self._upsert_product(request)

    async def put(self, request):
        return await self._upsert_product(request)


class DeactivateProductView(APIView):
    serializer_class = DeleteDataSerializer
    permission_classes = [AllowAny]
    
    
    async def delete(self, request, ean: str):
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)

        controller = serializer.validated_data["controller"]
        log(f"deactivate product start ean={ean} controller={controller}")

        result = {
            "response": [],
            "failed_storefronts": [],
        }

        async with aiohttp.ClientSession() as client:
            rac = RestApiController(
                controller=controller,
                session=client,
            )
            for storefront in storefronts:
                try:
                    log(
                        f"deactivate product fetch units ean={ean} controller={controller} storefront={storefront}"
                    )
                    get_response = await rac.send_request(
                        method="GET",
                        endpoint=(
                            f"/products/ean/{ean}"
                            f"?storefront={storefront}&embedded=units"
                        ),
                    )
                    data = get_response.get("data") or {}
                    units = data.get("units") or []

                    if "units" not in data:
                        log(
                            f"deactivate product units key missing ean={ean} controller={controller} storefront={storefront} data_keys={sorted(data.keys())}",
                            level="warning",
                        )

                    if not units:
                        log(
                            f"deactivate product units not found ean={ean} controller={controller} storefront={storefront}",
                            level="warning",
                        )
                        result["failed_storefronts"].append({
                            "storefront": storefront,
                            "error": "Units not found",
                        })
                        continue

                    unit_data = units[0]
                    unit_id = unit_data["id_unit"]
                    log(
                        f"deactivate product delete unit ean={ean} controller={controller} storefront={storefront} unit_id={unit_id}"
                    )

                    delete_response = await rac.send_request(
                        method="DELETE",
                        endpoint=f"/units/{unit_id}?storefront={storefront}",
                    )

                    if delete_response["status"] != 204:
                        log(
                            f"deactivate product unexpected status ean={ean} controller={controller} storefront={storefront} unit_id={unit_id} status={delete_response.get('status')}",
                            level="warning",
                        )
                        result["failed_storefronts"].append({
                            "storefront": storefront,
                            "error": f"Unexpected status",
                        })
                        continue

                    await Unit.objects.aupdate_or_create(
                        ean=ean,
                        controller=controller,
                        storefront=storefront,
                        defaults={
                            "unit": unit_data
                        }
                    )

                    result["response"].append({
                        "storefront": storefront,
                        "unit": unit_data,
                    })
                    log(
                        f"deactivate product success ean={ean} controller={controller} storefront={storefront} unit_id={unit_id}"
                    )

                except Exception as error:
                    log(
                        f"deactivate product failed ean={ean} controller={controller} storefront={storefront} error_type={type(error).__name__} error={error}",
                        level="error",
                    )
                    result["failed_storefronts"].append({
                        "storefront": storefront,
                        "error": str(error),
                    })
            log(
                f"deactivate product finished ean={ean} controller={controller} success_count={len(result['response'])} failed_count={len(result['failed_storefronts'])}"
            )
            return Response(result)


class ActivateProductView(APIView):
    serializer_class = ActivateProductSerializer
    permission_classes = [AllowAny]

    @staticmethod
    def _normalize_name(value):
        return str(value or "").strip().casefold()

    @staticmethod
    def _get_response_items(response: dict) -> list[dict]:
        data = response.get("data") if isinstance(response, dict) else None
        return data if isinstance(data, list) else []

    async def _fetch_all(self, rac: RestApiController, endpoint: str) -> list[dict]:
        items = []
        limit = 30
        offset = 0

        while True:
            separator = "&" if "?" in endpoint else "?"
            response = await rac.send_request(
                method="GET",
                endpoint=f"{endpoint}{separator}limit={limit}&offset={offset}",
            )
            chunk = self._get_response_items(response)
            items.extend(chunk)

            if len(chunk) < limit:
                return items
            offset += limit

    async def _fetch_warehouse_map(self, rac: RestApiController) -> dict:
        warehouses = await self._fetch_all(rac, "/warehouses")
        by_name = {
            self._normalize_name(item.get("name")): item.get("id_warehouse")
            for item in warehouses
            if item.get("name") and item.get("id_warehouse")
        }
        default_id = next(
            (
                item.get("id_warehouse")
                for item in warehouses
                if item.get("is_default") and item.get("id_warehouse")
            ),
            None,
        )
        return {"by_name": by_name, "default_id": default_id}

    async def _fetch_shipping_group_map(
        self, rac: RestApiController, storefront: str
    ) -> dict:
        groups = await self._fetch_all(rac, f"/shipping-groups?storefront={storefront}")
        by_name = {
            self._normalize_name(item.get("name")): item.get("id_shipping_group")
            for item in groups
            if item.get("name") and item.get("id_shipping_group")
        }
        default_id = next(
            (
                item.get("id_shipping_group")
                for item in groups
                if item.get("is_default") and item.get("id_shipping_group")
            ),
            None,
        )
        return {"by_name": by_name, "default_id": default_id}

    def _resolve_warehouse_id(self, unit: dict, warehouse_map: dict):
        existing_id = unit.get("id_warehouse")
        if existing_id:
            return existing_id

        warehouse_name = self._normalize_name(unit.get("warehouse"))
        if warehouse_name:
            warehouse_id = warehouse_map["by_name"].get(warehouse_name)
            if warehouse_id:
                return warehouse_id

        return warehouse_map["default_id"]

    def _resolve_shipping_group_id(self, unit: dict, shipping_group_map: dict):
        existing_id = unit.get("id_shipping_group")
        if existing_id:
            return existing_id

        shipping_group_name = self._normalize_name(unit.get("shipping_group"))
        if shipping_group_name:
            shipping_group_id = shipping_group_map["by_name"].get(shipping_group_name)
            if shipping_group_id:
                return shipping_group_id

        return shipping_group_map["default_id"]

    def build_payload(
        self,
        unit: dict,
        ean: str,
        warehouse_id=None,
        shipping_group_id=None,
    ):
        minimum_price = unit.get("minimum_price")

        payload = {
            "amount": unit["amount"],
            "handling_time": unit.get("delivery_time_min", 0),
            "listing_price": unit["price"],
            "minimum_price": minimum_price if minimum_price is not None else unit["price"],
            "note": unit.get("note", ""),
            "ean": ean,
            "id_product": unit["id_product"],
            "id_offer": str(unit.get("id_offer") or ean),
            "condition": unit["condition"],
            "vat_indicator": "standard_rate",
        }

        if warehouse_id:
            payload["id_warehouse"] = warehouse_id
        if shipping_group_id:
            payload["id_shipping_group"] = shipping_group_id

        return payload
    
    async def post(self, request, ean: str):
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)

        validated_data = serializer.validated_data
        controller = validated_data["controller"]
        log(f"activate product start ean={ean} controller={controller}")

        result = {"response": [], "failed_storefronts": []}

        async with aiohttp.ClientSession() as client:
            rac = RestApiController(controller=controller, session=client)
            warehouse_map = await self._fetch_warehouse_map(rac)
            shipping_group_maps = {}
            
            # Retrieve unit by id from table
            for storefront in storefronts:
                try:
                    log(
                        f"activate product fetch saved unit ean={ean} controller={controller} storefront={storefront}"
                    )
                    saved_unit = await Unit.objects.aget(
                        ean=ean,
                        storefront=storefront,
                        controller=controller
                    )
                    
                    unit_data = saved_unit.unit
                    shipping_group_maps[storefront] = (
                        shipping_group_maps.get(storefront)
                        or await self._fetch_shipping_group_map(rac, storefront)
                    )
                    warehouse_id = self._resolve_warehouse_id(
                        unit_data, warehouse_map
                    )
                    shipping_group_id = self._resolve_shipping_group_id(
                        unit_data, shipping_group_maps[storefront]
                    )

                    if not warehouse_id:
                        raise ValueError(
                            f"Warehouse not found for saved warehouse={unit_data.get('warehouse')!r}"
                        )
                    if not shipping_group_id:
                        raise ValueError(
                            f"Shipping group not found for saved shipping_group={unit_data.get('shipping_group')!r}"
                        )

                    payload = self.build_payload(
                        unit_data,
                        ean,
                        warehouse_id=warehouse_id,
                        shipping_group_id=shipping_group_id,
                    )
                    log(
                        f"activate product create unit ean={ean} controller={controller} storefront={storefront} old_unit_id={unit_data.get('id_unit')} warehouse={unit_data.get('warehouse')} id_warehouse={warehouse_id} shipping_group={unit_data.get('shipping_group')} id_shipping_group={shipping_group_id}"
                    )
                    
                    response = await rac.send_request(
                        method="POST",
                        endpoint=f"/units?storefront={storefront}&embedded=eco_participation",
                        json=payload,
                    )
                    result["response"].append({
                        "storefront": storefront,
                        "response": response,
                    })
                    log(    
                        f"activate product success ean={ean} controller={controller} storefront={storefront} old_unit_id={unit_data.get('id_unit')}"
                    )
                except Exception as error:
                    log(
                        f"activate product failed ean={ean} controller={controller} storefront={storefront} error_type={type(error).__name__} error={error}",
                        level="error",
                    )

                    result["failed_storefronts"].append({
                        "storefront": storefront,
                        "error": str(error)
                    })
        log(
            f"activate product finished ean={ean} controller={controller} success_count={len(result['response'])} failed_count={len(result['failed_storefronts'])}"
        )
        return Response(result)


class ProductStatusView(APIView):
    """
    Эндпоинт для проверки статуса публикации и валидации товара на Kaufland:
    GET /api/products/status/<str:ean>/?controller=jv[&storefront=de]
    """
    permission_classes = [AllowAny]

    async def get(self, request, ean: str = None):
        clean_ean = str(ean or request.query_params.get("ean") or "").strip()
        if not clean_ean:
            return Response(
                {
                    "success": False,
                    "message": "Parameter 'ean' is required (in URL path or as query parameter ?ean=...).",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        controller = request.query_params.get("controller", "").strip().lower()
        if not controller or controller not in ["jv", "xl"]:
            return Response(
                {
                    "success": False,
                    "message": "Query parameter 'controller' is required and must be 'jv' or 'xl'.",
                    "ean": clean_ean,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        storefront = request.query_params.get("storefront", "de").strip().lower()

        async with aiohttp.ClientSession() as session:
            kaufland_controller = KauflandController(session, controller)
            status_data = await kaufland_controller.get_product_status(clean_ean, storefront=storefront)

        http_status = status.HTTP_200_OK if status_data.get("status") != "NOT_FOUND" else status.HTTP_404_NOT_FOUND
        return Response(status_data, status=http_status)

