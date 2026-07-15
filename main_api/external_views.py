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

        for item in items:
            serializer = self.serializer_class(data=item)
            serializer.is_valid(raise_exception=True)

            validated_data = serializer.validated_data
            ean = validated_data["ean"]
            controller = validated_data["controller"]

            attributes = {}
            if "title" in validated_data:
                attributes["title"] = [validated_data["title"]]
                
            if "description" in validated_data:
                attributes["description"] = [validated_data["description"]]
                
            if "picture_urls" in validated_data:
                attributes["picture"] = validated_data["picture_urls"]

            storefront = str(validated_data.get("storefront")) if validated_data.get("storefront") else "de"

            payload = {"ean": [ean], "attributes": attributes}

            response_item = {
                "received_data": validated_data,
                "product_data_updated": False,
                "price_updated": False,
            }

            if attributes:
                async with aiohttp.ClientSession() as client:
                    rac = RestApiController(controller=controller, session=client)
                    api_response = await rac.send_request(
                        method="PATCH",
                        endpoint=f"/product-data?locale={self.to_locale_value(storefront)}",
                        json=payload,
                    )
                response_item["product_data_updated"] = True
                response_item["product_data_response"] = api_response

            if validated_data.get("unit_id"):
                unit_id: str = validated_data.get("unit_id")
                price = validated_data.get("price")

                if unit_id and price is not None:
                    unit_endpoint = (
                        f"/units/{unit_id}?storefront={storefront}"
                    )
                    async with aiohttp.ClientSession() as client:
                        rac = RestApiController(controller=controller, session=client)
                        payload = rac.make_price_payload(
                            storefront=storefront,
                            price=float(price),
                        )

                        try:
                            price_response = await rac.send_request(
                                method="PATCH",
                                endpoint=unit_endpoint,
                                json=payload,
                            )

                        except Exception as exc:
                            return Response(f"Failed to update price: {exc}, json = {payload}", status=status.HTTP_400_BAD_REQUEST)

                    response_item["price_updated"] = True
                    response_item["price_response"] = price_response

                results.append(response_item)
        return Response({"results": results}, status=status.HTTP_200_OK)
        

    
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
        return "de-DE"


class DeleteProductView(APIView):
    serializer_class = DeleteDataSerializer
    permission_classes = [AllowAny]
    
    async def delete(self, request, ean: str):
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        validated_data = serializer.validated_data
        
        controller = validated_data["controller"]
        
        locales = ["de-DE", "cs-CZ", "sk-SK", "de-AT", "pl-PL", "fr-FR", "it-IT"]
        
        async with aiohttp.ClientSession() as client:
            rac = RestApiController(controller=controller, session=client)
            result = {}
            for locale in locales:
                delete_response = {}
                try:
                    delete_response = await rac.send_request(
                        method="DELETE",
                        endpoint=f"/product-data/{ean}?locale={locale}",
                    )
                    result[locale] = delete_response.get("data")
                except Exception as exc:
                    result[locale] = f"Failed to delete: {exc}"
                    
            return Response({"result": result}, status=status.HTTP_200_OK)
        

class PutProductView(APIView):
    serializer_class = PutDataSerializer
    permission_classes = [AllowAny]
    
    async def get(self, request):
        return Response({"method": "GET", "message": "ok"}, status=status.HTTP_200_OK)
    
    
    async def put(self, request):
        serializer = self.serializer_class(data=request.data)
        try:
            serializer.is_valid(raise_exception=True)
        except ValidationError as ve:
            return Response({"error": str(ve)}, status=status.HTTP_400_BAD_REQUEST)
        
        validated_data = serializer.validated_data
        
        result = {
            "data": {},
            "errors": {},
            "success": False,
        }
        
        # Main body
        ean = validated_data["ean"]
        controller = validated_data["controller"]
        
        price = int(validated_data["price"])
        size = validated_data["size"]
        color = validated_data["color"]
        material = validated_data["material"]
        delivery = validated_data["delivery"]
        
        height = validated_data["height"]
        length = validated_data["length"]
        width = validated_data["width"]
        
        # Attributes
        attributes = {
            "title": [validated_data["title"]],
            "description": [validated_data["description"]],
            "picture": validated_data["picture"],
            "product_safety_contact": get_product_safety_contact(controller) 
        }
        
        # Short description generation
        try: 
            seo_list = await generate_seo(
                article=attributes["title"][0],
                size = size,
                color = color,
                material = material
            )
        except Exception as e:
            result["errors"] = {"gtp_error": str(e)}
            seo_list = []
            
        attributes["short_description"] = seo_list
        
        category_selector_data = {
            "item": {
                "title": attributes["title"][0],
                "description": attributes["description"][0],
                "manufacturer": "AEA GmbH & Co. KG" if controller == "jv" else "XL MOEBEL GmbH",
            },
            "price": price
        }
        
        # Category selector
        async with aiohttp.ClientSession() as client:
            try:
                rac = RestApiController(controller, client)
                endpoint = "/categories/decide?storefront=de&locale=de-DE"
                response = await rac.send_request("POST", endpoint, json=category_selector_data)
            except Exception as e:
                print(f"Exception {str(e)} happend: {type(e).__name__}, args: {e.args}")
                return Response({"success": False, "data": {"category": {"status": "failed"}}, "errors": str(e)})
            
            category_name = response["data"][0]["name"]
            category_id = response["data"][0]["id_category"]
            result["data"]["category"] = category_name
            result["success"] = True

        attributes["category"] = [category_name]
        attributes["category_detail"] = {
            "id": category_id,
            "title": category_name,
            "name": category_name,
        }
        formatted_width = format_si_dimension(width)
        formatted_length = format_si_dimension(length)
        formatted_height = format_si_dimension(height)
        if formatted_width:
            attributes["width"] = formatted_width
        if formatted_length:
            attributes["length"] = formatted_length
        if formatted_height:
            attributes["height"] = formatted_height
        attributes["material"] = material
        attributes["colour"] = color
        attributes["size"] = size
        attributes["manufacturer_number"] = "JVM" + str(ean)
        
        # Product data creation
        product_data = {
            "ean": ean,
            "attributes": attributes,
        }
        async with aiohttp.ClientSession() as client:
            try:
                rac = RestApiController(controller, client)
                endpoint = "/product-data?locale=de-DE"
                response = await rac.send_request("PUT", endpoint, json=product_data)

            except Exception as e:
                return Response({"success": False, "data": {"product": {"status": "failed"}}, "errors": str(e)})
            
        result["data"]["product-data"] = response
        result["success"] = True
        
        # Unit creation
        unit_data = {
            "amount": 20,
            "handling_time": delivery,
            "listing_price": price,
            "minimum_price": price,
            "ean": ean,
            "id_offer": ean,
            "condition": "NEW"
        }
        async with aiohttp.ClientSession() as client:
            rac = RestApiController(controller, client)
            for storefront in storefronts:
                try:
                    endpoint = f"/units?storefront={storefront}&embedded=eco_participation"
                    response = await rac.send_request("POST", endpoint, json=unit_data)
                    
                except Exception as e:
                    offer = result.setdefault("data", {}).setdefault("offer", {})
                    statuses = offer.setdefault("statuses", {})
                    if "already exists" in str(e):
                        statuses[storefront] = "already exists"
                    continue
                
                offer = result.setdefault("data", {}).setdefault("offer", {})
                statuses = offer.setdefault("statuses", {})
                statuses[storefront] = "created"

        result["send_data"] = product_data
        print(result)
        
        if len(result["data"]["offer"]["statuses"]) == 7 and all([status == "created" for status in result["data"]["offer"]["statuses"].values()]):
            result["data"]["offer"] = {**result.get("data", {}).get("offer", {}), "status": "success"}
            return Response({"result": result}, status=status.HTTP_201_CREATED)
        
        elif len(result["data"]["offer"]["statuses"]) != 7 and all([status == "created" for status in result["data"]["offer"]["statuses"].values()]):
            result["data"]["offer"] = {**result.get("data", {}).get("offer", {}), "status": "partial success"}
            return Response({"result": result, "completed": "partial"}, status=status.HTTP_201_CREATED)
        
        else:
            result["data"]["offer"] = {**result.get("data", {}).get("offer", {}), "status": "failed"}
            return Response({"result": result, "completed": "none"}, status=status.HTTP_422_UNPROCESSABLE_ENTITY)


class DeactivateProductView(APIView):
    serializer_class = DeleteDataSerializer
    permission_classes = [AllowAny]
    
    
    async def delete(self, request, ean: str):
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)

        controller = serializer.validated_data["controller"]

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
                    get_response = await rac.send_request(
                        method="GET",
                        endpoint=(
                            f"/product/ean/{ean}"
                            f"?storefront={storefront}&embedded=units"
                        ),
                    )
                    units = get_response["data"]["units"]

                    if not units:
                        result["failed_storefronts"].append({
                            "storefront": storefront,
                            "error": "Units not found",
                        })
                        continue

                    unit_data = units[0]
                    unit_id = unit_data["id_unit"]

                    delete_response = await rac.send_request(
                        method="DELETE",
                        endpoint=f"/units/{unit_id}?storefront={storefront}",
                    )

                    if delete_response["status"] != 204:
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

                except Exception as error:
                    result["failed_storefronts"].append({
                        "storefront": storefront,
                        "error": str(error),
                    })
            return Response(result)


class ActivateProductView(APIView):
    serializer_class = ActivateProductSerializer
    permission_classes = [AllowAny]
    
    
    
    def build_payload(self, unit: dict, ean: str):
        return {
        "amount": unit["amount"],
        "handling_time": unit.get("delivery_time_min", 0),
        "listing_price": unit["price"],
        "minimum_price": (
            minimum_price
            if minimum_price is not None
            else unit["price"]
        ),
        "note": unit.get("note", ""),
        "id_shipping_group": 100003,
        "id_warehouse": 63925,
        "ean": ean,
        "id_product": unit["id_product"],
        "id_offer": str(unit["id_unit"]),
        "condition": unit["condition"],
        "vat_indicator": "standard_rate",
        "eco_participation": 0,
        "battery_participation": 0,
        "manufacturer_guarantee_years": 0,
    }
    
    async def post(self, request, ean: str):
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        validated_data = serializer.validated_data
        controller = validated_data["controller"]
        
        result = {"response": "success", "failed_storefront"}
        async with aiohttp.ClientSession() as client:
            rac = RestApiController(controller=contoller, session=client)
            
            # Retrieve unit by id from table
            for storefront in storefronts:
                try:
                    saved_unit = await Unit.objects.aget(
                        ean=ean,
                        storefront=storefront,
                        controller=controller
                    )
                    
                    unit_data = saved_unit.unit
                    
                    payload = build_payload(unit_data, ean)
                    
                    response = rac.send_request(
                        method="POST",
                        endpoint=
                        f"/units"
                        f"?storefront={storefront}",
                        json={payload}
                    )
                except Exception as error:
                    result["response"] = "failed"

                    result["failed_storefronts"].append({
                        "storefront": storefront,
                        "error": str(error)
                    })
        return response 

    