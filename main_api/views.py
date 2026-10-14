import aiohttp
from uuid import uuid4
import asyncio
import os
from django.db import connection
from adrf.views import APIView
from rest_framework import status
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.response import Response
from rest_framework_simplejwt.authentication import JWTStatelessUserAuthentication
import pandas as pd
from main_api.serializers import (
    FileUploadSerializer,
    CombinedUploadSerializer,
)
from main_api.src.controller.kaufland_controller import KauflandController
from main_api.src.servises.kaufland_upload_service import KauflandUploadService
from main_api.src.logger import log
from main_api.src.job_registry import (
    cancel_job,
)


try:
    MAX_CONCURRENT_JOBS = max(1, int(os.getenv("MAX_CONCURRENT_JOBS", "3")))
except ValueError:
    MAX_CONCURRENT_JOBS = 3
JOB_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_JOBS)


def _db_ping() -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        cursor.fetchone()


async def _run_checker_job(controller_name: str, eans: list[str], job_id: str) -> None:
    async with JOB_SEMAPHORE:
        async with aiohttp.ClientSession() as session:
            controller = KauflandController(session, controller_name)
            try:
                await controller.products_checker(eans, job_id=job_id)
            except Exception as e:
                await controller._send_task_progress(
                    job_id,
                    "checker",
                    "job_failed",
                    payload={
                        "total": len(eans),
                        "processed": 0,
                        "success": 0,
                        "error": len(eans),
                        "failed_eans": eans,
                    },
                    info=f"failed: {str(e)}",
                )
                log(
                    f"checker job failed job_id={job_id} controller={controller_name} eans={eans} error={e}",
                    save=True,
                )


async def _run_delete_job(controller_name: str, eans: list[str], job_id: str) -> None:
    async with JOB_SEMAPHORE:
        async with aiohttp.ClientSession() as session:
            controller = KauflandController(session, controller_name)
            try:
                await controller.delete_all_products(eans, job_id=job_id)
            except Exception as e:
                await controller._send_task_progress(
                    job_id,
                    "delete",
                    "job_completed",
                    payload={
                        "total": len(eans),
                        "processed": 0,
                        "success": 0,
                        "failed": len(eans),
                    },
                    info=f"failed: {str(e)}",
                )


async def _run_upload_collection_job(
    controller_name: str,
    json_content: list[dict],
    job_id: str,
) -> None:
    try:
        async with JOB_SEMAPHORE:
            async with aiohttp.ClientSession() as session:
                controller: KauflandController = KauflandController(
                    session, controller_name
                )
                service = KauflandUploadService(controller)
                await service.upload_collection(json_content, job_id=job_id)
    except Exception as e:
        log(
            f"upload_collection job failed job_id={job_id} controller={controller_name} error={e}",
            save=True,
            level="error",
        )




async def _handle_upload_collections_via_json_request(
    request,
    serializer_class,
    username: str | None,
) -> Response:
    log("<------Post method initialized----->")
    serializer = serializer_class(data=request.data)
    try:
        serializer.is_valid(raise_exception=True)
    except ValidationError as exc:
        log(f"upload_json validation failed: {exc.detail}", save=True)
        raise

    controller_name = serializer.validated_data["controller"]
    mode = serializer.validated_data["mode"]
    json_content = serializer.validated_data.get("json_content")
    job_id = serializer.validated_data.get("job_id")

    if json_content is None:
        return Response(
            {"error": "invalid json content"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    if isinstance(json_content, dict):
        json_content = [json_content]

    async with aiohttp.ClientSession() as session:
        controller: KauflandController = KauflandController(
            session, controller_name
        )
        service = KauflandUploadService(controller)
        if mode == "upload_product":
            if not job_id:
                job_id = uuid4().hex
            result = await service.upload_single(json_content[0], job_id=job_id)

            return Response(
                (
                    {"message": "success", "job_id": job_id}
                    if result
                    else {"message": "failed", "job_id": job_id}
                ),
                status=200 if result else 500,
            )

        if mode == "upload_collection":
            log("Massive upload")
            if not job_id:
                job_id = uuid4().hex
            asyncio.create_task(
                _run_upload_collection_job(
                    controller_name, json_content, job_id
                )
            )
            return Response(
                {"message": "upload job started", "job_id": job_id},
                status=status.HTTP_202_ACCEPTED,
            )

    return Response({"error": "invalid mode"}, status=status.HTTP_400_BAD_REQUEST)


class MainOperationsView(APIView):
    permission_classes = [IsAuthenticated]
    serializer_class = FileUploadSerializer

    async def get(self, request):
        return Response({"message": "ok"}, status=status.HTTP_200_OK)

    async def post(self, request):
        controller = None
        mode = None
        ean = None
        try:
            serializer = FileUploadSerializer(data=request.data)
            if not serializer.is_valid():
                return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

            file = serializer.validated_data.get("file")
            ean = serializer.validated_data.get("ean")
            job_id = serializer.validated_data.get("job_id")
            mode = serializer.validated_data["mode"]
            controller = serializer.validated_data["controller"]
            log(f"Contoller: {controller}")

            if mode == "checker" and not file:
                single_ean = str(ean).strip() if ean is not None else ""
                if not single_ean:
                    return Response(
                        {"error": "ean is required for checker"},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                async with aiohttp.ClientSession() as session:
                    kaufland_controller = KauflandController(session, controller)
                    check_result = await kaufland_controller._check_product_by_unit_id(
                        single_ean, job_id=job_id
                    )
                return Response(
                    {
                        "controller": controller,
                        "ean": single_ean,
                        **check_result,
                    },
                    status=status.HTTP_200_OK,
                )

            filename = file.name.lower()
            if filename.endswith(".csv"):
                df = await asyncio.to_thread(pd.read_csv, file)
            elif filename.endswith(".xlsx"):
                df = await asyncio.to_thread(pd.read_excel, file)
            else:
                return Response(
                    {"error": "unsupported file format"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            async with aiohttp.ClientSession() as session:
                kaufland_controller = KauflandController(session, controller)

                if set(df.columns) == {"ean"}:
                    df = df.astype({"ean": str})
                    eans_list = list(set(df["ean"].tolist()))

                    if mode == "delete":
                        if not job_id:
                            job_id = uuid4().hex
                        asyncio.create_task(
                            _run_delete_job(controller, eans_list, job_id)
                        )
                        response = Response(
                            {
                                "message": "delete job started",
                                "job_id": job_id,
                            },
                            status=status.HTTP_202_ACCEPTED,
                        )
                        return response

                    if mode == "checker":
                        if not job_id:
                            job_id = uuid4().hex
                        asyncio.create_task(
                            _run_checker_job(controller, eans_list, job_id)
                        )
                        return Response(
                            {
                                "message": "checker job started",
                                "job_id": job_id,
                                "eans": eans_list,
                            },
                            status=status.HTTP_202_ACCEPTED,
                        )

                    return Response(
                        {"info": "mode is not allowed for ean-only file"},
                        status=status.HTTP_400_BAD_REQUEST,
                    )

                if set(df.columns) >= {"ean", "price"}:
                    df = df.astype({"ean": str, "price": float})
                    eans_prices = dict(zip(df["ean"], df["price"]))

                    if mode == "delete":
                        if not job_id:
                            job_id = uuid4().hex
                        asyncio.create_task(
                            _run_delete_job(
                                controller, list(eans_prices.keys()), job_id
                            )
                        )
                        response = Response(
                            {
                                "message": "delete job started",
                                "job_id": job_id,
                            },
                            status=status.HTTP_202_ACCEPTED,
                        )
                        return response

                    if mode == "change_price":
                        result = await kaufland_controller.update_price(eans_prices)
                        if result:
                            response = Response(
                                "all prices updated", status=status.HTTP_200_OK
                            )
                            return response
                        return Response(
                            "something went wrong",
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        )

                return Response(
                    {"error": "file must include column 'ean' (and optional 'price')"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
        except Exception as e:
            log(
                f"file processing failed controller={controller} mode={mode} input_ean={ean} error={e}",
                save=True,
            )
            response = Response(
                {
                    "error": f"file processing error: {str(e)}",
                    "input_ean": str(ean).strip() if ean is not None else None,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
            return response


class UploadCollectionsViaJsonView(APIView):
    permission_classes = [IsAuthenticated]
    serializer_class = CombinedUploadSerializer

    async def get(self, request):
        return Response(
            {"info": "Kaufland uploader via json"}, status=status.HTTP_200_OK
        )

    async def post(self, request):
        username = (
            request.user.get_username()
            if getattr(request.user, "is_authenticated", False)
            else None
        )
        return await _handle_upload_collections_via_json_request(
            request=request,
            serializer_class=self.serializer_class,
            username=username,
        )


class UploadCollectionsViaJsonJwtView(APIView):
    permission_classes = [IsAuthenticated]
    authentication_classes = [JWTStatelessUserAuthentication]
    serializer_class = CombinedUploadSerializer

    async def get(self, request):
        return Response(
            {"info": "Kaufland uploader via json (jwt only)"},
            status=status.HTTP_200_OK,
        )

    async def post(self, request):
        return await _handle_upload_collections_via_json_request(
            request=request,
            serializer_class=self.serializer_class,
            username=None,
        )


class ProtectedView(APIView):
    permission_classes = [IsAuthenticated]

    async def get(self, request):
        return Response({"message": "ok"}, status=status.HTTP_200_OK)


class HealthCheckView(APIView):
    permission_classes = [AllowAny]
    authentication_classes = []

    async def get(self, request):
        try:
            await asyncio.to_thread(_db_ping)
        except Exception:
            return Response(
                {
                    "status": "degraded",
                    "service": "kaufland-api",
                    "database": "error",
                },
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        return Response(
            {
                "status": "ok",
                "service": "kaufland-api",
                "database": "ok",
            },
            status=status.HTTP_200_OK,
        )


class ProductByEanView(APIView):
    permission_classes = [IsAuthenticated]

    async def get(self, request, ean: str):
        normalized_ean = str(ean or "").strip()
        if not normalized_ean:
            return Response(
                {"error": "ean is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        controller_name = (
            str(request.query_params.get("controller", "jv")).strip().lower()
        )
        if controller_name not in {"jv", "xl"}:
            return Response(
                {"error": "controller must be 'jv' or 'xl'"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        async with aiohttp.ClientSession() as session:
            controller = KauflandController(session, controller_name)
            result = await controller._check_product_by_unit_id(normalized_ean)

        return Response(
            {
                "controller": controller_name,
                **result,
            },
            status=status.HTTP_200_OK,
        )


class StopJobView(APIView):
    permission_classes = [IsAuthenticated]

    async def post(self, request):
        job_id = str(request.data.get("job_id") or "").strip()
        if not job_id:
            return Response(
                {"error": "job_id is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        stopped = await cancel_job(job_id)
        if not stopped:
            return Response(
                {"error": "unable to stop job"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        return Response({"message": "stop requested", "job_id": job_id}, status=200)


class PriceCheckerView(APIView):
    """
    Вьюха для того чтобы чекать цену. Планируется использовать как бекграунд джоб.
    """

    async def post(self, request):
        # Some logic here that starts job
        ...
