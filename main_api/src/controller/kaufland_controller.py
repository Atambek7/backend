from cmath import pi
import os
from pydoc import describe
import re
import math
from sqlite3 import adapt
import time
import hmac
import json
import asyncio
from typing import Any
from uuid import uuid4
import certifi
import aiohttp
import hashlib
from datetime import datetime
from django.conf import settings
from channels.layers import get_channel_layer
from config import (
    controllers_base,
    storefronts,
    XL_CONTACT_DATA,
    XL_MANUFACTURER,
    JV_CONTACT_DATA,
    JV_MANUFACTURER,
    SSH_HOST,
    SSH_USER,
    SSH_KEY_PATH,
)
import ssl
from aiolimiter import AsyncLimiter
from main_api.src.logger import log
from main_api.src.job_registry import (
    is_cancelled,
    clear_cancel,
    register_running_job,
    unregister_running_job,
)
from main_api.auth.aftercool_auth_servise import AftercoolAuthService
from main_api.auth.aftercool_product_retrieve import AftercoolProductRetrieveService
from main_api.src.ssh_client import SSHFileClient
from main_api.src.servises.pic_pipline import process_pics
from main_api.src.gpt.gpt_helper import generate_description, generate_seo
from main_api.src.extracter import adapt_html_description, adapt_html_description_v2
from main_api.src.utils.timer import log_time
from aiohttp import ClientSession
from rest_framework import status

try:
    _GLOBAL_API_RATE = max(1, int(os.getenv("KAUFLAND_API_RATE_PER_SEC", "60")))
except ValueError:
    _GLOBAL_API_RATE = 60
_GLOBAL_API_LIMITER = AsyncLimiter(_GLOBAL_API_RATE, time_period=1)

try:
    _GLOBAL_REQUEST_DELAY_SEC = max(
        0.0, float(os.getenv("KAUFLAND_API_REQUEST_DELAY_SEC", "0.08"))
    )
except ValueError:
    _GLOBAL_REQUEST_DELAY_SEC = 0.08
_GLOBAL_REQUEST_DELAY_LOCK = asyncio.Lock()
_GLOBAL_LAST_REQUEST_AT = 0.0

try:
    _WS_PROGRESS_INTERVAL_SEC = max(
        0.0, float(os.getenv("WS_PROGRESS_INTERVAL_SEC", "60"))
    )
except ValueError:
    _WS_PROGRESS_INTERVAL_SEC = 60
try:
    _WS_PROGRESS_EVERY = max(0, int(os.getenv("WS_PROGRESS_EVERY", "0")))
except ValueError:
    _WS_PROGRESS_EVERY = 0
try:
    _UPLOAD_HEARTBEAT_SEC = max(5.0, float(os.getenv("UPLOAD_HEARTBEAT_SEC", "20")))
except ValueError:
    _UPLOAD_HEARTBEAT_SEC = 20.0
try:
    _UPLOAD_STUCK_TIMEOUT_SEC = max(
        30.0, float(os.getenv("UPLOAD_STUCK_TIMEOUT_SEC", "180"))
    )
except ValueError:
    _UPLOAD_STUCK_TIMEOUT_SEC = 180.0
try:
    _AFTERCOOL_CB_FAIL_THRESHOLD = max(
        1, int(os.getenv("AFTERCOOL_CB_FAIL_THRESHOLD", "5"))
    )
except ValueError:
    _AFTERCOOL_CB_FAIL_THRESHOLD = 5
try:
    _AFTERCOOL_CB_OPEN_SEC = max(10.0, float(os.getenv("AFTERCOOL_CB_OPEN_SEC", "300")))
except ValueError:
    _AFTERCOOL_CB_OPEN_SEC = 300.0


EMBEDDED_ENTITIES = ["category", "category_basic", "units"]


def format_si_dimension(value: Any, default_unit: str = "cm") -> list[str] | None:
    """Return Kaufland SI dimension format, e.g. ["184,5cm"]."""
    text = KauflandController._as_scalar_string(value)
    if not text:
        return None

    match = re.search(
        r"(\d+(?:[,.]\d+)?)\s*(mm|cm|m|km)?\b",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    number = match.group(1).replace(".", ",")
    unit = (match.group(2) or default_unit).lower()
    return [f"{number}{unit}"]


class KauflandController:
    """
    Класс для взаимодействия с кауфлендом
    """

    def __init__(self, session: ClientSession, version):
        # Shared limiter for the whole process to avoid per-job rate multiplication.
        self.limiter = _GLOBAL_API_LIMITER
        self.session = session
        self.version = version
        self.channel_layer = get_channel_layer()
        self.retries = 3
        self.backoff = 2
        try:
            self.upload_batch_size = max(1, int(os.getenv("UPLOAD_BATCH_SIZE", "6")))
        except ValueError:
            self.upload_batch_size = 6
        self.base_api_url = "https://sellerapi.kaufland.com/v2"
        self._ssh_client = None  # Коннекшн к ншашему серверу

        # Мини кэшируемая система
        self._ean_cache = {
            "unit_ids": {},
            "product_ids": {},
            "main_data": {},
        }

        self.aftercool_base_url = "https://aftercool.de"
        self.aftercool_failures = 0
        self.aftercool_circuit_open_until = 0.0
        self.last_upload_error = None

    def _aftercool_circuit_open(self) -> bool:
        return time.monotonic() < self.aftercool_circuit_open_until

    def _aftercool_record_success(self) -> None:
        self.aftercool_failures = 0
        self.aftercool_circuit_open_until = 0.0

    def _aftercool_record_failure(self) -> None:
        self.aftercool_failures += 1
        if self.aftercool_failures >= _AFTERCOOL_CB_FAIL_THRESHOLD:
            self.aftercool_circuit_open_until = time.monotonic() + _AFTERCOOL_CB_OPEN_SEC
            log(
                f"aftercool circuit opened failures={self.aftercool_failures} open_for_sec={_AFTERCOOL_CB_OPEN_SEC}",
                save=True,
                level="warning",
            )

    def _fail_upload(self, reason: str, *, level: str = "error") -> bool:
        self.last_upload_error = reason
        log(reason, save=True, level=level)
        return False

    @staticmethod
    def _as_scalar_string(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, list):
            for item in value:
                if item is None:
                    continue
                text = str(item).strip()
                if text:
                    return text
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _should_emit_progress(
        *,
        processed: int,
        total: int,
        last_emit_at: float | None,
        interval_sec: float = _WS_PROGRESS_INTERVAL_SEC,
        every_n: int = _WS_PROGRESS_EVERY,
    ) -> bool:
        if total > 0 and processed >= total:
            return True
        if processed <= 0:
            return False
        if every_n > 0 and processed % every_n == 0:
            return True
        now = time.monotonic()
        if last_emit_at is None:
            return True
        return (now - last_emit_at) >= max(0.0, interval_sec)


    async def _apply_inter_request_delay(self) -> None:
        global _GLOBAL_LAST_REQUEST_AT
        if _GLOBAL_REQUEST_DELAY_SEC <= 0:
            return
        async with _GLOBAL_REQUEST_DELAY_LOCK:
            now = time.monotonic()
            wait_seconds = (_GLOBAL_LAST_REQUEST_AT + _GLOBAL_REQUEST_DELAY_SEC) - now
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            _GLOBAL_LAST_REQUEST_AT = time.monotonic()

    # NOTE: НЕ ТРОГАТЬ!!!
    @staticmethod
    def _sign_request(method, uri, body, timestamp, secret_key):
        """
        Функция для генерирования нового HMAC делается для
        безопасности использования в Kaufland, для КАЖДОГО запроса
        """
        if isinstance(body, dict):
            body = json.dumps(body, separators=(",", ":"))  # важно без пробелов
        elif body is None:
            body = ""
        plain_text = "\n".join([method, uri, body, str(timestamp)])
        digest_maker = hmac.new(secret_key.encode(), None, hashlib.sha256)
        digest_maker.update(plain_text.encode())
        return digest_maker.hexdigest()

    # NOTE: НЕ ТРОГАТЬ!!!
    async def _get_headers(self, url, body, method):
        if body is None:
            body = ""
        timenow = str(int(time.time()))
        controller = self.version
        client_key = controllers_base[controller]["client_key"]
        secret_key = controllers_base[controller]["secret_key"]
        signed_key = self._sign_request(
            method=method, uri=url, body=body, timestamp=timenow, secret_key=secret_key
        )
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Inhouse_development",
            "Shop-Timestamp": timenow,
            "Shop-Client-Key": client_key,
            "Shop-Signature": signed_key,
        }
        return headers

    # NOTE: НЕ ТРОГАТЬ!!!
    async def _universal_request(self, method, url, body=None, params=None) -> Any:
        """
        Универсальная функция для выполнения реквестов для любых
        методов
        """
        ssl_cert = ssl.create_default_context(cafile=certifi.where())

        if body is not None and body != "":
            if isinstance(body, dict):
                serialized_body = json.dumps(body, separators=(",", ":"))
            else:
                serialized_body = body
        else:
            serialized_body = ""

        retryable_statuses = {408, 409, 425, 429, 500, 502, 503, 504}
        for attempt in range(1, self.retries + 1):
            headers = await self._get_headers(url, serialized_body, method=method)
            try:
                async with self.limiter:
                    await self._apply_inter_request_delay()
                    if method == "GET":
                        async with self.session.get(
                            url=url, headers=headers, params=params, ssl=ssl_cert
                        ) as response:
                            if response.status == status.HTTP_200_OK:
                                return await response.json()
                            if response.status in retryable_statuses and attempt < self.retries:
                                log(
                                    f"_universal_request retry method={method} url={url} status={response.status} attempt={attempt}/{self.retries}",
                                    save=True,
                                    level="warning",
                                )
                                await asyncio.sleep(min(10, (2 ** attempt) + 0.2 * attempt))
                                continue
                            if response.status == status.HTTP_404_NOT_FOUND:
                                return {
                                    "message": "not found",
                                    "status": status.HTTP_404_NOT_FOUND,
                                    "data": None,
                                }
                            return await response.json()

                    if method == "POST":
                        async with self.session.post(
                            url=url, headers=headers, data=serialized_body, ssl=ssl_cert
                        ) as response:
                            if response.status in retryable_statuses and attempt < self.retries:
                                log(
                                    f"_universal_request retry method={method} url={url} status={response.status} attempt={attempt}/{self.retries}",
                                    save=True,
                                    level="warning",
                                )
                                await asyncio.sleep(min(10, (2 ** attempt) + 0.2 * attempt))
                                continue
                            return await response.json()
                    if method == "PUT":
                        async with self.session.put(
                            url=url, headers=headers, data=serialized_body, ssl=ssl_cert
                        ) as response:
                            if response.status in retryable_statuses and attempt < self.retries:
                                log(
                                    f"_universal_request retry method={method} url={url} status={response.status} attempt={attempt}/{self.retries}",
                                    save=True,
                                    level="warning",
                                )
                                await asyncio.sleep(min(10, (2 ** attempt) + 0.2 * attempt))
                                continue
                            return await response.json()
                    if method == "DELETE":
                        async with self.session.delete(
                            url=url, headers=headers, json=body, ssl=ssl_cert
                        ) as response:
                            if response.status in (204, 200, 404):
                                return True
                            if response.status in retryable_statuses and attempt < self.retries:
                                log(
                                    f"_universal_request retry method={method} url={url} status={response.status} attempt={attempt}/{self.retries}",
                                    save=True,
                                    level="warning",
                                )
                                await asyncio.sleep(min(10, (2 ** attempt) + 0.2 * attempt))
                                continue
                            return False
                    if method == "PATCH":
                        async with self.session.patch(
                            url=url, headers=headers, data=serialized_body, ssl=ssl_cert
                        ) as response:
                            if response.status == 200:
                                return {"message": "success"}
                            if response.status in retryable_statuses and attempt < self.retries:
                                log(
                                    f"_universal_request retry method={method} url={url} status={response.status} attempt={attempt}/{self.retries}",
                                    save=True,
                                    level="warning",
                                )
                                await asyncio.sleep(min(10, (2 ** attempt) + 0.2 * attempt))
                                continue
                            if response.status == 404:
                                return {"message": "not found"}
                            return {
                                "message": "something went wrong",
                                "status": response.status,
                            }
            except Exception as e:
                log(
                    f"_universal_request exception method={method} url={url} attempt={attempt}/{self.retries} error_type={type(e).__name__} error={e!r}",
                    save=True,
                    level="error",
                )
                if attempt < self.retries:
                    await asyncio.sleep(min(10, (2 ** attempt) + 0.2 * attempt))
                    continue
                raise

    async def _get_session(self) -> str | None:
        aftercool = AftercoolAuthService(
            self.aftercool_base_url, username="Miras", password="Miras112112"
        )
        auth = await aftercool.authenticate()

        if auth.get("success"):
            log("Session obtained from aftercool", save=True, level="success")
            session_cookie = auth.get("session")
            return session_cookie
        else:
            log("Failed session obtain from aftercool", save=True, level="error")
            return None

    async def _get_product(
        self, product_id: str, session_cookie: str
    ) -> dict[str, Any]:
        log("Fetching product data from aftercool...", save=True, level="info")
        
        aftercoool = AftercoolProductRetrieveService(
            self.aftercool_base_url, session_cookie
        )
        product = await aftercoool.get_product(product_id)
        return product

    @staticmethod
    async def get_exchange_rate(currency: str = "PLN") -> float:
        """
        Возвращает курс EUR -> currency.
        """
        currency_dict = {"PLN": 4.7, "CZK": 25.4, "ITP": 1.05}
        return currency_dict[currency]

    @staticmethod
    def format_price(price: float) -> int:
        """
        Округляет цену в большую сторону и заменяет последнюю цифру на 9.
        Например, 12.34 -> 19.99, 56.78 -> 59.99
        Args:
            price (float): Исходная цена.
        Returns:
            int: Отформатированная цена в центах.
        """
        # округляем в большую сторону
        price_up = math.ceil(price)

        # отбрасываем последнюю цифру и прибавляем 9
        return ((price_up // 10) * 10 + 9) * 100

    async def _make_price(self, storefront, price):
        """
        Функция которая создает цену взависимости от страны
        Принимает страну и цену
        """
        if storefront == "cz":
            rate = await self.get_exchange_rate("CZK")
            total_price = int(self.format_price(rate * price))
        elif storefront == "pl":
            rate = await self.get_exchange_rate("PLN")
            total_price = int(self.format_price(rate * price))
        elif storefront == "fr" or storefront == "it":
            rate = await self.get_exchange_rate(
                "ITP"
            )  # ITP Increased Tax Price = EUR_1.05
            total_price = int(self.format_price(price * rate))
        else:
            total_price = int(self.format_price(price))
        return total_price

    @staticmethod
    async def status_processing(tasks: list) -> bool:
        """
        Функция для обработки статуса всех тасков.
        Если все успешно возвращает True иначе False.
        """
        bool_list = []
        for elem in range(0, len(tasks), 10):
            new_tasks = tasks[elem : elem + 10]
            result = await asyncio.gather(*new_tasks)
            if all(result):
                bool_list.append(True)
            else:
                bool_list.append(False)
        if all(bool_list):
            return True
        return False

    async def _fetch_unit_id_by_ean(self, ean):
        """
        Функция которая собирает unit_id
        о продукте по его EAN со всех стран.
        Так же кешируема для более быстрого поиска
        """
        if ean in self._ean_cache["unit_ids"]:
            return self._ean_cache["unit_ids"][ean]

        async def fetch_storefront_unit_id(storefront):
            url = f"{self.base_api_url}/products/ean/{ean}?storefront={storefront}&embedded=units"
            result = await self._universal_request(method="GET", url=url)

            if not isinstance(result, dict):
                return storefront, None

            data = result.get("data") or {}
            units = data.get("units") or [{}]
            unit_id = units[0].get("id_unit", None)

            return storefront, unit_id

        final_res = {}
        try:
            fetched = await asyncio.gather(
                *(fetch_storefront_unit_id(storefront) for storefront in storefronts)
            )
            final_res = {
                storefront: unit_id for storefront, unit_id in fetched if unit_id
            }
        except Exception as e:
            log(f"_fetch_unit_id_by_ean failed ean={ean} error={e}")
        self._ean_cache["unit_ids"][ean] = final_res
        return final_res

    async def _fetch_product_id_by_ean(self, ean):
        """
        Функция которая собирает product_id
        о продукте по его EAN со всех стран
        """
        if ean in self._ean_cache["product_ids"]:
            return self._ean_cache["product_ids"][ean]

        async def fetch_storefront_product_id(storefront):
            log(f"[{ean}] Start of parsing product's product_id")
            url = f"{self.base_api_url}/products/ean/{ean}?storefront={storefront}&embedded=units"
            result = await self._universal_request(method="GET", url=url)
            log(f"[{ean}] End of parsing product_id. Result is -----> {result}")
            if not isinstance(result, dict):
                log(f"[{ean} Error happend while fetching]")
                return storefront, ""
            data = result.get("data") or {}
            product_id = data.get("id_product", "")
            return storefront, product_id

        fetched = await asyncio.gather(
            *(fetch_storefront_product_id(storefront) for storefront in storefronts)
        )
        final_res = {
            storefront: product_id for storefront, product_id in fetched if product_id
        }
        self._ean_cache["product_ids"][ean] = final_res
        return final_res

    @staticmethod
    def _is_offer_already_exists_response(result: Any) -> bool:
        return "already exists" in json.dumps(
            result,
            ensure_ascii=False,
            default=str,
        ).lower()

    async def _patch_unit_payload(self, storefront, price, delivery=None, amount=None):
        formatted_price = await self._make_price(storefront=storefront, price=price)
        payload = {
            "listing_price": formatted_price,
            "minimum_price": formatted_price,
        }
        if delivery is not None:
            payload["handling_time"] = delivery
        if amount is not None:
            payload["amount"] = amount
        return payload

    async def _add_unit_id(
        self,
        ean,
        price,
        delivery,
        target_storefronts=None,
        id_offer=None,
        amount=20,
    ):
        """
        Create a unit for a product in Kaufland.
        """
        if target_storefronts is None:
            target_storefronts = storefronts
        id_offer = str(id_offer or ean)

        async def add_unit_for_storefront(storefront):
            url = f"{self.base_api_url}/units?storefront={storefront}&embedded=eco_participation"
            formatted_price = await self._make_price(storefront=storefront, price=price)

            body = {
                "amount": amount,
                "handling_time": delivery,
                "listing_price": formatted_price,
                "minimum_price": formatted_price,
                "ean": str(ean),
                "id_offer": id_offer,
                "condition": "NEW",
            }

            result = await self._universal_request("POST", url, body=body)
            if not isinstance(result, dict):
                return self._fail_upload(
                    f"add_unit failed ean={ean} storefront={storefront} response_type={type(result).__name__}"
                )

            data = result.get("data") or {}
            if data.get("status", "") == "AVAILABLE":
                return True

            if self._is_offer_already_exists_response(result):
                self._ean_cache["unit_ids"].pop(ean, None)
                unit_ids = await self._fetch_unit_id_by_ean(ean)
                unit_id = unit_ids.get(storefront) if isinstance(unit_ids, dict) else None
                if not unit_id:
                    return self._fail_upload(
                        f"add_unit offer exists but unit_id not found ean={ean} storefront={storefront} id_offer={id_offer} response={result}",
                    )

                patch_url = f"{self.base_api_url}/units/{unit_id}?storefront={storefront}&embedded=eco_participation"
                patch_payload = await self._patch_unit_payload(
                    storefront=storefront,
                    price=price,
                    delivery=delivery,
                    amount=amount,
                )
                patch_result = await self._universal_request(
                    method="PATCH",
                    url=patch_url,
                    body=patch_payload,
                )
                patched = patch_result.get("message") in {"success", "not found"}
                if not patched:
                    self.last_upload_error = (
                        f"patch existing offer failed ean={ean} storefront={storefront} "
                        f"id_offer={id_offer} unit_id={unit_id} response={patch_result}"
                    )
                log(
                    f"add_unit existing offer patched ean={ean} storefront={storefront} id_offer={id_offer} unit_id={unit_id} patched={patched}",
                    save=True,
                    level="info" if patched else "error",
                )
                return patched

            return self._fail_upload(
                f"add_unit failed ean={ean} storefront={storefront} response={result} body={json.dumps(body, ensure_ascii=False)}"
            )

        bool_list = await asyncio.gather(
            *(add_unit_for_storefront(storefront) for storefront in target_storefronts)
        )
        return all(bool_list)

    async def _fetch_main_data_by_ean(self, ean):
        """
        Функция которая собирает unit_id, title, price
        """

        if ean in self._ean_cache["main_data"]:
            return self._ean_cache["main_data"][ean]

        async def fetch_storefront_main_data(storefront):
            url = f"{self.base_api_url}/products/ean/{ean}?storefront={storefront}&embedded=units"
            result = await self._universal_request(method="GET", url=url)
            if not isinstance(result, dict):
                return None
            if result.get("status") == status.HTTP_404_NOT_FOUND:
                return None
            data = result.get("data") or {}
            units = data.get("units") or []
            id_unit = units[0].get("id_unit") if units else None
            if id_unit:
                title = data.get("title")
                price = float(units[0].get("price")) / 100
                storefront = data.get("storefront")
                return {
                    "ean": ean,
                    "title": title,
                    "price": price,
                    "storefront": storefront,
                }
            return None

        fetched = await asyncio.gather(
            *(fetch_storefront_main_data(storefront) for storefront in storefronts)
        )
        final_res = [item for item in fetched if item]
        self._ean_cache["main_data"][ean] = final_res
        return final_res

    # async def _send_progress(self, ean: str, stage: str, extra: dict | None = None):
    #     channel_layer = get_channel_layer()

    #     await channel_layer.group_send(
    #         "upload_progress",
    #         {
    #             "type": "upload_progress",
    #             "ean": ean,
    #             "stage": stage,
    #             "controller": self.version,
    #             "extra": extra or {},
    #             "timestamp": datetime.now().isoformat(),
    #         },
    #     )

    async def _send_progress(
        self,
        job_id: str,
        event: str,
        payload: dict | None = None,
        info: str | None = None,
    ):
        if not job_id:
            return
        await self._ws_message_send(
            group_name=f"{job_id}_upload",
            job_id=job_id,
            event=event,
            payload=payload,
            info=info,
        )

    async def _send_task_progress(
        self,
        job_id: str | None,
        task: str,
        event: str,
        payload: dict | None = None,
        info: str | None = None,
    ):
        if not job_id:
            return
        payload_with_task = dict(payload or {})
        payload_with_task.setdefault("task", task)
        await self._ws_message_send(
            group_name=f"{job_id}_{task}",
            job_id=job_id,
            event=event,
            payload=payload_with_task,
            info=info,
        )

    async def _ws_message_send(
        self,
        group_name: str,
        job_id: str,
        event: str,
        payload: dict | None = None,
        info: str | None = None,
    ):
        if not self.channel_layer:
            return
        await self.channel_layer.group_send(
            group_name,
            {
                "type": "ws_message",
                "job_id": job_id,
                "event": event,
                "payload": payload,
                "info": info,
                "timestamp": datetime.now().isoformat(),
            },
        )

    @staticmethod
    def _normalize_value(value):
        if isinstance(value, list):
            return ", ".join([str(v) for v in value if v is not None])
        return value

    def _build_item_payload(self, elem: dict | None) -> dict:
        if not isinstance(elem, dict):
            return {
                "ean": None,
                "article": None,
                "price": None,
                "size": None,
                "color": None,
                "material": None,
                "fabric": None,
            }
        return {
            "ean": elem.get("ean"),
            "article": elem.get("article"),
            "price": elem.get("price"),
            "size": self._normalize_value(elem.get("size")),
            "color": self._normalize_value(elem.get("color")),
            "material": self._normalize_value(elem.get("material")),
            "fabric": elem.get("fabric"),
        }

    async def _emit_progress_event(
        self,
        job_id: str | None,
        event: str,
        payload: dict | None = None,
        info: str | None = None,
    ):
        if not job_id:
            return
        await self._send_progress(job_id, event, payload=payload, info=info)

    async def _emit_stage(
        self,
        job_id: str | None,
        ean: str | None,
        item: dict | None,
        stage: str,
        message: str | None = None,
        status: str = "running",
        extra: dict | None = None,
    ):
        payload = {
            "status": status,
            "ean": ean,
            "item": item,
            "stage": stage,
        }
        if message:
            payload["message"] = message
        if extra:
            payload["extra"] = extra
        await self._emit_progress_event(job_id, "progress", payload=payload)

    async def _emit_ean_status(
        self,
        job_id: str | None,
        status: str,
        ean: str | None,
        item: dict | None,
        stage: str | None = None,
        message: str | None = None,
        detail: str | None = None,
    ):
        payload = {
            "status": status,
            "ean": ean,
            "item": item,
        }
        if stage:
            payload["stage"] = stage
        if message:
            payload["message"] = message
        if detail:
            payload["detail"] = detail
        await self._emit_progress_event(job_id, "ean_completed", payload=payload)

    async def _update_price_by_unit_id(
        self,
        ean,
        price,
        new_unit_id: dict[str, str] | None = None,
        delivery=None,
        amount=None,
    ):
        """
        Update product price by unit_id.
        """
        channel_layer = get_channel_layer()
        final_res = (
            new_unit_id
            if new_unit_id is not None
            else await self._fetch_unit_id_by_ean(ean)
        )

        if final_res:
            bool_list = []
            for storefront, unit_id in final_res.items():
                url = f"{self.base_api_url}/units/{unit_id}?storefront={storefront}&embedded=eco_participation"
                payload = await self._patch_unit_payload(
                    storefront=storefront,
                    price=price,
                    delivery=delivery,
                    amount=amount,
                )

                result = await self._universal_request(
                    method="PATCH", url=url, body=payload
                )
                message = result["message"]

                if message == "success":
                    bool_list.append(True)

                elif message == "not found":
                    bool_list.append(True)

                else:
                    self.last_upload_error = (
                        f"update unit failed ean={ean} storefront={storefront} "
                        f"unit_id={unit_id} response={result} payload={payload}"
                    )
                    log(self.last_upload_error, save=True, level="error")
                    bool_list.append(False)

            return all(bool_list)

        return True

    async def _delete_product_by_unit_id(
        self, ean: str, job_id: str | None = None
    ) -> bool:
        """
        Delete products by unit_id.
        """
        channel_layer = get_channel_layer()
        logs_dir = os.path.join(settings.MEDIA_ROOT, "logs")
        logs_file_path = os.path.join(logs_dir, "delete_products_logs.csv")
        os.makedirs(logs_dir, exist_ok=True)

        final_res = await self._fetch_unit_id_by_ean(ean)
        if final_res:
            bool_list = []
            for storefront, unit_id in final_res.items():
                url = f"{self.base_api_url}/units/{unit_id}?storefront={storefront}"
                result = await self._universal_request(method="DELETE", url=url)
                if result:
                    bool_list.append(True)
                    with open(logs_file_path, "a", encoding="utf-8") as file:
                        file.write(
                            f"\nDeleted product {self.version} EAN: {ean} storefront: {storefront}\n"
                        )
                else:
                    bool_list.append(False)
                    log(
                        f"delete failed ean={ean} storefront={storefront} unit_id={unit_id}",
                    )
                    with open(logs_file_path, "a", encoding="utf-8") as file:
                        file.write(
                            f"\nDelete failed {self.version} EAN: {ean} storefront: {storefront}\n"
                        )

                await channel_layer.group_send(
                    "delete_group",
                    {
                        "type": "delete_progress",
                        "message": (
                            {"info": "success"} if result else {"info": "fail"}
                        ),
                        "controller": self.version,
                        "ean": ean,
                        "storefront": storefront,
                    },
                )
                await self._send_task_progress(
                    job_id,
                    "delete",
                    "storefront_result",
                    payload={
                        "controller": self.version,
                        "ean": ean,
                        "storefront": storefront,
                        "result": "success" if result else "fail",
                    },
                )

            return all(bool_list)

        log(f"delete no_unit_ids ean={ean}")

        await channel_layer.group_send(
            "delete_group",
            {
                "type": "delete_message",
                "message": {"info": "no_unit_ids"},
                "controller": self.version,
                "ean": ean,
                "storefront": "all",
            },
        )
        await self._send_task_progress(
            job_id,
            "delete",
            "storefront_result",
            payload={
                "controller": self.version,
                "ean": ean,
                "storefront": "all",
                "result": "no_unit_ids",
            },
        )

        log(f"delete ean_not_found ean={ean}")
        with open(logs_file_path, "a", encoding="utf-8") as file:
            file.write(f"\nEAN not found: {ean}\n")
        return True

    async def _check_product_by_unit_id(self, ean, job_id: str | None = None):
        """
    Check a single product by unit_id.
        """
        final_res = await self._fetch_main_data_by_ean(ean)
        if final_res:
            await self._send_task_progress(
                job_id,
                "checker",
                "item",
                payload={"controller": self.version, "ean": ean, "items": final_res},
            )
            storefronts = sorted(
                {item.get("storefront") for item in final_res if item.get("storefront")}
            )
            return {
                "ean": ean,
                "found": True,
                "storefronts": storefronts,
                "items": final_res,
                "message": None,
            }
        await self._send_task_progress(
            job_id,
            "checker",
            "item",
            payload={"controller": self.version, "ean": ean, "items": []},
            info="not found",
        )
        return {
            "ean": ean,
            "found": False,
            "storefronts": [],
            "items": [],
            "message": "not found",
        }

    async def _category_selector(self, article, price, ean, description):
        """
        Функция которая определяет категорию на основе article
        и его description
        """
        item_for_category_js = {
            "item": {
                "title": article,
                "description": description,
                "manufacturer": "AEA GmbH & Co. KG",
            },
            "price": price,
        }
        url_category = (
            f"{self.base_api_url}/categories/decide?storefront=de&locale=de-DE"
        )

        result_category = await self._universal_request(
            method="POST", url=url_category, body=item_for_category_js
        )
        if "data" not in result_category:
            log(
                f"ERROR WITH CATEGORY: {result_category} article: {article}, price: {price}, ean: {ean}, description: {description}",
            )
            return None, None
        category_id = result_category["data"][0]["id_category"]
        category_name = result_category["data"][0]["name"]
        return category_id, category_name

    async def upload_single_product(
        self, item: dict, job_id: str | None = None
    ) -> bool:
        """
        Upload exactly 1 product.
        No batching, no asyncio.gather
        """
        return await self._create_product(item, job_id=job_id)

    async def _create_product(self, elem: dict, job_id=None, controller=None):
        """
        Функция для создания продукта приходит elem: dict
        """

        ean = elem.get("ean")

        log(f"start upload item ean={ean}", save=True, level="info")
        self.last_upload_error = None
        if await is_cancelled(job_id):
            return self._fail_upload(
                f"upload cancelled before validation ean={ean} job_id={job_id}",
                level="warning",
            )

        if not ean:
            return self._fail_upload(f"ERROR WITH EAN: elem: {elem}")

        elif not elem.get("article"):
            return self._fail_upload(f"ERROR WITH ARTICLE: elem: {elem}")

        elif elem.get("price", 0) < 30:
            return self._fail_upload(f"ERROR WITH PRICE: elem: {elem}")

        elif not elem.get("pic_main"):
            return self._fail_upload(f"ERROR WITH PIC: elem: {elem}")

        log(
            f"validation passed ean={ean} article_present={bool(elem.get('article'))} price={elem.get('price')} pic_main_present={bool(elem.get('pic_main'))}",
            save=True,
            level="info",
        )

        article = elem["article"]
        size = elem["size"]
        color = elem["color"]
        material = elem["material"]
        price = int(elem["price"])
        amount = int(elem.get("amount") or 20)
        id_offer = str(elem.get("id_offer") or ean)
        target_storefronts = elem.get("storefronts") or storefronts
        total = (
            [elem["pic_main"]] + elem["pics"]
            if len(elem["pics"]) > 0 and elem["pics"][0]
            else [elem["pic_main"]]
        )
        log(
            f"prepared source images ean={ean} total_input_images={len(total)}",
            save=True,
            level="info",
        )
        fabric = elem["fabric"] if elem.get("fabric") else None

        if isinstance(material, list):
            material = ", ".join(material)

        if isinstance(color, list):
            color = ", ".join(color)

        delivery = 28

        if isinstance(fabric, str) and "cn" in fabric.lower():
            delivery = 40
        elif isinstance(fabric, str) and "tr" in fabric.lower() or isinstance(fabric, str) and "pl" in fabric.lower() or isinstance(fabric, str) and "it" in fabric.lower():
            delivery = 32
        log(
            f"delivery computed ean={ean} delivery_days={delivery} fabric={fabric}",
            save=True,
            level="info",
        )

        ean = elem["ean"]

        description_task = asyncio.create_task(
            generate_description(article, size, color, material)
        )
        pics_task = asyncio.create_task(process_pics(total))
        log(
            f"parallel preprocessing started ean={ean} tasks=description,pics",
            save=True,
            level="info",
        )

        with log_time(f"[{ean}] generate desc + process img total"):
            description_result, pics_result = await asyncio.gather(
                description_task, pics_task, return_exceptions=True
            )

        description_fallback = (
            f"{article}. Size: {size}. Color: {color}. Material: {material}."
        )
        if isinstance(description_result, Exception):
            log(
                f"generate_description failed for EAN {ean}: {description_result}. Using fallback description.",
                save=True,
                level="warning",
            )
            description = description_fallback
        else:
            description = description_result

        if isinstance(pics_result, Exception):
            log(
                f"process_pics failed for EAN {ean}: {pics_result}. Falling back to source image URLs.",
                save=True,
                level="warning",
            )
            pics = total
        elif isinstance(pics_result, list) and pics_result:
            pics = pics_result
        else:
            log(
                f"process_pics returned no images for EAN {ean}. Falling back to source image URLs.",
                save=True,
                level="warning",
            )
            pics = total
        log(
            f"preprocessing completed ean={ean} description_len={len(str(description))} pics_count={len(pics)} fallback_desc={isinstance(description_result, Exception)} fallback_pics={not (isinstance(pics_result, list) and bool(pics_result))}",
            save=True,
            level="info",
        )

        height = format_si_dimension(elem.get("height"))
        length = format_si_dimension(elem.get("length"))
        width = format_si_dimension(elem.get("width"))

        # Получаем SSH клиент
        # with log_time("Connection to ssh"):
        #     ssh_client = self._get_ssh_client()

        # # Run independent stage calls in parallel to reduce per-EAN latency.
        seo_task = asyncio.create_task(
            generate_seo(article=article, size=size, color=color, material=material)
        )
        # adapt_task = asyncio.create_task(
        # adapt_html_description(ean, description, ssh_client, controller)
        # )
        adapt_task = None
        log(f"starting html description adaptation ean={ean}", save=True, level="info")
        
        if elem.get("I_stammartikel"):
            log("<Body fetch...>", save=True, level="info")
            if self._aftercool_circuit_open():
                remaining = max(0.0, self.aftercool_circuit_open_until - time.monotonic())
                log(
                    f"aftercool circuit open ean={ean} remaining_sec={round(remaining, 1)}; skipping html fetch",
                    save=True,
                    level="warning",
                )
                session = None
            else:
                session = await self._get_session()

            if session is None:
                log(
                    f"aftercool session unavailable ean={ean}; continuing with fallback description",
                    save=True,
                    level="warning",
                )
                self._aftercool_record_failure()
                session = None

            if session:
                log(
                    "Fetching product data for HTML description adaptation...",
                    save=True,
                    level="info",
                )
                response: dict[str, Any] = await self._get_product(
                    product_id=elem["I_stammartikel"], session_cookie=session
                )
                log(
                    f"aftercool fetch completed ean={ean} success={bool(response.get('success'))}",
                    save=True,
                    level="info",
                )
                if response.get("success"):
                    self._aftercool_record_success()
                    items = response.get("items")
                    if not isinstance(items, list) or not items:
                        payload = response.get("payload")
                        if isinstance(payload, dict):
                            payload_items = payload.get("items")
                            if isinstance(payload_items, list):
                                items = payload_items

                    first_item = items[0] if isinstance(items, list) and items else {}
                    row = first_item.get("row") if isinstance(first_item, dict) else {}
                    html = row.get("Beschreibung") if isinstance(row, dict) else ""

                    if html:
                        log(
                            f"Aftercool Beschreibung received for EAN {ean} (len={len(str(html))})",
                            save=True,
                            level="info",
                        )
                        adapt_task = asyncio.create_task(
                            adapt_html_description_v2(html=html, description=description)
                        )
                    else:
                        log(
                            f"Aftercool returned empty/malformed Beschreibung for EAN {ean}; using generated description fallback",
                            save=True,
                            level="warning",
                        )
                else:
                    log(
                        f"aftercool fetch failed ean={ean} response={response}; continuing with fallback description",
                        save=True,
                        level="warning",
                    )
                    self._aftercool_record_failure()
        else:
            log("No I_stammartikel, skipping HTML description adaptation...", save=True, level="info")

        category_task = asyncio.create_task(
            self._category_selector(article, price, ean, description)
        )
        adapt_result = None
        category_result = None
        if adapt_task:
            log(
                "Waiting for description adaptation and category selection...",
                save=True,
                level="info",
            )
            adapt_result, category_result = await asyncio.gather(
                adapt_task, category_task, return_exceptions=True
            )
        else:
            log("Waiting for category selection...", save=True, level="info")
            adapt_result = description
            category_result = await category_task
        if isinstance(adapt_result, Exception) or adapt_result is None:
            log(
                f"HTML description adaptation failed for EAN {ean}: {adapt_result}",
            )
            new_webtag = description
        else:
            new_webtag = adapt_result

        if isinstance(category_result, Exception):
            if not seo_task.done():
                seo_task.cancel()
            return self._fail_upload(
                f"category selection failed ean={ean} error={category_result}"
            )

        if isinstance(category_result, BaseException):
            return self._fail_upload("Critical error during category selection")

        category_id, category_name = category_result
        log(
            f"category selected ean={ean} category_id={category_id} category_name={category_name}",
            save=True,
            level="info",
        )

        if category_id is None:
            return self._fail_upload(f"ERROR WITH CATEGORY ID IS NONE: EAN: {ean}")    

        try:
            seo_list = await seo_task
        except Exception as e:
            log(f"generate_seo failed for EAN {ean}: {e}")
            seo_list = []
        log(
            f"seo completed ean={ean} seo_items={len(seo_list) if isinstance(seo_list, list) else 0}",
            save=True,
            level="info",
        )
        log(f"description prepared ean={ean} len={len(str(new_webtag))}")
        attributes = {
            "title": [article],
            "description": [new_webtag],
            "ean": [ean],
            "picture": pics,
            "category": [category_name],
            "category_detail": {
                "id": category_id,
                "title": category_name,
                "name": category_name,
            },
            "short_description": seo_list,
        }

        optional_fields = {
            "colour": self._as_scalar_string(color),
            "height": height,
            "length": length,
            "width": width,
            "material": (
                self._as_scalar_string(material)
                if self._as_scalar_string(material) not in ["Stoff", "Textil"]
                else "Synthetic"
            ),
            "material_composition": "No information required",
            "abnehmbarer_bezug": "No",
            "parts_of_animal_origin": "Yes" if self._as_scalar_string(material) == "Leder" else "No",
        }

        for key, value in optional_fields.items():
            if value:
                attributes[key] = value

        json_body = {"ean": [ean], "attributes": attributes}

        if self.version == "jv":
            json_body["attributes"]["manufacturer"] = JV_MANUFACTURER
            json_body["attributes"]["product_safety_contact"] = JV_CONTACT_DATA
        else:
            json_body["attributes"]["manufacturer"] = XL_MANUFACTURER
            json_body["attributes"]["product_safety_contact"] = XL_CONTACT_DATA

        url = f"{self.base_api_url}/product-data?locale=de-DE"

        normalized_item = self._build_item_payload(elem)
        log(
            f"payload ready ean={ean} title_len={len(str(article))} desc_len={len(str(new_webtag))} pics={len(pics)}",
            save=True,
            level="info",
        )

        try:
            log(
                "Sending request to Kaufland API to create or update product...",
                save=True,
                level="info",
            )
            result = await self._universal_request("PUT", url, json_body)
            if isinstance(result, dict):
                log(
                    f"kaufland product-data response ean={ean} keys={list(result.keys())[:8]} data={result.get('data')}",
                    save=True,
                    level="info",
                )
            else:
                log(
                    f"kaufland product-data response non-dict ean={ean} type={type(result).__name__}",
                    save=True,
                    level="error",
                )

        except Exception as e:
            return self._fail_upload(
                f"Request to Kaufland API failed for EAN {ean}: {e}"
            )

        if not isinstance(result, dict):
            return self._fail_upload(
                f"Kaufland product-data returned non-dict response ean={ean} response={result}"
            )

        if result.get("data") == "Content created or replaced":
            try:
                unit_ids = await self._fetch_unit_id_by_ean(ean)
                log(
                    f"fetched unit_ids ean={ean} storefront_count={len(unit_ids) if isinstance(unit_ids, dict) else 0}",
                    save=True,
                    level="info",
                )
            except Exception as e:
                return self._fail_upload(f"fetch unit_ids failed ean={ean} error={e}")

            if unit_ids:
                log(
                    "Product already exists, updating price for existing unit_ids...",
                    save=True,
                    level="info",
                )
                target_unit_ids = {
                    storefront: unit_id
                    for storefront, unit_id in unit_ids.items()
                    if storefront in target_storefronts
                }
                update_result = await self._update_price_by_unit_id(
                    ean,
                    price,
                    target_unit_ids,
                    delivery=delivery,
                    amount=amount,
                )

                missing_storefronts = [
                    storefront
                    for storefront in target_storefronts
                    if storefront not in unit_ids
                ]
                add_result = True
                if missing_storefronts:
                    log(
                        f"adding missing unit_ids ean={ean} missing_storefronts={missing_storefronts}",
                        save=True,
                        level="info",
                    )
                    add_result = await self._add_unit_id(
                        ean,
                        price,
                        delivery,
                        target_storefronts=missing_storefronts,
                        id_offer=id_offer,
                        amount=amount,
                    )

                overall_result = update_result and add_result
                log(
                    f"final existing-product result ean={ean} update_result={update_result} add_result={add_result} overall={overall_result}",
                    save=True,
                    level="info",
                )
                return overall_result
            else:
                log("Fresh product, adding unit_id...", save=True, level="info")
                result = await self._add_unit_id(
                    ean,
                    price,
                    delivery,
                    target_storefronts=target_storefronts,
                    id_offer=id_offer,
                    amount=amount,
                )
                log(f"final new-product result ean={ean} add_unit_result={result}", save=True, level="info")
                return result
        else:
            return self._fail_upload(
                f"Failed to create or replace product for EAN {ean}: {result}"
            )

    async def delete_all_products(self, eans, job_id: str | None = None):
        """
        Удаление товаров по их EAN со всех сайтов в kaufland
        """
        logs_dir = os.path.join(settings.MEDIA_ROOT, "logs")
        logs_file_path = os.path.join(logs_dir, "delete_products_logs.csv")

        # Проверка существования файла
        if os.path.exists(logs_file_path):
            os.remove(logs_file_path)

        clean_eans = [ean for ean in eans if ean and ean != "nan"]
        total = len(clean_eans)
        if job_id:
            await clear_cancel(job_id)
            await self._send_task_progress(
                job_id,
                "delete",
                "job_started",
                payload={"total": total, "controller": self.version},
            )

        if not clean_eans:
            return True

        processed = 0
        result_list = []
        failed_eans = []
        success_count = 0
        error_count = 0
        last_progress_emit_at = None
        step = 10
        if job_id:
            await register_running_job(job_id)
        try:
            for elem in range(0, len(clean_eans), step):
                if await is_cancelled(job_id):
                    if job_id:
                        await self._send_task_progress(
                            job_id,
                            "delete",
                            "job_failed",
                            payload={
                                "total": total,
                                "processed": processed,
                                "success": success_count,
                                "error": error_count,
                                "failed_eans": failed_eans,
                            },
                            info="stopped",
                        )
                    return False
                batch_eans = clean_eans[elem : elem + step]
                tasks = [
                    self._delete_product_by_unit_id(str(int(float(ean))), job_id=job_id)
                    for ean in batch_eans
                ]
                batch_result = await asyncio.gather(*tasks)
                for ean, ok in zip(batch_eans, batch_result):
                    processed += 1
                    result_list.append(ok)
                    normalized_ean = str(int(float(ean)))
                    if not ok:
                        error_count += 1
                        failed_eans.append(normalized_ean)
                    else:
                        success_count += 1
                    if job_id:
                        if self._should_emit_progress(
                            processed=processed,
                            total=total,
                            last_emit_at=last_progress_emit_at,
                        ):
                            await self._send_task_progress(
                                job_id,
                                "delete",
                                "job_progress",
                                payload={
                                    "total": total,
                                    "processed": processed,
                                    "success": success_count,
                                    "error": error_count,
                                    "ean": normalized_ean,
                                    "status": "success" if ok else "failed",
                                },
                            )
                            last_progress_emit_at = time.monotonic()
        except asyncio.CancelledError:
            if job_id:
                await self._send_task_progress(
                    job_id,
                    "delete",
                    "job_failed",
                    payload={
                        "total": total,
                        "processed": processed,
                        "success": success_count,
                        "error": error_count,
                        "failed_eans": failed_eans,
                    },
                    info="stopped",
                )
            return False
        finally:
            if job_id:
                await unregister_running_job(job_id)

        status_result = all(result_list)
        if job_id:
            await self._send_task_progress(
                job_id,
                "delete",
                "job_completed",
                payload={
                    "total": total,
                    "processed": processed,
                    "success": success_count,
                    "failed": error_count,
                    "failed_eans": failed_eans,
                },
                info="success" if status_result else "partial_or_failed",
            )
        return status_result

    async def update_price(self, eans_and_prices):
        """
        Функция для обновления цен по EAN с уже указанным price
        """
        logs_dir = os.path.join(settings.MEDIA_ROOT, "logs")
        logs_file_path = os.path.join(logs_dir, "update_price_logs.csv")

        # Проверка существования файла
        if os.path.exists(logs_file_path):
            os.remove(logs_file_path)

        tasks = [
            self._update_price_by_unit_id(str(int(float(ean))), price)
            for ean, price in eans_and_prices.items()
            if ean and ean != "nan"
        ]
        status_result = await self.status_processing(tasks)
        return status_result

    async def products_checker(self, eans, job_id: str | None = None):
        """
        Чекер продуктов выводит их ean, title, price
        """
        results = []
        clean_eans = [ean for ean in eans if ean and ean != "nan"]
        total = len(clean_eans)
        if job_id:
            await clear_cancel(job_id)
            await self._send_task_progress(
                job_id,
                "checker",
                "job_started",
                payload={"total": total, "controller": self.version},
            )
        if not clean_eans:
            if job_id:
                await self._send_task_progress(
                    job_id,
                    "checker",
                    "job_completed",
                    payload={
                        "total": total,
                        "processed": 0,
                        "found": 0,
                        "not_found": 0,
                        "results": [],
                    },
                )
            return results

        processed = 0
        found_count = 0
        not_found_count = 0
        last_progress_emit_at = None
        if job_id:
            await register_running_job(job_id)
        try:
            for elem in range(0, len(clean_eans), 10):
                if await is_cancelled(job_id):
                    if job_id:
                        await self._send_task_progress(
                            job_id,
                            "checker",
                            "job_failed",
                            payload={
                                "total": total,
                                "processed": processed,
                                "success": found_count,
                                "error": 0,
                                "not_found": not_found_count,
                                "results": results,
                            },
                            info="stopped",
                        )
                    return results
                batch_eans = clean_eans[elem : elem + 10]
                normalized_batch_eans = [str(int(float(ean))) for ean in batch_eans]
                tasks = [
                    self._check_product_by_unit_id(normalized_ean, job_id=job_id)
                    for normalized_ean in normalized_batch_eans
                ]
                result = await asyncio.gather(*tasks, return_exceptions=True)
                for normalized_ean, item in zip(normalized_batch_eans, result):
                    if isinstance(item, Exception):
                        log(
                            f"products_checker failed and stopped ean={normalized_ean} error={item}",
                        )
                        if job_id:
                            await self._send_task_progress(
                                job_id,
                                "checker",
                                "job_failed",
                                payload={
                                    "total": total,
                                    "processed": processed,
                                    "failed_ean": normalized_ean,
                                    "results": results,
                                },
                                info=str(item),
                            )
                        raise RuntimeError(
                            f"products_checker failed for ean={normalized_ean}: {item}"
                        ) from item
                    results.append(item)
                    if item.get("found"):
                        found_count += 1
                    else:
                        not_found_count += 1
                    processed += 1
                    if job_id:
                        if self._should_emit_progress(
                            processed=processed,
                            total=total,
                            last_emit_at=last_progress_emit_at,
                        ):
                            await self._send_task_progress(
                                job_id,
                                "checker",
                                "job_progress",
                                payload={
                                    "total": total,
                                    "processed": processed,
                                    "success": processed,
                                    "error": 0,
                                    "ean": normalized_ean,
                                },
                            )
                            last_progress_emit_at = time.monotonic()
        except asyncio.CancelledError:
            if job_id:
                await self._send_task_progress(
                    job_id,
                    "checker",
                    "job_failed",
                    payload={
                        "total": total,
                        "processed": processed,
                        "success": found_count,
                        "error": 0,
                        "not_found": not_found_count,
                        "results": results,
                    },
                    info="stopped",
                )
            return results
        finally:
            if job_id:
                await unregister_running_job(job_id)
        if job_id:
            await self._send_task_progress(
                job_id,
                "checker",
                "job_completed",
                payload={
                    "total": total,
                    "processed": processed,
                    "found": found_count,
                    "not_found": not_found_count,
                    "results": results,
                },
            )
        return results

    async def upload_via_json(
        self, data: list[dict], job_id: str | None = None
    ) -> bool | None:
        """
        Функция для загрузки товаров на Kaufland через JSON
        Arguments:
            data (list[dict]): Список товаров в формате словаря
        Returns:
            optional[bool]:
                - True: все товары успешно загружены
                - False: загружены не все товары
                - None: все товары не загружены
        """
        # Компилируем регулярное выражение для поиска ключевых слов
        pattern = re.compile(r"(delet\w*|remove\w*|l[oö]sch\w*)", re.IGNORECASE)

        # Сохраняем исходную длину данных
        original_len = len(data)

        # Фильтруем данные, исключая элементы, содержащие ключевые слова
        safe_data = [elem for elem in data if isinstance(elem, dict)]
        removed_invalid_items = len(data) - len(safe_data)
        data = [
            elem
            for elem in safe_data
            if not pattern.search(str(elem.get("article", "")))
        ]

        # Keep only unique EANs from the start.
        unique_data: list[dict] = []
        seen_eans: set[str] = set()
        removed_duplicates = 0
        for item in data:
            ean_value = str(item.get("ean") or "").strip()
            if not ean_value:
                unique_data.append(item)
                continue
            if ean_value in seen_eans:
                removed_duplicates += 1
                continue
            seen_eans.add(ean_value)
            unique_data.append(item)
        data = unique_data

        result_list = []
        concurrency = self.upload_batch_size

        skipped_count = original_len - len(data)

        # Если были удалены элементы, фиксируем это в логах и прогрессе.
        if removed_invalid_items:
            log(f"upload skipped invalid items count={removed_invalid_items}")
        if removed_duplicates:
            log(f"upload skipped duplicate eans count={removed_duplicates}")

        if not job_id:
            job_id = uuid4().hex
        await clear_cancel(job_id)
        logs_dir = os.path.join(settings.MEDIA_ROOT, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        checkpoint_path = os.path.join(logs_dir, f"upload_checkpoint_{job_id}.json")
        failed_report_path = os.path.join(logs_dir, f"upload_failed_{job_id}.json")

        total = len(data)

        success_count = 0
        error_count = 0
        processed_count = 0
        failed_eans = []
        failed_items: list[dict[str, Any]] = []
        started_at = time.time()
        last_done_at = time.monotonic()
        last_heartbeat_at = 0.0

        last_progress_emit_at = None
        work_queue: asyncio.Queue = asyncio.Queue()
        done_queue: asyncio.Queue = asyncio.Queue()
        for item in data:
            await work_queue.put(item)
        worker_count = min(concurrency, total) if total > 0 else 0
        for _ in range(worker_count):
            await work_queue.put(None)

        def _write_checkpoint() -> None:
            remaining = max(0, total - processed_count)
            payload = {
                "job_id": job_id,
                "controller": self.version,
                "total": total,
                "processed": processed_count,
                "success": success_count,
                "error": error_count,
                "remaining": remaining,
                "failed_eans": failed_eans,
                "updated_at": datetime.utcnow().isoformat() + "Z",
            }
            try:
                with open(checkpoint_path, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
            except Exception as e:
                log(
                    f"checkpoint write failed job_id={job_id} path={checkpoint_path} error_type={type(e).__name__} error={e!r}",
                    save=True,
                    level="error",
                )

        def _write_failed_report() -> None:
            payload = {
                "job_id": job_id,
                "controller": self.version,
                "created_at": datetime.utcnow().isoformat() + "Z",
                "total": total,
                "processed": processed_count,
                "success": success_count,
                "error": error_count,
                "failed_count": len(failed_items),
                "failed_items": failed_items,
            }
            try:
                with open(failed_report_path, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
            except Exception as e:
                log(
                    f"failed report write failed job_id={job_id} path={failed_report_path} error_type={type(e).__name__} error={e!r}",
                    save=True,
                    level="error",
                )

        async def run_upload_worker():
            item_retry_attempts = max(1, int(os.getenv("UPLOAD_ITEM_RETRY_ATTEMPTS", "2")))

            def _is_timeout_like(exc: Exception) -> bool:
                timeout_names = {
                    "TimeoutError",
                    "ReadTimeout",
                    "ConnectTimeout",
                    "ClientTimeoutError",
                    "ServerTimeoutError",
                }
                return type(exc).__name__ in timeout_names

            while True:
                item = await work_queue.get()
                if item is None:
                    work_queue.task_done()
                    return

                ok = False
                ean = None
                timeout_error: Exception | None = None
                for attempt in range(1, item_retry_attempts + 1):
                    try:
                        if await is_cancelled(job_id):
                            ok = False
                            break
                        if isinstance(item, dict):
                            raw_ean = item.get("ean")
                            ean = str(raw_ean).strip() if raw_ean is not None else None
                        ok = bool(await self._create_product(item, job_id, self.version))
                        break
                    except Exception as e:
                        timeout_error = e
                        if _is_timeout_like(e) and attempt < item_retry_attempts:
                            delay = min(10, (2 ** attempt))
                            log(
                                f"upload retry timeout ean={ean} attempt={attempt}/{item_retry_attempts} delay_s={delay} error_type={type(e).__name__}",
                                save=True,
                                level="warning",
                            )
                            await asyncio.sleep(delay)
                            continue
                        log(
                            f"upload unexpected error ean={ean} attempt={attempt}/{item_retry_attempts} error_type={type(e).__name__} error={e!r}",
                            save=True,
                            level="error",
                        )
                        ok = False
                        break
                if timeout_error and not ok and _is_timeout_like(timeout_error):
                    log(
                        f"upload item failed after timeout retries ean={ean} attempts={item_retry_attempts}",
                        save=True,
                        level="warning",
                    )
                await done_queue.put((item, ok))
                work_queue.task_done()

        if job_id:
            await register_running_job(job_id)
        workers = [
            asyncio.create_task(run_upload_worker()) for _ in range(worker_count)
        ]
        try:
            while processed_count < total:
                if await is_cancelled(job_id):
                    for worker in workers:
                        worker.cancel()
                    if workers:
                        await asyncio.gather(*workers, return_exceptions=True)
                        
                    return False
                now = time.monotonic()
                if now - last_heartbeat_at >= _UPLOAD_HEARTBEAT_SEC:
                    queued = work_queue.qsize()
                    done_pending = done_queue.qsize()
                    elapsed = round(time.time() - started_at, 1)
                    log(
                        f"upload heartbeat job_id={job_id} processed={processed_count}/{total} success={success_count} error={error_count} queue={queued} done_queue={done_pending} elapsed_s={elapsed}",
                        save=True,
                        level="info",
                    )
                    last_heartbeat_at = now
                if now - last_done_at > _UPLOAD_STUCK_TIMEOUT_SEC:
                    queued = work_queue.qsize()
                    done_pending = done_queue.qsize()
                    log(
                        f"upload stuck watchdog triggered job_id={job_id} no_progress_for_sec={round(now-last_done_at,1)} queue={queued} done_queue={done_pending}",
                        save=True,
                        level="error",
                    )
                    for worker in workers:
                        worker.cancel()
                    if workers:
                        await asyncio.gather(*workers, return_exceptions=True)
                    _write_checkpoint()
                    _write_failed_report()
                    return False
                try:
                    item, ok = await asyncio.wait_for(done_queue.get(), timeout=0.25)
                    
                except asyncio.TimeoutError:
                    continue

                result_list.append(ok)
                processed_count += 1
                last_done_at = time.monotonic()
                current_ean = item.get("ean") if isinstance(item, dict) else None
                
                if ok:
                    success_count += 1
                    
                else:
                    error_count += 1
                    if current_ean is not None:
                        failed_eans.append(str(current_ean))
                    failed_items.append(
                        {
                            "ean": str(current_ean) if current_ean is not None else None,
                            "item": item if isinstance(item, dict) else None,
                            "failed_at": datetime.utcnow().isoformat() + "Z",
                        }
                    )

                log(
                    f"upload progress processed={processed_count}/{total} success={success_count} error={error_count}",
                    save=True,
                    level="info",
                )
                _write_checkpoint()
                    
            if workers:
                await asyncio.gather(*workers, return_exceptions=True)
                
        except asyncio.CancelledError:
            for worker in workers:
                worker.cancel()
                
            if workers:
                await asyncio.gather(*workers, return_exceptions=True)
            _write_checkpoint()
            _write_failed_report()
            return False
        
        finally:
            if job_id:
                await unregister_running_job(job_id)
        _write_checkpoint()
        _write_failed_report()
        log(
            f"upload artifacts job_id={job_id} checkpoint={checkpoint_path} failed_report={failed_report_path}",
            save=True,
            level="info",
        )
                
        if all(result_list):  # Все успешно
            return True
        
        elif any(result_list) and not all(result_list):  # Частично успешно
            return False

        elif not any(result_list):  # Все неуспешно
            return None

    async def get_product_by_ean(self, ean: str):
        """
        Получить информацию о товаре по его EAN в читаемом формате:
        один объект на storefront с объединенным data:
        main product data + embedded entities.
        """

        base_url = f"{self.base_api_url}/product-data/{ean}?locale=de-DE"
        additional_url = (
            f"{self.base_api_url}/products/ean/{ean}?storefront=de&embedded=units"
        )

        main_data = None
        additional_data = None

        main_data = await self._universal_request("GET", base_url)
        additional_data = await self._universal_request("GET", additional_url)

        main_attributes = main_data.get("data").get("attributes")

        units = additional_data.get("data").get("units")
        prices = map(lambda unit: unit.get("price"), units)

        response_data = {}

        for key, value in main_attributes.items():
            if not value:
                response_data[key] = [None]
            elif isinstance(value, list):
                response_data[key] = value
            else:
                response_data[key] = [value]
        response_data["price"] = prices
        response_data["units"] = units

        return {"response_data": response_data}


async def main():
    async with aiohttp.ClientSession() as session:
        cnt = KauflandController(session, version="jv")
        ean_dict = {
            "4067282748224": 4999,
            "4067282749382": 4999,
            "4067282736344": 4999,
            "4067282749030": 4999,
            "4067282748941": 4999,
            "4067282749214": 4999,
            "4067282736887": 4999,
            "4067282749290": 4999,
            "4067282749122": 4999,
            "4067282748316": 4999,
            "4067282736795": 4999,
            "4067282748132": 4999,
            "4067282737150": 4999,
            "4067282736078": 4999,
            "4067282736160": 4999,
            "4067282748859": 4999,
            "4067282737426": 4999,
            "4067282740051": 4999,
            "4067282748767": 4999,
            "4067282740310": 4999,
            "4067282735613": 4999,
            "4067282731301": 4999,
            "4067282740228": 4999,
            "4067282736528": 4999,
            "4067282737068": 4999,
            "4067282748675": 4999,
            "4067282736610": 4999,
            "4067282736252": 4999,
            "4067282735354": 4999,
            "4067282737242": 4999,
            "4067282737334": 4999,
            "4067282736702": 4999,
            "4067282735897": 4999,
            "4062292279850": 4999,
            "4067282735262": 4999,
            "4062292723353": 4999,
            "4062292419706": 4999,
            "4067282498785": 4999,
            "4067282497658": 4999,
            "4067282740150": 4999,
            "4067282736009": 4999,
            "4067282736993": 4999,
        }
        settings.configure()

        # for ean, price in ean_dict.items():
        #     result = await cnt._add_unit_id(ean, price, 28)
        #     log(result)
        # https://sellerapi.kaufland.com/v2/units/387978729501?storefront=de&embedded=battery_participation


if __name__ == "__main__":
    asyncio.run(main())
