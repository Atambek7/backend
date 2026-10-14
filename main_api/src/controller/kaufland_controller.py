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
from typing import Any, Optional, Dict, List, Tuple, Set
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
    storefronts_by_account,
    get_account_storefronts,
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
from main_api.src.utils.ean_utils import normalize_ean as _utils_normalize_ean
from main_api.src.utils.price_utils import format_price_cents as _utils_format_price_cents
from main_api.src.utils.cache_utils import BoundedLRUCache
from main_api.src.utils.text_utils import (
    sanitize_title,
    sanitize_description,
    is_blacklisted_category,
    resolve_fallback_category,
    generate_localized_description,
)
import config
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
        self.base_api_url = config.KAUFLAND_API_BASE_URL
        self._ssh_client = None  # Коннекшн к нашему серверу

        # Ограниченный по памяти LRU-кэш с TTL (3600 сек) для предотвращения утечек памяти при массовой обработке
        self._ean_cache = {
            "unit_ids": BoundedLRUCache(maxsize=5000, ttl_sec=3600),
            "product_ids": BoundedLRUCache(maxsize=5000, ttl_sec=3600),
            "main_data": BoundedLRUCache(maxsize=5000, ttl_sec=3600),
        }

        self.aftercool_base_url = config.AFTERCOOL_BASE_URL
        self._aftercool_session: str | None = None
        self.aftercool_failures = 0
        self.aftercool_circuit_open_until = 0.0
        self.last_upload_error = None

    def get_account_storefronts(self) -> list[str]:
        """Возвращает список витрин для текущего аккаунта (jv или xl)."""
        return get_account_storefronts(self.version)

    @staticmethod
    def normalize_ean(value: Any) -> str:
        """
        Нормализует EAN, сохраняя ведущие нули и очищая от .0, пробелов и мусора.
        """
        return _utils_normalize_ean(value)

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

    async def _get_session(self, force_refresh: bool = False) -> str | None:
        if self._aftercool_session and not force_refresh:
            return self._aftercool_session

        username = os.getenv("AFTERCOOL_USERNAME", config.AFTERCOOL_USERNAME)
        password = os.getenv("AFTERCOOL_PASSWORD", config.AFTERCOOL_PASSWORD)
        aftercool = AftercoolAuthService(
            self.aftercool_base_url, username=username, password=password
        )
        auth = await aftercool.authenticate()

        if auth.get("success"):
            log("Session obtained from aftercool", save=True, level="success")
            self._aftercool_session = auth.get("session")
            return self._aftercool_session
        else:
            log("Failed session obtain from aftercool", save=True, level="error")
            self._aftercool_session = None
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
        return config.EXCHANGE_RATES.get(currency.upper(), 1.0)

    @staticmethod
    def format_price(price: float) -> int:
        """
        Конвертирует цену в центы (например, 1000.00 EUR -> 100000 центов).
        """
        return _utils_format_price_cents(price)

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
            # 1. Primary: query merchant inventory by id_offer
            url = f"{self.base_api_url}/units?storefront={storefront}&id_offer={ean}"
            result = await self._universal_request(method="GET", url=url)
            if isinstance(result, dict) and result.get("data"):
                return storefront, result["data"][0].get("id_unit")

            # 2. Fallback: query merchant inventory by ean
            url_ean = f"{self.base_api_url}/units?storefront={storefront}&ean={ean}"
            result_ean = await self._universal_request(method="GET", url=url_ean)
            if isinstance(result_ean, dict) and result_ean.get("data"):
                return storefront, result_ean["data"][0].get("id_unit")

            # 3. Fallback: check public catalog with embedded units
            url_cat = f"{self.base_api_url}/products/ean/{ean}?storefront={storefront}&embedded=units"
            result_cat = await self._universal_request(method="GET", url=url_cat)
            if isinstance(result_cat, dict):
                data = result_cat.get("data") or {}
                units = data.get("units") or []
                if units:
                    return storefront, units[0].get("id_unit")

            return storefront, None

        final_res = {}
        target_sfs = self.get_account_storefronts()
        try:
            fetched = await asyncio.gather(
                *(fetch_storefront_unit_id(storefront) for storefront in target_sfs)
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

        target_sfs = self.get_account_storefronts()
        fetched = await asyncio.gather(
            *(fetch_storefront_product_id(storefront) for storefront in target_sfs)
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

    async def _get_shipping_group(self, storefront: str) -> Optional[int]:
        if not hasattr(self, "_shipping_group_cache"):
            self._shipping_group_cache = {}
        if storefront in self._shipping_group_cache:
            return self._shipping_group_cache[storefront]

        try:
            url = f"{self.base_api_url}/shipping-groups?storefront={storefront}"
            res = await self._universal_request("GET", url)
            groups = res.get("data", []) if isinstance(res, dict) else (res if isinstance(res, list) else [])
            default_id = next(
                (g.get("id_shipping_group") for g in groups if isinstance(g, dict) and g.get("is_default") and g.get("id_shipping_group")),
                None
            )
            if not default_id and groups:
                first = groups[0]
                if isinstance(first, dict):
                    default_id = first.get("id_shipping_group")
            self._shipping_group_cache[storefront] = default_id
            return default_id
        except Exception as e:
            log(f"Failed to fetch shipping groups for storefront {storefront}: {e}", level="warning")
            return None

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
            target_storefronts = self.get_account_storefronts()
        if amount is None:
            amount = 20
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
            sg_id = await self._get_shipping_group(storefront)
            if sg_id:
                body["id_shipping_group"] = sg_id

            # Ретраи на случай асинхронной задержки индексации карточки товара в каталоге Kaufland
            max_index_retries = 2
            result = None
            for idx_attempt in range(max_index_retries + 1):
                result = await self._universal_request("POST", url, body=body)
                if not isinstance(result, dict):
                    return self._fail_upload(
                        f"add_unit failed ean={ean} storefront={storefront} response_type={type(result).__name__}"
                    )

                data = result.get("data") or {}
                if data.get("status", "") == "AVAILABLE":
                    return True

                err_text = str(result).lower()
                is_item_indexing = (
                    "parameter [item]" in err_text
                    or ("item" in err_text and "is missing or has wrong value" in err_text)
                )
                if is_item_indexing and idx_attempt < max_index_retries:
                    wait_sec = (idx_attempt + 1) * 3
                    log(
                        f"Catalog indexing delay for ean={ean} storefront={storefront}. Waiting {wait_sec}s before retry ({idx_attempt + 1}/{max_index_retries})...",
                        save=True,
                        level="warning",
                    )
                    await asyncio.sleep(wait_sec)
                    continue
                break

            if self._is_offer_already_exists_response(result):
                self._ean_cache["unit_ids"].pop(ean, None)
                unit_ids = await self._fetch_unit_id_by_ean(ean)
                unit_id = unit_ids.get(storefront) if isinstance(unit_ids, dict) else None
                if not unit_id:
                    # Direct query merchant inventory by id_offer for this storefront
                    inv_res = await self._universal_request(
                        method="GET",
                        url=f"{self.base_api_url}/units?storefront={storefront}&id_offer={id_offer}",
                    )
                    if isinstance(inv_res, dict) and inv_res.get("data"):
                        unit_id = inv_res["data"][0].get("id_unit")
                if not unit_id:
                    # Fallback query merchant inventory by ean for this storefront
                    inv_res_ean = await self._universal_request(
                        method="GET",
                        url=f"{self.base_api_url}/units?storefront={storefront}&ean={ean}",
                    )
                    if isinstance(inv_res_ean, dict) and inv_res_ean.get("data"):
                        unit_id = inv_res_ean["data"][0].get("id_unit")
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
                if sg_id:
                    patch_payload["id_shipping_group"] = sg_id
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
            *(fetch_storefront_main_data(storefront) for storefront in self.get_account_storefronts())
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
        if new_unit_id is not None:
            if not new_unit_id:
                return True
            final_res = new_unit_id
        else:
            final_res = await self._fetch_unit_id_by_ean(ean)
            if not final_res:
                log(f"_update_price_by_unit_id: no units found for ean={ean}", save=True, level="warning")
                return False

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
            message = result.get("message") if isinstance(result, dict) else str(result)

            if message == "success":
                bool_list.append(True)
            else:
                self.last_upload_error = (
                    f"update unit failed ean={ean} storefront={storefront} "
                    f"unit_id={unit_id} response={result} payload={payload}"
                )
                log(self.last_upload_error, save=True, level="error")
                bool_list.append(False)

        return bool(bool_list) and all(bool_list)

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
        manufacturer = (
            JV_MANUFACTURER
            if getattr(self, "version", "jv") == "jv"
            else XL_MANUFACTURER
        )
        try:
            numeric_price = float(price or 0)
            price_cents = int(round(numeric_price * 100)) if numeric_price > 0 else 10000
        except (ValueError, TypeError):
            price_cents = 10000

        item_for_category_js = {
            "item": {
                "title": article,
                "description": description,
                "manufacturer": manufacturer,
            },
            "price": price_cents,
        }
        url_category = (
            f"{self.base_api_url}/categories/decide?storefront=de&locale=de-DE"
        )

        result_category = await self._universal_request(
            method="POST", url=url_category, body=item_for_category_js
        )
        if "data" not in result_category or not result_category["data"]:
            log(
                f"Category decide returned no data for EAN {ean}, article: {article}. Using fallback category...",
                level="warning",
            )
            fallback_id, fallback_name = resolve_fallback_category(article)
            return fallback_id, fallback_name

        category_id = result_category["data"][0]["id_category"]
        category_name = result_category["data"][0]["name"]

        if is_blacklisted_category(category_name, title=article):
            fallback_id, fallback_name = resolve_fallback_category(article)
            log(
                f"Category '{category_name}' (id={category_id}) is blacklisted for EAN {ean}. Overriding with fallback category {fallback_id} ('{fallback_name}').",
                level="warning",
            )
            return fallback_id, fallback_name

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

        elif not elem.get("material"):
            return self._fail_upload(f"ERROR WITH MATERIAL: 'material' is missing or empty for EAN {ean}")

        elif re.search(r"[а-яА-ЯёЁ]", str(elem.get("material") or "")):
            return self._fail_upload(
                f"ERROR WITH MATERIAL: 'material' must be in German for EAN {ean}. "
                f"Received Cyrillic value '{elem.get('material')}'. Allowed examples: 'Holz', 'Sperrholz', 'Textil', 'Leder', 'Metall', 'Kunststoff'."
            )

        log(
            f"validation passed ean={ean} article_present={bool(elem.get('article'))} price={elem.get('price')} pic_main_present={bool(elem.get('pic_main'))}",
            save=True,
            level="info",
        )

        article = elem["article"]
        size = elem["size"]
        color = elem["color"]
        material = elem["material"]
        mat_scalar = self._as_scalar_string(material) or ""
        article = sanitize_title(article, material=mat_scalar)

        raw_price = elem["price"]
        if isinstance(raw_price, str):
            raw_price = raw_price.replace(",", ".").strip()
        price = float(raw_price)

        raw_amount = elem.get("amount")
        if raw_amount is None:
            raw_amount = (
                elem.get("Menge")
                or elem.get("menge")
                or elem.get("Bestand")
                or elem.get("bestand")
                or elem.get("stock")
            )
        if raw_amount is not None and str(raw_amount).strip() != "":
            try:
                amount = int(float(str(raw_amount).strip().replace(",", ".")))
            except (ValueError, TypeError):
                amount = 20
        else:
            amount = 20

        id_offer = str(elem.get("id_offer") or ean)
        target_storefronts = elem.get("storefronts") or self.get_account_storefronts()
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


        # NOTE: Это тест: спрашивайте Алихан
        # if isinstance(material, list):
        #     material = ", ".join(material)

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

        if not (height and length and width):
            from main_api.src.servises.json_mapper import _extract_dimensions
            raw_size = elem.get("size") or elem.get("Maße") or size or ""
            dim_l, dim_w, dim_h = _extract_dimensions(raw_size)
            if not length and dim_l:
                length = format_si_dimension(dim_l)
            if not width and dim_w:
                width = format_si_dimension(dim_w)
            if not height and dim_h:
                height = format_si_dimension(dim_h)

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
                if not response.get("success") and (response.get("expired") or response.get("status_code") in (401, 403)):
                    log(
                        f"Aftercool session expired for ean={ean}; re-authenticating...",
                        save=True,
                        level="warning",
                    )
                    session = await self._get_session(force_refresh=True)
                    if session:
                        response = await self._get_product(
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
        mat_scalar = self._as_scalar_string(material) or ""
        article = sanitize_title(article, material=mat_scalar)
        attributes = {
            "title": [article],
            "description": [sanitize_description(new_webtag, material=mat_scalar)],
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

        norm_material = "Echtleder" if mat_scalar in ["Leder", "Echtleder"] else ("Textil" if mat_scalar in ["Stoff", "Textil", "Synthetic"] else mat_scalar)

        raw_mat_comp = str(elem.get("material_composition") or "").strip()
        if raw_mat_comp.lower() in ["no information required", "no information", "none", "null"]:
            raw_mat_comp = ""

        cat_lower = str(category_name or "").lower()
        is_upholstered = any(
            x in cat_lower for x in [
                "sofa", "sessel", "stuhl", "stühle", "bett", "couch", "polster",
                "hocker", "bank", "liege", "garnitur", "kissen", "matratze"
            ]
        ) or any(x in mat_scalar.lower() for x in ["stoff", "textil", "polyester", "leder", "kunstleder", "samts"])

        parts_animal = "Yes" if norm_material == "Leder" else (elem.get("parts_of_animal_origin") or "No")
        if is_upholstered and not raw_mat_comp:
            from main_api.services.material_service import resolve_material_attributes
            _, auto_comp, auto_animal = resolve_material_attributes(
                row=elem,
                title=str(article or ""),
                description=str(description or ""),
                cat_name=str(category_name or ""),
            )
            raw_mat_comp = auto_comp or "100% Polyester"
            if auto_animal and norm_material != "Leder":
                parts_animal = auto_animal

        optional_fields = {
            "colour": self._as_scalar_string(color),
            "height": height,
            "length": length,
            "width": width,
            "material": norm_material,
            "abnehmbarer_bezug": "No",
            "parts_of_animal_origin": parts_animal,
        }
        if raw_mat_comp:
            optional_fields["material_composition"] = raw_mat_comp

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
                    self._delete_product_by_unit_id(self.normalize_ean(ean), job_id=job_id)
                    for ean in batch_eans
                ]
                batch_result = await asyncio.gather(*tasks)
                for ean, ok in zip(batch_eans, batch_result):
                    processed += 1
                    result_list.append(ok)
                    normalized_ean = self.normalize_ean(ean)
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
            self._update_price_by_unit_id(self.normalize_ean(ean), price)
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
                normalized_batch_eans = [self.normalize_ean(ean) for ean in batch_eans]
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
                
        if not result_list:
            return None

        if all(result_list):  # Все успешно
            return True

        elif any(result_list):  # Частично успешно
            return False

        else:  # Все неуспешно
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

        main_attributes = {}
        if isinstance(main_data, dict) and isinstance(main_data.get("data"), dict):
            main_attributes = main_data.get("data", {}).get("attributes", {}) or {}

        units = []
        if isinstance(additional_data, dict) and isinstance(additional_data.get("data"), dict):
            units = additional_data.get("data", {}).get("units", []) or []
        prices = list(map(lambda unit: unit.get("price"), units))

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

    async def get_product_status(self, ean: str, storefront: str = "de") -> dict:
        """
        Проверить статус публикации товара и офферов в Kaufland:
        - Живой ли товар на витрине (is_live: true/false).
        - Флаг валидности карточки в каталоге Kaufland (is_valid: true/false).
        - Атрибуты карточки и наличие известных проблем (кириллица в материале, фиктивный состав, нет фото).
        - Статусы офферов по витринам.
        """
        clean_ean = str(ean).strip()

        target_storefront = (storefront or "de").strip().lower()
        if target_storefront in ["all", ""]:
            units_endpoint = f"{self.base_api_url}/units?ean={clean_ean}"
        else:
            units_endpoint = f"{self.base_api_url}/units?ean={clean_ean}&storefront={target_storefront}"

        units_resp = None
        try:
            units_resp = await self._universal_request("GET", units_endpoint)
        except Exception as exc:
            log(f"get_product_status units fetch failed ean={clean_ean} err={exc}", level="warning")

        units_list = []
        if isinstance(units_resp, dict) and isinstance(units_resp.get("data"), list):
            units_list = units_resp.get("data", [])

        # 1. Product-data attributes
        pdata_url = f"{self.base_api_url}/product-data/{clean_ean}?locale=de-DE"
        pdata_resp = None
        try:
            pdata_resp = await self._universal_request("GET", pdata_url)
        except Exception as exc:
            log(f"get_product_status product-data fetch failed ean={clean_ean} err={exc}", level="debug")

        has_pdata = isinstance(pdata_resp, dict) and isinstance(pdata_resp.get("data"), dict)
        pdata_attributes = pdata_resp.get("data", {}).get("attributes", {}) if has_pdata else {}

        # 2. General catalog product info (gives is_valid, main_picture, url, category)
        sf_for_prod = target_storefront if target_storefront not in ["all", ""] else "de"
        prod_url = f"{self.base_api_url}/products/ean/{clean_ean}?storefront={sf_for_prod}"
        prod_resp = None
        try:
            prod_resp = await self._universal_request("GET", prod_url)
        except Exception as exc:
            log(f"get_product_status product fetch failed ean={clean_ean} err={exc}", level="debug")

        has_prod = isinstance(prod_resp, dict) and isinstance(prod_resp.get("data"), dict)
        prod_data = prod_resp.get("data", {}) if has_prod else {}

        if not has_pdata and not has_prod and not units_list:
            return {
                "success": False,
                "ean": clean_ean,
                "controller": self.version,
                "status": "NOT_FOUND",
                "is_live": False,
                "summary": f"Product with EAN {clean_ean} was not found on Kaufland.",
                "storefronts": {},
                "attributes": {},
                "issues_detected": ["Product does not exist in Kaufland catalog (neither product-data, product, nor units found)."],
            }

        storefronts_info = {}
        any_is_live = False
        for u in units_list:
            sf = u.get("storefront") or "unknown"
            is_live = bool(u.get("is_live", False))
            if is_live:
                any_is_live = True

            raw_price = u.get("price")
            price_val = round(float(raw_price) / 100.0, 2) if raw_price is not None else None

            storefronts_info[sf] = {
                "is_live": is_live,
                "status": u.get("status"),
                "id_unit": u.get("id_unit"),
                "id_offer": u.get("id_offer"),
                "id_product": u.get("id_product"),
                "price": price_val,
                "amount": u.get("amount"),
                "handling_time": u.get("handling_time"),
                "date_inserted": u.get("date_inserted_iso"),
                "date_lastchange": u.get("date_lastchange_iso"),
            }

        issues = []
        parsed_attributes = {}

        # Catalog-level inspection
        if has_prod:
            catalog_title = prod_data.get("title")
            if catalog_title:
                parsed_attributes["title"] = catalog_title
            catalog_mfr = prod_data.get("manufacturer")
            if catalog_mfr:
                parsed_attributes["manufacturer"] = catalog_mfr
            main_pic = prod_data.get("main_picture")
            if main_pic:
                parsed_attributes["main_picture"] = main_pic
            cat_id = prod_data.get("id_category")
            if cat_id:
                parsed_attributes["category_id"] = cat_id

            is_valid_flag = prod_data.get("is_valid")
            if is_valid_flag is False:
                issues.append("Kaufland product catalog flag 'is_valid' is FALSE (product failed internal validation or requires attribute approval).")

            if not main_pic:
                issues.append("Product has no main image (main_picture is empty on Kaufland).")

        # Detailed product-data attributes inspection
        if has_pdata:
            title_val = pdata_attributes.get("title")
            title_str = title_val[0] if isinstance(title_val, list) and title_val else str(title_val or "")
            if title_str:
                parsed_attributes["title"] = title_str
            elif not parsed_attributes.get("title"):
                issues.append("Missing attribute 'title'.")

            cat_val = pdata_attributes.get("category")
            cat_str = cat_val[0] if isinstance(cat_val, list) and cat_val else str(cat_val or "")
            cat_detail = pdata_attributes.get("category_detail")
            if isinstance(cat_detail, list) and cat_detail:
                cat_name = cat_detail[0].get("title") or cat_str
            elif isinstance(cat_detail, dict):
                cat_name = cat_detail.get("title") or cat_str
            else:
                cat_name = cat_str
            if cat_name:
                parsed_attributes["category"] = cat_name

            mfr_val = pdata_attributes.get("manufacturer")
            mfr_str = mfr_val[0] if isinstance(mfr_val, list) and mfr_val else str(mfr_val or "")
            if mfr_str:
                parsed_attributes["manufacturer"] = mfr_str
            elif not parsed_attributes.get("manufacturer"):
                issues.append("Missing attribute 'manufacturer'.")

            pics_val = pdata_attributes.get("picture")
            pics_list = pics_val if isinstance(pics_val, list) else ([pics_val] if pics_val else [])
            parsed_attributes["pictures_count"] = len(pics_list)
            if not pics_list and not parsed_attributes.get("main_picture"):
                issues.append("Missing attribute 'picture' (no product images uploaded).")

            mat_val = pdata_attributes.get("material")
            mat_str = mat_val[0] if isinstance(mat_val, list) and mat_val else str(mat_val or "")
            parsed_attributes["material"] = mat_str
            if not mat_str:
                issues.append("Missing attribute 'material'.")
            elif re.search(r"[а-яА-ЯёЁ]", mat_str):
                issues.append(f"Attribute 'material' contains Cyrillic characters ('{mat_str}'). Kaufland requires German terms.")

            comp_val = pdata_attributes.get("material_composition")
            comp_str = comp_val[0] if isinstance(comp_val, list) and comp_val else str(comp_val or "")
            parsed_attributes["material_composition"] = comp_str

            cat_lower = (cat_name or "").lower()
            is_upholstered = any(
                x in cat_lower for x in [
                    "sofa", "sessel", "stuhl", "stühle", "bett", "couch", "polster",
                    "hocker", "bank", "liege", "garnitur", "kissen", "matratze"
                ]
            ) or any(x in (mat_str or "").lower() for x in ["stoff", "textil", "polyester", "leder", "kunstleder", "samts"])

            if is_upholstered:
                if not comp_str:
                    issues.append(f"Category '{cat_name}' is upholstered furniture and requires attribute 'material_composition' (e.g. '100% Polyester').")
                elif comp_str.lower() in ["no information required", "no information", "none", "null"]:
                    issues.append(f"Attribute 'material_composition' contains invalid placeholder '{comp_str}' which is rejected for upholstered category '{cat_name}'.")
        else:
            if units_list:
                issues.append("Product-data attributes (material, description, composition) not active in Kaufland catalog. Offer is held in draft/need-attributes state.")

        if any_is_live:
            final_status = "LIVE"
            summary_msg = "Product is LIVE and actively listed for buyers on Kaufland."
        elif issues:
            final_status = "BLOCKED"
            summary_msg = "Product is NOT LIVE (is_live: false) due to detected attribute validation issues."
        elif units_list and not any_is_live:
            final_status = "PENDING"
            summary_msg = "Product is submitted, but offer is not live yet (is_live: false). Kaufland is likely processing images or reviewing catalog placement."
        else:
            final_status = "INCOMPLETE"
            summary_msg = "Product exists in catalog, but no units (offers) found."

        result = {
            "success": True,
            "ean": clean_ean,
            "controller": self.version,
            "status": final_status,
            "is_live": any_is_live,
            "summary": summary_msg,
            "storefronts": storefronts_info,
            "attributes": parsed_attributes,
            "issues_detected": issues,
        }
        if has_prod and prod_data.get("url"):
            result["product_url"] = prod_data.get("url")
        if has_prod and "is_valid" in prod_data:
            result["is_valid"] = prod_data.get("is_valid")

        return result


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
