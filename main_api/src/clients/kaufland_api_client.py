"""
Единый канонический клиент для работы с Kaufland Seller API v2.
Централизует:
- HMAC-SHA256 подпись запросов для аккаунтов JV и XL
- Rate-limiting и обработку лимитов маркетплейса
- Экспоненциальный retry при сетевых и серверных ошибках (429, 5xx)
- Чистую обработку ответов без мусорных print и исключений
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import ssl
import time
from typing import Any, Optional

import aiohttp
import certifi
from django.conf import settings

import config
from main_api.src.utils.price_utils import format_price_cents

logger = logging.getLogger(__name__)

RETRYABLE_STATUSES = {408, 409, 425, 429, 500, 502, 503, 504}


class KauflandApiClient:
    """
    Канонический HTTP клиент с поддержкой HMAC подписи и ретраев.
    """

    def __init__(
        self,
        account: str,
        session: aiohttp.ClientSession,
        base_url: str | None = None,
        retries: int = 5,
        rate_limiter: Any = None,
    ):
        acc = str(account).strip().lower()
        if acc not in config.controllers_base:
            raise ValueError(f"Unknown Kaufland account: '{account}'. Expected 'jv' or 'xl'.")

        self.account = acc
        self.session = session
        self.base_url = (base_url or config.KAUFLAND_API_BASE_URL).rstrip("/")
        self.retries = max(1, retries)
        self.rate_limiter = rate_limiter

        self.client_key = config.controllers_base[self.account]["client_key"]
        self.secret_key = config.controllers_base[self.account]["secret_key"]
        self._ssl_context = ssl.create_default_context(cafile=certifi.where())

    @staticmethod
    def serialize_body(body: Any) -> str:
        """
        Компактная сериализация JSON без пробелов, критично для валидной HMAC подписи Kaufland.
        """
        if body is None or body == "":
            return ""
        if isinstance(body, (dict, list)):
            return json.dumps(body, separators=(",", ":"))
        return str(body)

    @staticmethod
    def sign_request(
        method: str,
        uri: str,
        body: str,
        timestamp: str | int,
        secret_key: str,
    ) -> str:
        """
        Генерирует SHA256 HMAC-подпись по спецификации Kaufland API.
        plain_text = METHOD\nURI\nBODY\nTIMESTAMP
        """
        plain_text = "\n".join([method.upper(), uri, str(body or ""), str(timestamp)])
        digest_maker = hmac.new(secret_key.encode("utf-8"), None, hashlib.sha256)
        digest_maker.update(plain_text.encode("utf-8"))
        return digest_maker.hexdigest()

    def get_headers(
        self,
        method: str,
        url: str,
        serialized_body: str = "",
        has_payload: bool = True,
    ) -> dict[str, str]:
        """Формирует обязательные заголовки Kaufland API."""
        timestamp = str(int(time.time()))
        signature = self.sign_request(
            method=method,
            uri=url,
            body=serialized_body,
            timestamp=timestamp,
            secret_key=self.secret_key,
        )
        headers = {
            "Accept": "application/json",
            "User-Agent": "Inhouse_development",
            "Shop-Timestamp": timestamp,
            "Shop-Client-Key": self.client_key,
            "Shop-Signature": signature,
        }
        if has_payload:
            headers["Content-Type"] = "application/json"
        return headers

    def calculate_storefront_price(self, storefront: str, base_eur_price: float) -> int:
        """
        Рассчитывает цену в центах для конкретной витрины с учётом курсов и надбавок.
        """
        sf = str(storefront).strip().lower()
        rates = config.EXCHANGE_RATES

        if sf == "cz":
            rate = rates.get("CZK", 25.4)
            return format_price_cents(rate * base_eur_price)
        elif sf == "pl":
            rate = rates.get("PLN", 4.7)
            return format_price_cents(rate * base_eur_price)
        elif sf in {"fr", "it"}:
            rate = rates.get("ITP", 1.05)
            return format_price_cents(base_eur_price * rate)
        else:
            return format_price_cents(base_eur_price)

    async def request(
        self,
        method: str,
        endpoint_or_url: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any = None,
        data: Any = None,
        **kwargs,
    ) -> Any:
        """
        Выполняет запрос к Kaufland API с HMAC-подписью, rate-limiting и автоматическими ретраями.
        """
        method = method.upper()
        if endpoint_or_url.startswith("http://") or endpoint_or_url.startswith("https://"):
            full_url = endpoint_or_url
        else:
            endpoint = endpoint_or_url if endpoint_or_url.startswith("/") else f"/{endpoint_or_url}"
            full_url = f"{self.base_url}{endpoint}"

        # Сериализация тела
        body_source = json_body if json_body is not None else data
        serialized_body = self.serialize_body(body_source)
        has_payload = bool(serialized_body) or method in {"POST", "PUT", "PATCH"}

        for attempt in range(1, self.retries + 1):
            headers = self.get_headers(
                method=method,
                url=full_url,
                serialized_body=serialized_body,
                has_payload=has_payload,
            )

            try:
                if self.rate_limiter:
                    async with self.rate_limiter:
                        resp_data = await self._execute_http(
                            method=method,
                            url=full_url,
                            headers=headers,
                            data=serialized_body if serialized_body else None,
                            params=params,
                            **kwargs,
                        )
                else:
                    resp_data = await self._execute_http(
                        method=method,
                        url=full_url,
                        headers=headers,
                        data=serialized_body if serialized_body else None,
                        params=params,
                        **kwargs,
                    )
                return resp_data

            except aiohttp.ClientResponseError as http_err:
                if http_err.status in RETRYABLE_STATUSES and attempt < self.retries:
                    backoff = min(10.0, (2 ** attempt) + 0.2 * attempt)
                    logger.warning(
                        f"[{self.account}] Kaufland API retryable error {http_err.status} on {method} {full_url}. "
                        f"Attempt {attempt}/{self.retries}. Retrying in {backoff:.1f}s."
                    )
                    await asyncio.sleep(backoff)
                    continue
                raise
            except Exception as exc:
                if attempt < self.retries:
                    backoff = min(10.0, (2 ** attempt) + 0.2 * attempt)
                    logger.warning(
                        f"[{self.account}] Network error '{exc}' on {method} {full_url}. "
                        f"Attempt {attempt}/{self.retries}. Retrying in {backoff:.1f}s."
                    )
                    await asyncio.sleep(backoff)
                    continue
                raise

    async def _execute_http(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        data: str | None,
        params: dict[str, Any] | None,
        **kwargs,
    ) -> Any:
        async with self.session.request(
            method=method,
            url=url,
            headers=headers,
            data=data,
            params=params,
            ssl=self._ssl_context,
            **kwargs,
        ) as response:
            text = await response.text()
            if response.status in RETRYABLE_STATUSES:
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message=text,
                    headers=response.headers,
                )

            if response.status >= 400:
                # 404 обрабатывается специальным образом
                if response.status == 404:
                    return {"message": "not found", "status": 404, "data": None}
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message=f"Kaufland API Error {response.status}: {text}",
                    headers=response.headers,
                )

            if response.status == 204 or not text.strip():
                return {"status": response.status, "data": None}

            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {"status": response.status, "data": text}
