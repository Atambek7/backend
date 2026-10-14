import aiohttp
import json
import logging
import config
from main_api.src.clients.kaufland_api_client import KauflandApiClient
from main_api.src.utils.price_utils import format_price_cents

logger = logging.getLogger(__name__)


class RestApiController:
    """
    Контроллер для отправки запросов к Kaufland Seller API.
    Использует единый KauflandApiClient и централизованные курсы валют.
    """

    def __init__(self, controller: str, session: aiohttp.ClientSession):
        self.controller = controller.lower()
        self.session = session
        self.base_api_url = config.KAUFLAND_API_BASE_URL
        self.client_key = config.controllers_base[self.controller]["client_key"]
        self.secret_key = config.controllers_base[self.controller]["secret_key"]
        self.api_client = KauflandApiClient(
            account=self.controller,
            session=self.session,
            base_url=self.base_api_url,
        )

    @staticmethod
    def _serialize_body(body) -> str:
        return KauflandApiClient.serialize_body(body)

    def _sign_request(self, method, uri, body, timestamp):
        return KauflandApiClient.sign_request(method, uri, body, timestamp, self.secret_key)

    def _get_header(self, method, uri, body, has_payload: bool = True):
        return self.api_client.get_headers(
            method,
            uri,
            serialized_body=str(body or ""),
            has_payload=has_payload,
        )

    @staticmethod
    def get_exchange_rate(currency: str = "PLN") -> float:
        return config.EXCHANGE_RATES.get(currency.upper(), 1.0)

    @staticmethod
    def format_price(price: float) -> int:
        """
        Конвертирует цену в центы (например, 1000.00 EUR -> 100000 центов).
        """
        return format_price_cents(price)

    def make_price(self, storefront: str, price: float) -> int:
        return self.api_client.calculate_storefront_price(storefront, price)

    def make_price_payload(self, storefront: str, price: float) -> dict:
        formatted = self.make_price(storefront=storefront, price=price)
        return {"listing_price": formatted, "minimum_price": formatted}

    async def send_request(self, method: str, endpoint: str, **kwargs) -> dict:
        endpoint_clean = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        full_url = self.base_api_url + endpoint_clean

        request_kwargs = dict(kwargs)
        body_for_signature = None
        if "json" in request_kwargs:
            body_for_signature = request_kwargs.pop("json")
        elif "data" in request_kwargs:
            body_for_signature = request_kwargs.get("data")
        elif "body" in request_kwargs:
            body_for_signature = request_kwargs.pop("body")

        serialized_body = self._serialize_body(body_for_signature)
        has_payload = bool(serialized_body) or method.upper() in {"POST", "PUT", "PATCH"}
        if serialized_body:
            request_kwargs["data"] = serialized_body

        headers = self._get_header(
            method,
            full_url,
            serialized_body,
            has_payload=has_payload,
        )

        async with self.session.request(
            method, full_url, headers=headers, **request_kwargs
        ) as response:
            response_text = await response.text()
            if response.status >= 400:
                raise Exception(f"API request failed: {response.status} - {response_text}")

            if response.status == 204 or not response_text.strip():
                return {"status": response.status, "data": None}

            try:
                return json.loads(response_text)
            except json.JSONDecodeError:
                return {"status": response.status, "data": response_text}