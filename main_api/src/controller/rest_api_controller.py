import aiohttp
import config
import hmac
import json
import hashlib
import time
import logging
import math
import inspect

logger = logging.getLogger(__name__)


class RestApiController:

    def __init__(self, controller: str, session: aiohttp.ClientSession):
        self.controller = controller
        self.session = session

        self.base_api_url = "https://sellerapi.kaufland.com/v2"

        self.client_key = config.controllers_base[self.controller]["client_key"]
        self.secret_key = config.controllers_base[self.controller]["secret_key"]

    @staticmethod
    def _serialize_body(body) -> str:
        if body is None or body == "":
            return ""
        if isinstance(body, dict):
            # Keep JSON compact to match Kaufland signature requirements.
            return json.dumps(body, separators=(",", ":"))
        return body

    def _sign_request(self, method, uri, body, timestamp):
        if isinstance(body, dict):
            body = json.dumps(body, separators=(",", ":"))  # важно без пробелов
        elif body is None:
            body = ""
        plain_text = "\n".join([method, uri, body, str(timestamp)])
        digest_maker = hmac.new(self.secret_key.encode(), None, hashlib.sha256)
        digest_maker.update(plain_text.encode())

        return digest_maker.hexdigest()

    def _get_header(self, method, uri, body, has_payload: bool = True):
        timestamp = str(int(time.time()))
        signature = self._sign_request(method, uri, body, timestamp)

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

    @staticmethod
    def get_exchange_rate(currency: str = "PLN") -> float:
        currency_dict = {"PLN": 4.7, "CZK": 25.4, "ITP": 1.05}
        return currency_dict[currency]

    @staticmethod
    def format_price(price: float) -> int:
        price_up = math.ceil(price)
        return ((price_up // 10) * 10 + 9) * 100

    def make_price(self, storefront: str, price: float) -> int:
        if storefront == "cz":
            rate = self.get_exchange_rate("CZK")
            total_price = int(self.format_price(rate * price))
            
        elif storefront == "pl":
            rate = self.get_exchange_rate("PLN")
            total_price = int(self.format_price(rate * price))

        elif storefront in {"fr", "it"}:
            rate = self.get_exchange_rate("ITP")
            total_price = int(self.format_price(price * rate))

        else:
            total_price = int(self.format_price(price))

        return total_price

    def make_price_payload(self, storefront: str, price: float) -> dict:
        formatted = self.make_price(storefront=storefront, price=price)
        return {"listing_price": formatted, "minimum_price": formatted}

    async def send_request(self, method: str, endpoint: str, **kwargs) -> dict:
        url = self.base_api_url + endpoint

        request_kwargs = dict(kwargs)
        has_payload = any(key in request_kwargs for key in ("json", "data", "body"))
        body_for_signature = None
        if "json" in request_kwargs:
            body_for_signature = request_kwargs.get("json")
            
        elif "data" in request_kwargs:
            body_for_signature = request_kwargs.get("data")

        elif "body" in request_kwargs:
            body_for_signature = request_kwargs.get("body")

        serialized_body = self._serialize_body(body_for_signature)

        if "json" in request_kwargs:
            request_kwargs.pop("json", None)
            request_kwargs["data"] = serialized_body
        elif "body" in request_kwargs:
            request_kwargs.pop("body", None)
            request_kwargs["data"] = serialized_body

        headers = self._get_header(
            method,
            url,
            serialized_body,
            has_payload=has_payload,
        )

        async with self.session.request(
            method, url, headers=headers, **request_kwargs
        ) as response:
            response_text = await response.text()
            if response.status >= 400:
                raise Exception(f"API request failed: {response.status} - {response_text}")

            if response.status == 204 or not response_text.strip():
                return {"status": response.status, "data": None}

            try:
                caller = inspect.stack()[1].function
                print(caller)
                return json.loads(response_text)
            except json.JSONDecodeError:
                print("ERROR HERE")
                return {"status": response.status, "data": response_text}
            except Exception as e:
                print(f"Exception {str(e)} happend: {type(e).__name__}, args: {e.args}")
                raise TypeError("KOTAK")