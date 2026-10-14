import httpx
from typing import Any
from main_api.src.logger import log


class AftercoolProductRetrieveService:
    def __init__(self, aftercool_base_url: str, session_cookie: str):
        self.aftercool_base_url = aftercool_base_url
        self.session_cookie = session_cookie
    
    async def get_product(self, product_id: str) -> dict[str, Any]:
        url = f"{self.aftercool_base_url}/api/products"
        headers = {"Cookie": f"session={self.session_cookie}"}
        params = {"q": product_id, "include_row": 1}
        timeout = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)
        retries = 3

        async with httpx.AsyncClient(timeout=timeout) as client:
            for attempt in range(1, retries + 1):
                try:
                    log(
                        f"aftercool get_product attempt={attempt}/{retries} product_id={product_id}",
                        save=True,
                        level="info",
                    )
                    response = await client.get(url, headers=headers, params=params)
                    if response.status_code in (401, 403):
                        log(
                            f"aftercool session expired status={response.status_code} product_id={product_id}",
                            save=True,
                            level="warning",
                        )
                        return {
                            "success": False,
                            "expired": True,
                            "status_code": response.status_code,
                            "items": [],
                            "payload": {},
                        }
                    payload = response.json() if response.status_code == 200 else {}
                    items = payload.get("items") if isinstance(payload, dict) else []

                    if isinstance(items, list) and items:
                        first_name = items[0].get("name") if isinstance(items[0], dict) else ""
                        if first_name:
                            log(str(first_name))
                        return {
                            "success": True,
                            "items": items,
                            "payload": payload,
                        }

                    return {
                        "success": False,
                        "items": [],
                        "payload": payload if isinstance(payload, dict) else {},
                        "status_code": response.status_code,
                    }
                except Exception as exc:
                    log(
                        f"aftercool get_product error attempt={attempt}/{retries} product_id={product_id} error_type={type(exc).__name__} error={exc!r}",
                        save=True,
                        level="error",
                    )
                    if attempt == retries:
                        return {
                            "success": False,
                            "items": [],
                            "payload": {},
                            "error": f"{type(exc).__name__}: {exc!r}",
                        }


async def main():
    session = "eyJ1c2VyIjogeyJ1c2VybmFtZSI6ICJNaXJhcyIsICJyb2xlIjogInVzZXIifSwgImlhdCI6IDE3Nzc0NDg4NzZ9.afG3rA.DOkHTA7UsHhORwa86sATpawB7Zk"
    aftercool_base_url = "https://aftercool.de"
    product_id = "175494961"
    
    product_service = AftercoolProductRetrieveService(aftercool_base_url, session)
    
    product_result = await product_service.get_product(product_id)
    print(product_result)
    
if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
    
