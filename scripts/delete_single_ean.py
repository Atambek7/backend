#!/usr/bin/env python3
import asyncio
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BACKEND_DIR))

import aiohttp
from main_api.src.controller.rest_api_controller import RestApiController
import config

EAN = "4072102283298"
ACCOUNT = "jv"
STOREFRONTS = config.get_account_storefronts(ACCOUNT)
LOCALES = ["de-DE", "fr-FR", "nl-NL", "pl-PL", "cs-CZ", "sk-SK", "de-AT", "it-IT"]

async def main():
    ean = sys.argv[1] if len(sys.argv) > 1 else EAN
    account = sys.argv[2] if len(sys.argv) > 2 else ACCOUNT
    storefronts = config.get_account_storefronts(account)

    print(f"=== Deleting EAN {ean} on account {account.upper()} ===", flush=True)
    async with aiohttp.ClientSession() as session:
        rac = RestApiController(account, session)

        print("\n--- 1. Checking and deleting Units ---", flush=True)
        deleted_units = 0
        for sf in storefronts:
            try:
                res = await rac.send_request("GET", f"/units?storefront={sf}&id_offer={ean}")
                units = res.get("data", []) if isinstance(res, dict) else []
                if not units:
                    res2 = await rac.send_request("GET", f"/units?storefront={sf}&ean={ean}")
                    units = res2.get("data", []) if isinstance(res2, dict) else []

                if not units:
                    print(f"[{sf}] No unit found", flush=True)
                    continue

                for u in units:
                    unit_id = u.get("id_unit")
                    try:
                        await rac.send_request("DELETE", f"/units/{unit_id}?storefront={sf}")
                        print(f"[{sf}] DELETED unit {unit_id}", flush=True)
                        deleted_units += 1
                    except Exception as e:
                        print(f"[{sf}] Error deleting unit {unit_id}: {e}", flush=True)
            except Exception as e:
                print(f"[{sf}] Lookup error: {e}", flush=True)

        print("\n--- 2. Deleting Product Data across locales ---", flush=True)
        deleted_pdata = 0
        for loc in LOCALES:
            try:
                await rac.send_request("DELETE", f"/product-data/{ean}?locale={loc}")
                print(f"[{loc}] DELETED product-data", flush=True)
                deleted_pdata += 1
            except Exception as e:
                err_str = str(e)
                if "404" in err_str:
                    print(f"[{loc}] Not present (404)", flush=True)
                else:
                    print(f"[{loc}] Status: {e}", flush=True)

        print(f"\nRESULT: Deleted {deleted_units} unit(s), deleted {deleted_pdata} product-data locale(s).", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
