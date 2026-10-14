#!/usr/bin/env python3
"""
Скрипт реактивации (возврата в продажу) ранее деактивированных товаров на Kaufland.

Принцип:
- Читает снимок бэкапа: /app/deactivated_units_backup.json
- Восстанавливает складской остаток (amount = previous_amount или 20) через PATCH /units/{unit_id}?storefront={sf}.
- При указании флага --update-prices одновременно выставляет обновленные правильные максимальные цены!
- Товар мгновенно возвращается на витрину активным с актуальной ценой.

Файлы:
- Бэкап: /app/deactivated_units_backup.json
- Цены: /app/problematic_eans_for_sheets.csv
- Лог: /app/price_updates_log.txt
"""

import argparse
import asyncio
import csv
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import sys
from typing import Dict

# Настройка Django окружения
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "kaufland_API.settings")
sys.path.append("/app")
import django
django.setup()

import aiohttp
from main_api.src.controller.rest_api_controller import RestApiController

BACKEND_DIR = Path("/app")
BACKUP_JSON = BACKEND_DIR / "deactivated_units_backup.json"
PRICES_CSV = BACKEND_DIR / "problematic_eans_for_sheets.csv"
LOG_FILE = BACKEND_DIR / "price_updates_log.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("reactivator")


def append_to_log(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


def load_backup_data() -> Dict[str, dict]:
    if BACKUP_JSON.exists():
        try:
            with open(BACKUP_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def load_target_prices() -> Dict[str, float]:
    prices = {}
    if PRICES_CSV.exists():
        with open(PRICES_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ean = row.get("EAN", "").strip()
                p = row.get("TARGET_MAX_PRICE", "").strip()
                if ean and p:
                    try:
                        prices[ean] = float(p)
                    except ValueError:
                        pass
    return prices


async def main():
    parser = argparse.ArgumentParser(description="Reactivate previously deactivated units on Kaufland")
    parser.add_argument("--dry-run", action="store_true", help="Симуляция без реальных изменений")
    parser.add_argument("--ean", type=str, default="", help="Реактивировать один конкретный EAN")
    parser.add_argument("--default-amount", type=int, default=20, help="Остаток по умолчанию (если previous_amount был 0)")
    parser.add_argument("--update-prices", action="store_true", help="Применить целевую максимальную цену при включении")
    parser.add_argument("--concurrency", type=int, default=8, help="Количество параллельных потоков")
    args = parser.parse_args()

    backup_data = load_backup_data()
    if not backup_data:
        logger.error("Файл бэкапа deactivated_units_backup.json пуст или не найден!")
        return

    target_prices = load_target_prices() if args.update_prices else {}

    units_to_reactivate = []
    for key, item in backup_data.items():
        ean = item.get("ean", "")
        if args.ean and ean != args.ean:
            continue
        units_to_reactivate.append(item)

    logger.info(f"Найдено юнитов для реактивации: {len(units_to_reactivate)} (dry_run={args.dry_run}, update_prices={args.update_prices})")
    append_to_log(f"START_REACTIVATION_RUN: Units={len(units_to_reactivate)}, dry_run={args.dry_run}, update_prices={args.update_prices}")

    sem = asyncio.Semaphore(args.concurrency)
    lock = asyncio.Lock()
    reactivated_count = 0

    async with aiohttp.ClientSession() as http_client:
        rac_map = {
            "jv": RestApiController(controller="jv", session=http_client),
            "xl": RestApiController(controller="xl", session=http_client),
        }

        async def reactivate_one(item: dict):
            nonlocal reactivated_count
            acc = item["account"]
            sf = item["storefront"]
            unit_id = item["unit_id"]
            ean = item["ean"]
            rac = rac_map.get(acc)
            if not rac:
                return

            restore_amount = item.get("previous_amount") or args.default_amount
            if restore_amount <= 0:
                restore_amount = args.default_amount

            patch_payload = {"amount": restore_amount}

            # Если включен режим обновления цен
            new_price = None
            if args.update_prices and ean in target_prices:
                new_price = target_prices[ean]
                price_payload = rac.make_price_payload(storefront=sf, price=new_price)
                patch_payload["listing_price"] = price_payload["listing_price"]
                patch_payload["minimum_price"] = price_payload["minimum_price"]

            async with sem:
                try:
                    if args.dry_run:
                        logger.info(f"[{acc.upper()}][{ean}][{sf}] DRY-RUN REACTIVATE: unit={unit_id} amount={restore_amount} price={new_price or item.get('previous_price_eur')}€")
                    else:
                        await rac.send_request("PATCH", f"/units/{unit_id}?storefront={sf}", json=patch_payload)
                        logger.info(f"[{acc.upper()}][{ean}][{sf}] REACTIVATED: unit={unit_id} amount={restore_amount}")
                        append_to_log(f"UNIT_REACTIVATED: account={acc.upper()} ean={ean} sf={sf} unit={unit_id} amount={restore_amount} price={new_price}")

                    async with lock:
                        reactivated_count += 1
                        if reactivated_count % 50 == 0 or reactivated_count == len(units_to_reactivate):
                            logger.info(f"Прогресс реактивации: [{reactivated_count}/{len(units_to_reactivate)}]")
                except Exception as e:
                    logger.error(f"[{acc.upper()}][{ean}][{sf}] Ошибка реактивации: {e}")
                    append_to_log(f"ERROR_REACTIVATING: account={acc.upper()} ean={ean} sf={sf} err={e}")

        tasks = [reactivate_one(u) for u in units_to_reactivate]
        await asyncio.gather(*tasks)

    logger.info(f"=== Реактивация завершена! Восстановлено офферов: {reactivated_count} ===")
    append_to_log(f"COMPLETED_REACTIVATION_RUN: Восстановлено офферов: {reactivated_count}")


if __name__ == "__main__":
    asyncio.run(main())
