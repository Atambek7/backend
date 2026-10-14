#!/usr/bin/env python3
"""
Скрипт применения МАКСИМАЛЬНЫХ цен для выявленных дублирующихся EAN на витринах Kaufland.

Читает:
- /app/duplicate_eans_found.csv (или .json)

Обновляет цены на витринах (de, cz, sk, pl, at, fr, it) через PATCH /units/{unit_id}.
Логирует все изменения в:
- /app/price_updates_log.txt
- /app/completed_price_fixes.txt
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

# Настройка Django окружения
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "kaufland_API.settings")
sys.path.append("/app")
import django
django.setup()

import aiohttp
from main_api.src.controller.rest_api_controller import RestApiController

BACKEND_DIR = Path("/app")
CSV_INPUT = BACKEND_DIR / "duplicate_eans_found.csv"
LOG_OUTPUT = BACKEND_DIR / "price_updates_log.txt"
COMPLETED_FIXES = BACKEND_DIR / "completed_price_fixes.txt"
MODIFIED_CSV = BACKEND_DIR / "modified_eans.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("price_updater")

ALL_STOREFRONTS = ["de", "cz", "sk", "pl", "at", "fr", "it"]


def append_to_update_log(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_OUTPUT, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


def record_modified(timestamp: str, ean: str, account: str, sf: str, unit_id: str, old_price: float, new_price: float, status: str):
    is_new = not MODIFIED_CSV.exists() or MODIFIED_CSV.stat().st_size == 0
    with open(MODIFIED_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow(["timestamp", "ean", "account", "storefront", "unit_id", "old_price_eur", "new_price_eur", "status"])
        writer.writerow([timestamp, ean, account.upper(), sf, unit_id, old_price, new_price, status])


def record_completed(ean: str):
    with open(COMPLETED_FIXES, "a", encoding="utf-8") as f:
        f.write(f"{ean}\n")


def load_completed() -> set:
    if COMPLETED_FIXES.exists():
        with open(COMPLETED_FIXES, "r", encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip()}
    return set()


async def update_ean_prices(
    http_client: aiohttp.ClientSession,
    account: str,
    ean: str,
    target_price: float,
    dry_run: bool = False
) -> bool:
    """Обновление цены оффера на всех витринах для указанного аккаунта."""
    rac = RestApiController(controller=account, session=http_client)
    success_sf = []

    for sf in ALL_STOREFRONTS:
        try:
            # 1. Поиск unit_id по id_offer
            u_res = await rac.send_request("GET", f"/units?storefront={sf}&id_offer={ean}")
            units = u_res.get("data", [])
            if not units or not isinstance(units, list):
                continue

            unit = units[0]
            unit_id = unit.get("id_unit")
            old_price = unit.get("price") or unit.get("listing_price")
            old_price_eur = round(float(old_price) / 100.0, 2) if old_price else 0.0

            if not unit_id:
                continue

            price_payload = rac.make_price_payload(storefront=sf, price=target_price)

            if dry_run:
                logger.info(f"[{account.upper()}][{ean}][{sf}] DRY-RUN: unit={unit_id} old={old_price_eur}€ -> new={target_price}€")
                success_sf.append(sf)
                continue

            # 2. PATCH цены
            patch_data = {
                "listing_price": price_payload["listing_price"],
                "minimum_price": price_payload["minimum_price"],
            }
            await rac.send_request("PATCH", f"/units/{unit_id}?storefront={sf}", json=patch_data)
            success_sf.append(sf)
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            record_modified(now_str, ean, account, sf, str(unit_id), old_price_eur, target_price, "UPDATED")
            logger.info(f"[{account.upper()}][{ean}][{sf}] UPDATED: unit={unit_id} old={old_price_eur}€ -> new={target_price}€")
            append_to_update_log(
                f"PRICE_UPDATE: account={account.upper()} ean={ean} sf={sf} "
                f"unit={unit_id} old_price={old_price_eur}EUR new_price={target_price}EUR"
            )
        except Exception as e:
            logger.error(f"[{account.upper()}][{ean}][{sf}] Error updating price: {e}")
            append_to_update_log(f"ERROR: account={account.upper()} ean={ean} sf={sf} err={e}")

    return len(success_sf) > 0


async def main():
    parser = argparse.ArgumentParser(description="Apply maximum prices to duplicated EANs in Kaufland")
    parser.add_argument("--dry-run", action="store_true", help="Симуляция без реальных изменений цен")
    parser.add_argument("--ean", type=str, default="", help="Обновить конкретный один EAN")
    parser.add_argument("--accounts", type=str, default="jv,xl", help="Список аккаунтов (jv, xl)")
    args = parser.parse_args()

    accounts = [a.strip().lower() for a in args.accounts.split(",") if a.strip()]

    if not CSV_INPUT.exists():
        logger.error(f"Файл {CSV_INPUT} не найден! Сначала запустите find_duplicate_eans_prices.py")
        return

    completed_eans = load_completed()
    items_to_update = []

    with open(CSV_INPUT, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ean = row.get("ean", "").strip()
            if not ean:
                continue
            if args.ean and ean != args.ean:
                continue
            if ean in completed_eans and not args.ean:
                continue
            
            # Обновляем если занижена цена или передан конкретный EAN
            if row.get("underpriced_on_kaufland") == "YES" or args.ean:
                try:
                    max_p = float(row.get("max_price", 0))
                    if max_p > 0:
                        items_to_update.append((ean, max_p, row))
                except ValueError:
                    pass

    logger.info(f"Найдено позиций для обновления цен: {len(items_to_update)} (dry_run={args.dry_run})")
    append_to_update_log(f"START_BATCH: Найдено {len(items_to_update)} EAN для обновления цен (dry_run={args.dry_run})")

    async with aiohttp.ClientSession() as http_client:
        for idx, (ean, max_p, row) in enumerate(items_to_update, 1):
            logger.info(f"[{idx}/{len(items_to_update)}] Обработка EAN {ean} -> целевая max_price: {max_p} €")
            
            any_updated = False
            for acc in accounts:
                updated = await update_ean_prices(http_client, acc, ean, max_p, dry_run=args.dry_run)
                if updated:
                    any_updated = True

            if any_updated and not args.dry_run:
                record_completed(ean)

            # Пауза для защиты от лимитов API Kaufland
            await asyncio.sleep(0.5)

    logger.info("=== Завершено обновление цен ===")
    append_to_update_log("COMPLETED_BATCH: Завершено обновление цен")


if __name__ == "__main__":
    asyncio.run(main())
