#!/usr/bin/env python3
"""
Скрипт безопасной деактивации (без удаления) проблемных товаров на Kaufland.

Принцип:
- Обнуляет складской остаток (amount = 0) через PATCH /units/{unit_id}?storefront={sf}.
- Товар мгновенно снимается с продажи на витрине («Out of stock / Недоступен»).
- Все карточки, описания, картинки и ID офферов сохраняются на 100%.
- Сохраняет полный снимок (backup) исходных параметров для последующей реактивации.

Файлы:
- Читает: /app/problematic_eans_for_sheets.csv
- Бэкап: /app/deactivated_units_backup.json
- Реестр: /app/deactivated_units.csv
- Чекпоинт: /app/deactivated_completed_eans.txt
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
from typing import Dict, List, Set

# Настройка Django окружения
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "kaufland_API.settings")
sys.path.append("/app")
import django
django.setup()

import aiohttp
from main_api.src.controller.rest_api_controller import RestApiController

BACKEND_DIR = Path("/app")
INPUT_CSV = BACKEND_DIR / "problematic_eans_for_sheets.csv"
BACKUP_JSON = BACKEND_DIR / "deactivated_units_backup.json"
OUTPUT_CSV = BACKEND_DIR / "deactivated_units.csv"
COMPLETED_TXT = BACKEND_DIR / "deactivated_completed_eans.txt"
LOG_FILE = BACKEND_DIR / "price_updates_log.txt"

ALL_STOREFRONTS = ["de", "cz", "sk", "pl", "at", "fr", "it"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("deactivator")


def append_to_log(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


def load_completed_eans() -> Set[str]:
    if COMPLETED_TXT.exists():
        with open(COMPLETED_TXT, "r", encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip()}
    return set()


def record_completed_ean(ean: str):
    with open(COMPLETED_TXT, "a", encoding="utf-8") as f:
        f.write(f"{ean}\n")


def load_backup_data() -> Dict[str, dict]:
    if BACKUP_JSON.exists():
        try:
            with open(BACKUP_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_backup_data(data: Dict[str, dict]):
    with open(BACKUP_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def append_to_csv(timestamp: str, ean: str, account: str, sf: str, unit_id: str, prev_amount: int, prev_price: float, status: str):
    is_new = not OUTPUT_CSV.exists() or OUTPUT_CSV.stat().st_size == 0
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow(["timestamp", "ean", "account", "storefront", "unit_id", "previous_amount", "previous_price_eur", "status"])
        writer.writerow([timestamp, ean, account.upper(), sf, unit_id, prev_amount, prev_price, status])


async def process_ean_units(
    ean: str,
    accounts: List[str],
    rac_map: Dict[str, RestApiController],
    backup_data: Dict[str, dict],
    dry_run: bool = False,
    lock: asyncio.Lock = None,
) -> int:
    """Поиск и деактивация всех юнитов для указанного EAN во всех аккаунтах и витринах."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    deactivated_units_count = 0

    for acc in accounts:
        rac = rac_map[acc]
        for sf in ALL_STOREFRONTS:
            try:
                res = await rac.send_request("GET", f"/units?storefront={sf}&id_offer={ean}")
                units = res.get("data", [])
                if not units or not isinstance(units, list):
                    continue

                for u in units:
                    unit_id = str(u.get("id_unit", "")).strip()
                    if not unit_id:
                        continue

                    current_amount = int(u.get("amount", 0) or 0)
                    raw_price = u.get("price") or u.get("listing_price")
                    price_eur = round(float(raw_price) / 100.0, 2) if raw_price else 0.0

                    # Ключ для бэкапа
                    backup_key = f"{acc}_{sf}_{unit_id}"

                    if current_amount == 0:
                        logger.info(f"[{acc.upper()}][{ean}][{sf}] unit={unit_id} уже деактивирован (amount=0)")
                        if lock:
                            async with lock:
                                append_to_csv(now_str, ean, acc, sf, unit_id, current_amount, price_eur, "ALREADY_INACTIVE")
                        continue

                    if dry_run:
                        logger.info(f"[{acc.upper()}][{ean}][{sf}] DRY-RUN: unit={unit_id} (amount={current_amount} -> 0, price={price_eur}€)")
                        deactivated_units_count += 1
                        continue

                    # Выполняем реальную деактивацию (amount = 0)
                    patch_payload = {"amount": 0}
                    await rac.send_request("PATCH", f"/units/{unit_id}?storefront={sf}", json=patch_payload)

                    logger.info(f"[{acc.upper()}][{ean}][{sf}] DEACTIVATED: unit={unit_id} amount {current_amount} -> 0")
                    deactivated_units_count += 1

                    if lock:
                        async with lock:
                            backup_data[backup_key] = {
                                "deactivated_at": now_str,
                                "ean": ean,
                                "account": acc,
                                "storefront": sf,
                                "unit_id": unit_id,
                                "previous_amount": current_amount,
                                "previous_price_eur": price_eur,
                            }
                            append_to_csv(now_str, ean, acc, sf, unit_id, current_amount, price_eur, "DEACTIVATED")
                            append_to_log(
                                f"UNIT_DEACTIVATED: account={acc.upper()} ean={ean} sf={sf} "
                                f"unit={unit_id} prev_amount={current_amount} prev_price={price_eur}EUR"
                            )

            except Exception as e:
                logger.error(f"[{acc.upper()}][{ean}][{sf}] Ошибка проверки/деактивации: {e}")
                if lock:
                    async with lock:
                        append_to_log(f"ERROR_DEACTIVATE: account={acc.upper()} ean={ean} sf={sf} err={e}")

    return deactivated_units_count


async def main():
    parser = argparse.ArgumentParser(description="Deactivate problematic units on Kaufland by setting amount=0")
    parser.add_argument("--dry-run", action="store_true", help="Симуляция без реального обнуления остатков")
    parser.add_argument("--ean", type=str, default="", help="Деактивировать один конкретный EAN")
    parser.add_argument("--accounts", type=str, default="jv,xl", help="Список аккаунтов (jv, xl)")
    parser.add_argument("--concurrency", type=int, default=8, help="Количество параллельных потоков")
    args = parser.parse_args()

    accounts = [a.strip().lower() for a in args.accounts.split(",") if a.strip()]

    if not INPUT_CSV.exists():
        logger.error(f"Файл {INPUT_CSV} не найден!")
        return

    # Загрузка списка EAN
    eans_to_process = []
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ean = row.get("EAN", "").strip()
            if not ean:
                continue
            if args.ean and ean != args.ean:
                continue
            eans_to_process.append(ean)

    completed_eans = load_completed_eans()
    if not args.ean:
        pending_eans = [e for e in eans_to_process if e not in completed_eans]
    else:
        pending_eans = eans_to_process

    logger.info(f"Всего проблемных EAN: {len(eans_to_process)}, ожидает обработки: {len(pending_eans)} (dry_run={args.dry_run})")
    append_to_log(f"START_DEACTIVATION_RUN: EANs={len(pending_eans)}, dry_run={args.dry_run}")

    backup_data = load_backup_data()
    lock = asyncio.Lock()
    sem = asyncio.Semaphore(args.concurrency)
    total_deactivated_units = 0
    processed_eans_count = 0

    async with aiohttp.ClientSession() as http_client:
        rac_map = {acc: RestApiController(controller=acc, session=http_client) for acc in accounts}

        async def worker(ean: str):
            nonlocal total_deactivated_units, processed_eans_count
            async with sem:
                deact_count = await process_ean_units(
                    ean=ean,
                    accounts=accounts,
                    rac_map=rac_map,
                    backup_data=backup_data,
                    dry_run=args.dry_run,
                    lock=lock,
                )
                async with lock:
                    total_deactivated_units += deact_count
                    processed_eans_count += 1
                    if not args.dry_run:
                        record_completed_ean(ean)
                        if processed_eans_count % 50 == 0:
                            save_backup_data(backup_data)

                    if processed_eans_count % 50 == 0 or processed_eans_count == len(pending_eans):
                        logger.info(f"Прогресс: [{processed_eans_count}/{len(pending_eans)} EAN] деактивировано офферов: {total_deactivated_units}")

        tasks = [worker(ean) for ean in pending_eans]
        await asyncio.gather(*tasks)

    if not args.dry_run:
        save_backup_data(backup_data)

    logger.info(f"=== Деактивация завершена! Всего отключено офферов: {total_deactivated_units} ===")
    append_to_log(f"COMPLETED_DEACTIVATION_RUN: Обработано EAN={processed_eans_count}, отключено офферов={total_deactivated_units}")


if __name__ == "__main__":
    asyncio.run(main())
