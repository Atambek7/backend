#!/usr/bin/env python3
"""
Скрипт запроса, скачивания и консолидации официальных отчетов invalid-items
с Kaufland API для всех активных витрин аккаунта JV (de, at, cz, sk, pl, fr, it, nl).
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp

BACKEND_DIR = Path(__file__).resolve().parent.parent
import sys
sys.path.insert(0, str(BACKEND_DIR))

import config
from main_api.src.controller.rest_api_controller import RestApiController
from scripts.purge_trash_catalog_items import SafeRateLimiter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fetch_jv_invalid")

STOREFRONTS = config.get_account_storefronts("jv")  # ["de", "cz", "sk", "pl", "at", "fr", "it", "nl"]
REPORTS_DIR = BACKEND_DIR / "reports" / "invalid_items_jv"


async def request_report_for_storefront(
    rac: RestApiController, sf: str, rate_limiter: SafeRateLimiter
) -> Optional[int]:
    logger.info(f"[{sf.upper()}] Отправка запроса POST /reports/invalid-items?storefront={sf}...")
    try:
        resp = await rac.send_request("POST", f"/reports/invalid-items?storefront={sf}")
        data = resp.get("data") or {}
        report_id = data.get("id_report")
        if report_id:
            logger.info(f"[{sf.upper()}] Отчет успешно поставлен в очередь: ID {report_id}")
            return report_id
        else:
            logger.warning(f"[{sf.upper()}] Ответ без id_report: {resp}")
            return None
    except Exception as e:
        logger.error(f"[{sf.upper()}] Ошибка при запросе отчета: {e}")
        return None


async def wait_and_download_report(
    rac: RestApiController,
    sf: str,
    report_id: int,
    session: aiohttp.ClientSession,
    rate_limiter: SafeRateLimiter,
    max_wait_seconds: int = 1800,
) -> Optional[bytes]:
    logger.info(f"[{sf.upper()}] Ожидание генерации отчета ID {report_id} (таймаут {max_wait_seconds}с)...")
    start_time = asyncio.get_event_loop().time()
    last_logged_status = None
    last_log_time = 0.0
    
    while asyncio.get_event_loop().time() - start_time < max_wait_seconds:
        await asyncio.sleep(8.0)
        try:
            resp = await rac.send_request("GET", f"/reports/{report_id}")
            data = resp.get("data") or {}
            status = str(data.get("status") or "").lower()
            
            now = asyncio.get_event_loop().time()
            if status != last_logged_status or (now - last_log_time) > 40:
                elapsed = int(now - start_time)
                logger.info(f"[{sf.upper()}] Отчет ID {report_id}: статус '{status}' (прошло {elapsed}с)...")
                last_logged_status = status
                last_log_time = now
            
            if status == "done":
                url = data.get("url")
                if not url:
                    logger.error(f"[{sf.upper()}] Статус 'done', но URL отсутствует!")
                    return None
                logger.info(f"[{sf.upper()}] Отчет готов! Скачивание CSV...")
                async with session.get(url) as download_resp:
                    if download_resp.status == 200:
                        content = await download_resp.read()
                        logger.info(f"[{sf.upper()}] Успешно скачано {len(content)} байт.")
                        return content
                    else:
                        logger.error(f"[{sf.upper()}] Ошибка скачивания по URL: {download_resp.status}")
                        return None
            elif status == "failed":
                logger.error(f"[{sf.upper()}] Генерация отчета завершилась со статусом 'failed'!")
                return None
        except Exception as e:
            logger.warning(f"[{sf.upper()}] Ошибка проверки статуса: {e}")
            
    logger.error(f"[{sf.upper()}] Таймаут ожидания отчета ({max_wait_seconds} сек)!")
    return None


def parse_csv_content(content: bytes) -> List[Dict[str, str]]:
    text = content.decode("utf-8-sig", errors="replace")
    # Определяем разделитель (\t, ; или ,)
    first_line = text.splitlines()[0] if text.splitlines() else ""
    if "\t" in first_line:
        delimiter = "\t"
    elif ";" in first_line:
        delimiter = ";"
    else:
        delimiter = ","
    
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    rows = []
    for r in reader:
        # Нормализуем ключи (нижний регистр, пробелы)
        clean_row = {str(k).strip().lower(): str(v).strip() for k, v in r.items() if k}
        rows.append(clean_row)
    return rows


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Fetch and consolidate invalid items reports for JV")
    parser.add_argument("--reparse-only", action="store_true", help="Reparse existing latest CSVs without downloading")
    parser.add_argument("--storefronts", nargs="+", default=STOREFRONTS, help="Specific storefronts to query")
    parser.add_argument("--existing-ids", type=str, default="", help="JSON or comma-separated SF:ID pairs of already requested reports")
    args = parser.parse_args()

    target_storefronts = [sf.lower().strip() for sf in args.storefronts]
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    rate_limiter = SafeRateLimiter(20.0)
    
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info("=" * 65)
    logger.info(f" Запуск сбора отчетов Invalid Items по аккаунту JV")
    logger.info(f" Целевые витрины: {', '.join(target_storefronts)}")
    logger.info(f" Директория сохранения: {REPORTS_DIR}")
    logger.info("=" * 65)
    
    parsed_reports: Dict[str, List[Dict[str, str]]] = {}

    if args.reparse_only:
        logger.info("Режим --reparse-only: чтение существующих *_latest.csv...")
        for sf in target_storefronts:
            latest_path = REPORTS_DIR / f"invalid_items_{sf}_latest.csv"
            if latest_path.exists():
                content = latest_path.read_bytes()
                rows = parse_csv_content(content)
                parsed_reports[sf] = rows
                logger.info(f"[{sf.upper()}] Распарсено записей: {len(rows)}")
            else:
                logger.warning(f"[{sf.upper()}] Файл {latest_path} не найден!")
    else:
        timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=120)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            rac = RestApiController("jv", session)
            
            # 1. Постановка отчетов в очередь через POST /reports/invalid-items?storefront={sf} или использование существующих
            report_ids: Dict[str, int] = {}
            if args.existing_ids:
                try:
                    if args.existing_ids.startswith("{"):
                        report_ids = json.loads(args.existing_ids)
                    else:
                        for pair in args.existing_ids.split(","):
                            sf, rid = pair.split(":")
                            report_ids[sf.strip().lower()] = int(rid.strip())
                    logger.info(f"Используем переданные ID отчетов: {report_ids}")
                except Exception as e:
                    logger.error(f"Не удалось распарсить --existing-ids: {e}")
                    report_ids = {}

            if not report_ids:
                for sf in target_storefronts:
                    rep_id = await request_report_for_storefront(rac, sf, rate_limiter)
                    if rep_id:
                        report_ids[sf] = rep_id
                    await asyncio.sleep(0.5)
                
            logger.info("-" * 65)
            logger.info(f"Задач на отслеживание: {len(report_ids)}")
            for sf, rid in report_ids.items():
                logger.info(f"  - {sf.upper()}: Report ID {rid}")
            logger.info("-" * 65)
            
            # 2. Ожидание и скачивание всех отчетов параллельно
            download_tasks = [
                wait_and_download_report(rac, sf, rid, session, rate_limiter)
                for sf, rid in report_ids.items()
            ]
            results = await asyncio.gather(*download_tasks)
            
            for (sf, _), content in zip(report_ids.items(), results):
                if content:
                    csv_path = REPORTS_DIR / f"invalid_items_{sf}_{timestamp_str}.csv"
                    csv_path.write_bytes(content)
                    latest_path = REPORTS_DIR / f"invalid_items_{sf}_latest.csv"
                    latest_path.write_bytes(content)
                    
                    rows = parse_csv_content(content)
                    parsed_reports[sf] = rows
                    logger.info(f"[{sf.upper()}] Успешно сохранен и распарсен: {len(rows)} строк.")
                else:
                    logger.error(f"[{sf.upper()}] Не удалось получить отчет.")

    # 3. Консолидация данных по товарам
    all_items: Dict[str, Dict] = defaultdict(lambda: {
        "ean": "",
        "titles": set(),
        "categories": set(),
        "storefronts": [],
        "missing_attributes": defaultdict(list),
        "reasons": defaultdict(list),
    })
    
    sf_counts = {}
    for sf, rows in parsed_reports.items():
        sf_counts[sf] = len(rows)
        for r in rows:
            ean = r.get("ean", "").strip()
            if not ean:
                continue
            item = all_items[ean]
            item["ean"] = ean
            if r.get("title"):
                item["titles"].add(r.get("title"))
            if r.get("category"):
                item["categories"].add(r.get("category"))
            item["storefronts"].append(sf)
            if r.get("missing_attributes"):
                item["missing_attributes"][sf].append(r.get("missing_attributes"))
            if r.get("reason"):
                item["reasons"][sf].append(r.get("reason"))

    total_unique_invalid = len(all_items)
    
    # 4. Сохранение сводного CSV
    summary_csv_path = REPORTS_DIR / f"consolidated_invalid_items_jv_{timestamp_str}.csv"
    latest_summary_csv = REPORTS_DIR / "consolidated_invalid_items_jv_latest.csv"
    
    fieldnames = [
        "ean",
        "storefronts_count",
        "storefronts",
        "title_sample",
        "category_sample",
        "missing_attributes_summary",
        "reasons_summary",
    ]
    
    with open(summary_csv_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for ean, data in sorted(all_items.items(), key=lambda x: len(x[1]["storefronts"]), reverse=True):
            missing_summary = []
            for sf, attrs in data["missing_attributes"].items():
                missing_summary.append(f"[{sf}]: {', '.join(attrs)}")
            
            reasons_summary = []
            for sf, reas in data["reasons"].items():
                reasons_summary.append(f"[{sf}]: {', '.join(reas)}")
                
            writer.writerow({
                "ean": ean,
                "storefronts_count": len(data["storefronts"]),
                "storefronts": ", ".join(data["storefronts"]),
                "title_sample": next(iter(data["titles"]), ""),
                "category_sample": next(iter(data["categories"]), ""),
                "missing_attributes_summary": " | ".join(missing_summary),
                "reasons_summary": " | ".join(reasons_summary),
            })
            
    latest_summary_csv.write_bytes(summary_csv_path.read_bytes())
    
    # 5. Сохранение JSON для детального программного анализа
    summary_json_path = REPORTS_DIR / f"consolidated_invalid_items_jv_{timestamp_str}.json"
    latest_summary_json = REPORTS_DIR / "consolidated_invalid_items_jv_latest.json"
    
    json_data = {
        "generated_at": datetime.now().isoformat(),
        "account": "jv",
        "storefronts_requested": target_storefronts,
        "counts_per_storefront": sf_counts,
        "total_unique_invalid_eans": total_unique_invalid,
        "items": [
            {
                "ean": ean,
                "storefronts": d["storefronts"],
                "titles": list(d["titles"]),
                "categories": list(d["categories"]),
                "missing_attributes": dict(d["missing_attributes"]),
                "reasons": dict(d["reasons"]),
            }
            for ean, d in all_items.items()
        ],
    }
    with open(summary_json_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)
    latest_summary_json.write_bytes(summary_json_path.read_bytes())
    
    # 6. Сохранение TXT со списком уникальных EAN
    eans_txt_path = REPORTS_DIR / f"invalid_eans_jv_unique_{timestamp_str}.txt"
    latest_eans_txt = REPORTS_DIR / "invalid_eans_jv_unique_latest.txt"
    with open(eans_txt_path, "w", encoding="utf-8") as f:
        for ean in sorted(all_items.keys()):
            f.write(f"{ean}\n")
    latest_eans_txt.write_bytes(eans_txt_path.read_bytes())

    # Вывод статистики
    logger.info("=" * 65)
    logger.info(" ИТОГОВЫЙ ОТЧЕТ INVALID ITEMS (АККАУНТ JV):")
    logger.info(f" Всего уникальных невалидных EAN: {total_unique_invalid}")
    for sf in target_storefronts:
        logger.info(f"  - {sf.upper()}: {sf_counts.get(sf, 0)} невалидных товаров")
    logger.info("-" * 65)
    logger.info(f" Сводный CSV:  {summary_csv_path}")
    logger.info(f" Сводный JSON: {summary_json_path}")
    logger.info(f" Список EAN:   {eans_txt_path}")
    logger.info("=" * 65)


if __name__ == "__main__":
    asyncio.run(main())
