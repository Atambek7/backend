#!/usr/bin/env python3
"""
Пайплайн автономной синхронизации каталогов JV и XL на Kaufland.

Правила:
1. Строгие белые списки:
   - JV: backend/reports/JV_VALID.txt (80 969 EAN)
   - XL: backend/reports/XL_VALID.txt (86 217 EAN)
2. Свежие данные из Aftercool (актуальные цены, атрибуты, мастер-описания с кэшированием).
3. Сроки доставки (Handling Time):
   - SOFORT (фабрики 504251, 504255): handling_time=7, amount=1 (строгая защита!).
   - Китай (CN_PATTERNS): handling_time=60 дней (12 недель).
   - Европа / Турция (не-Китай): handling_time=30 дней, amount=20.
4. Категории (СТРОГО ЛИСТОВЫЕ МЕБЕЛЬНЫЕ КАТЕГОРИИ):
   - Ecksofas / Wohnlandschaft / L-Form / U-Form -> 5911 (ecksofas).
   - Klassische Sofas -> 45081 (klassische-sofas).
   - Sessel -> 45071 (klassische-sessel) [листовая категория вместо родительской 3791!].
   - Couchtische / Kaffeetische -> 3531 (couchtische).
   - Esszimmerstühle -> 4211 (esszimmerstuehle).
   - Polsterbetten -> 2751 (polsterbetten).
   - Черный список: Dekostoffe (4921), Brennpasten (61021), Schminkstifte (21001), Sonstige Geschirre (66041) и т.д. -> перенаправление в валидную категорию.
   - Уже стоящие валидные мебельные листовые категории СОХРАНЯЮТСЯ (правило "Не сломай").
5. MPN для XL:
   - attributes["mpn"] = Herstellernummer или fallback XLM{ean}.
6. Структурированное описание (Maße, Farbe, Material, Produktmerkmale, Produktbeschreibung):
   - На основе CustomItemSpecifics и мастер-статьи I_stammartikel.
7. Офферы (units):
   - Если оффер уже есть: PATCH /units/{unit_id} (amount=20, handling_time=30/60).
   - Если оффера нет: POST /units (расчетная цена из Aftercool, amount=20, handling_time=30/60).
8. Безопасность, скорость и автономность:
   - SafeRateLimiter на 60.0 RPS.
   - Worker pool (по умолчанию 15 воркеров) для высокой скорости (~20-25 EAN/сек).
   - Чекпоинты (sync_checkpoint_jv.txt, sync_checkpoint_xl.txt) с fsync.
   - Изоляция ошибок (per-item try/except).
   - Раздельные PID и логи для независимого запуска JV и XL (--account jv / --account xl).
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import logging
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import aiohttp

BACKEND_DIR = Path("/home/Altera/workdir/kaufland/backend")
sys.path.insert(0, str(BACKEND_DIR))

import config
from main_api.src.controller.rest_api_controller import RestApiController
from main_api.services.aftercool_service import AftercoolService, AftercoolProductsQuery
from main_api.src.extracter import _normalize_aftercool_html
from main_api.src.utils.text_utils import sanitize_description, is_blacklisted_category
from main_api.src.utils.category_matcher import match_leaf_category
from scripts.purge_trash_catalog_items import SafeRateLimiter, request_with_retry
from scripts.reupload_aftercool_catalog import CN_PATTERNS

# Файлы белых списков
JV_VALID_FILE = BACKEND_DIR / "reports" / "JV_VALID.txt"
XL_VALID_FILE = BACKEND_DIR / "reports" / "XL_VALID.txt"

# Чекпоинты
CP_JV_FILE = BACKEND_DIR / "reports" / "sync_checkpoint_jv.txt"
CP_XL_FILE = BACKEND_DIR / "reports" / "sync_checkpoint_xl.txt"

# Фабрики SOFORT (строго 1 шт, 7 дней доставки)
SOFORT_FACTORIES = {"504251", "504255"}

# Черный список категорий (немебельные курьезы)
BLACKLIST_CATEGORIES = {
    4921,   # Dekostoffe
    61021,  # Brennpasten
    21001,  # Schminkstifte
    66041,  # Sonstige Geschirre
    59301,  # Schrankgriffe
    1,      # Möbel (слишком общая категория)
    3791,   # Sessel (родительская категория, не листовая!)
}

# Валидные мебельные листовые категории для классификатора
LEAF_FURNITURE_MAPPING = [
    (["ecksofa", "wohnlandschaft", "l-form", "u-form"], 5911, "ecksofas", "Ecksofas"),
    (["chaiselounge", "recamiere", "ottomane"], 5831, "cocktailsessel", "Cocktailsessel"),
    (["sessel", "einsitzer"], 45071, "klassische-sessel", "Klassische Sessel"),
    (["couchtisch", "kaffeetisch", "beistelltisch"], 3531, "couchtische", "Couchtische"),
    (["esstisch", "esstische"], 3541, "esstische", "Esstische"),
    (["stuhl", "stühle", "stuehle", "esszimmerstuhl"], 4211, "esszimmerstuehle", "Esszimmerstühle"),
    (["sofa", "couch", "garnitur", "dreisitzer", "zweisitzer"], 45081, "klassische-sofas", "Klassische Sofas"),
    (["bett", "polsterbett", "doppelbett", "boxspringbett"], 2751, "polsterbetten", "Polsterbetten"),
    (["schrank", "kleiderschrank"], 3271, "kleiderschraenke", "Kleiderschränke"),
    (["kommode", "sideboard", "highboard"], 3261, "kommoden", "Kommoden"),
]


def setup_logger(account: str) -> Tuple[logging.Logger, logging.Logger]:
    BACKEND_DIR.joinpath("logs").mkdir(parents=True, exist_ok=True)
    BACKEND_DIR.joinpath("reports").mkdir(parents=True, exist_ok=True)

    log_file = BACKEND_DIR / "logs" / f"sync_catalog_live_{account}.log"
    err_log_file = BACKEND_DIR / "logs" / f"sync_errors_{account}.log"

    main_logger = logging.getLogger(f"sync_{account}")
    main_logger.setLevel(logging.INFO)
    main_logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    main_logger.addHandler(fh)
    main_logger.addHandler(sh)

    err_logger = logging.getLogger(f"sync_errors_{account}")
    err_logger.setLevel(logging.ERROR)
    err_logger.handlers.clear()
    efh = logging.FileHandler(err_log_file, encoding="utf-8")
    efh.setFormatter(formatter)
    err_logger.addHandler(efh)

    return main_logger, err_logger


def parse_item_specifics(xml_str: str) -> dict:
    if not xml_str or not isinstance(xml_str, str):
        return {}
    res = {}
    try:
        root = ET.fromstring(xml_str)
        for nv in root.findall("NameValueList"):
            name = nv.findtext("Name")
            val = nv.findtext("Value")
            if name and val:
                res[name.strip()] = val.strip()
    except Exception:
        for m in re.finditer(r"<Name><!\[CDATA\[(.*?)\]\]></Name><Value><!\[CDATA\[(.*?)\]\]></Value>", xml_str):
            res[m.group(1).strip()] = m.group(2).strip()
    return res


def format_rich_description(title: str, specifics: dict, stamm_html: str = "") -> str:
    if stamm_html:
        norm = _normalize_aftercool_html(stamm_html)
        clean = sanitize_description(norm)
        if len(clean) > 250 and ("Maße:" in clean or "Maße" in clean) and "Produktbeschreibung:" in clean:
            return clean

    lines = []
    
    # Секция Maße
    m_dimensions = specifics.get("Maße") or specifics.get("Masse") or ""
    height = specifics.get("Höhe") or specifics.get("Hoehe") or ""
    width = specifics.get("Breite") or ""
    length = specifics.get("Länge") or specifics.get("Laenge") or ""
    depth = specifics.get("Tiefe") or specifics.get("Sitztiefe") or ""
    liege = specifics.get("Liegefläche") or specifics.get("Liegeflaeche") or ""
    prod_type = specifics.get("Produktart") or "Artikel"
    
    lines.append("<div><strong>Maße:</strong><br/><br/>")
    lines.append(f"<strong>{prod_type}:</strong><br/>")
    if m_dimensions:
        lines.append(f"ca: {m_dimensions}<br/>")
    if height:
        lines.append(f"Höhe: {height}<br/>")
    if width:
        lines.append(f"Breite: {width}<br/>")
    if length:
        lines.append(f"Länge: {length}<br/>")
    if depth:
        lines.append(f"Sitztiefe: {depth}<br/>")
    if liege:
        lines.append(f"Liegefläche: {liege}<br/>")
    lines.append("<br/>")

    # Секция Farbe
    color = specifics.get("Farbe") or ""
    if color:
        lines.append(f"<strong>Farbe:</strong> {color}<br/><br/>")

    # Секция Material
    material = specifics.get("Material") or specifics.get("Polsterstoff") or ""
    if material:
        lines.append(f"<strong>Material:</strong> {material}<br/><br/>")

    # Секция Produktmerkmale
    features = []
    ignore_keys = {
        "Maße", "Masse", "Höhe", "Hoehe", "Breite", "Länge", "Laenge", "Tiefe", "Sitztiefe",
        "Liegefläche", "Liegeflaeche", "Farbe", "EAN", "Herstellernummer", "Herstellungsjahr",
        "Angebotspaket", "Verpackung", "Material", "Polsterstoff"
    }
    for k, v in specifics.items():
        if k not in ignore_keys and v and str(v).lower() not in ("nein", "0", "false"):
            features.append(f"- {k}: {v}")
    
    if features:
        lines.append("<strong>Produktmerkmale:</strong><br/>")
        for f in features[:10]:
            lines.append(f"{f}<br/>")
        lines.append("<br/>")

    # Секция Produktbeschreibung
    clean_title = re.sub(r"\b(?:Neu|NEU)\b", "", title).strip()
    lines.append("<strong>Produktbeschreibung:</strong><br/>")
    lines.append(f"{clean_title}<br/>")
    
    custom_desc = None
    if stamm_html:
        norm = _normalize_aftercool_html(stamm_html)
        clean = sanitize_description(norm)
        plain = re.sub(r"<[^>]+>", " ", clean).strip()
        if len(plain) > 80:
            custom_desc = plain

    if custom_desc:
        lines.append(f"{custom_desc}<br/>")
    else:
        lines.append("Dieses hochwertige Möbelstück vereint erstklassige Materialqualität, elegantes Design und optimalen Wohnkomfort für Ihr Zuhause.<br/>")
    
    lines.append("</div>")
    return "".join(lines)


def resolve_category(title: str, curr_cat_id: Optional[int], curr_cat_name: Optional[str]) -> Tuple[int, str, str]:
    # 1. Приоритетное сопоставление по базе листовых категорий
    matched = match_leaf_category(title)
    if matched:
        return matched

    # 2. Если по правилам не определили, но текущая категория на Kaufland валидная -> сохраняем
    is_bad = (not curr_cat_id) or (curr_cat_id in BLACKLIST_CATEGORIES) or is_blacklisted_category(curr_cat_name or "", title=title)
    if not is_bad and curr_cat_id and curr_cat_name:
        return (curr_cat_id, curr_cat_name, curr_cat_name.capitalize())

    # 3. Резервный поиск по базовым ключевым словам
    t = (title or "").lower()
    for keywords, cat_id, cat_name, cat_title in LEAF_FURNITURE_MAPPING:
        if any(k in t for k in keywords):
            return (cat_id, cat_name, cat_title)

    # 4. Безопасный Fallback
    return (45081, "sofas", "Sofas")


def is_china(source_file: str, factory_id: str) -> bool:
    source_upper = f"{source_file or ''} {factory_id or ''}".upper()
    return any(p in source_upper for p in CN_PATTERNS)


def is_sofort(factory_id: str) -> bool:
    return str(factory_id).strip() in SOFORT_FACTORIES


def load_checkpoint(cp_file: Path) -> Set[str]:
    if not cp_file.exists():
        return set()
    with open(cp_file, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def append_checkpoint(cp_file: Path, ean: str, dry_run: bool = False):
    if dry_run:
        return
    with open(cp_file, "a", encoding="utf-8") as f:
        f.write(f"{ean}\n")
        f.flush()
        os.fsync(f.fileno())


async def process_single_item(
    account: str,
    ean: str,
    af_service: AftercoolService,
    af_cookie: str,
    rac: RestApiController,
    rate_limiter: SafeRateLimiter,
    cp_file: Path,
    stamm_cache: Dict[str, str],
    main_logger: logging.Logger,
    err_logger: logging.Logger,
    dry_run: bool = False,
) -> str:
    # 1. Извлечение данных из Aftercool
    af_res = await af_service.get_products(af_cookie, query=AftercoolProductsQuery(account=account, q=ean, limit=1, include_row=1))
    if not af_res:
        af_res = await af_service.get_products(af_cookie, query=AftercoolProductsQuery(account="", q=ean, limit=1, include_row=1))

    if not af_res:
        err_logger.error(f"[{account.upper()}][{ean}] Не найден в Aftercool! Пропуск.")
        append_checkpoint(cp_file, ean, dry_run=dry_run)
        return "NOT_IN_AFTERCOOL"

    it = af_res[0]
    row = it.get("row") or {}
    title = it.get("name") or row.get("Artikelbeschreibung") or ""
    source_file = it.get("source_file") or row.get("source_file") or ""
    factory_id = str(it.get("factory_id") or row.get("factory_id") or "")
    price = float(it.get("price") or row.get("Preis") or 0.0)
    spec_xml = row.get("CustomItemSpecifics") or ""
    specs = parse_item_specifics(spec_xml)
    stamm_id = str(row.get("I_stammartikel") or "").strip()

    # Сроки и остатки
    is_sof = is_sofort(factory_id)
    is_cn = is_china(source_file, factory_id)

    if is_sof:
        target_amount = 1
        target_ht = 7
    elif is_cn:
        target_amount = 20
        target_ht = 60
    else:
        target_amount = 20
        target_ht = 30

    # MPN для XL
    mpn = specs.get("Herstellernummer") or row.get("Herstellernummer") or f"XLM{ean}" if account == "xl" else f"JVM{ean}"
    if account == "xl" and not (mpn.startswith("XL") or mpn.startswith("XLM")):
        mpn = f"XLM{mpn}"

    # Описание (с кэшированием мастера)
    stamm_html = ""
    if stamm_id and stamm_id != "None" and stamm_id != "0":
        if stamm_id in stamm_cache:
            stamm_html = stamm_cache[stamm_id]
        else:
            try:
                stamm_html = await af_service.get_stammartikel_beschreibung(af_cookie, stamm_id)
                stamm_cache[stamm_id] = stamm_html
            except Exception:
                pass

    rich_desc = format_rich_description(title, specs, stamm_html)

    # 2. Получение текущего состояния с Kaufland
    pdata_res = await request_with_retry(rac, "GET", f"/product-data/{ean}?locale=de-DE", rate_limiter)
    pdata = pdata_res.data.get("data") if isinstance(pdata_res.data, dict) else {}
    attrs = pdata.get("attributes", {}) if isinstance(pdata, dict) else {}

    cat_detail = attrs.get("category_detail", [{}])
    curr_cat_id = cat_detail[0].get("id") if cat_detail else None
    curr_cat_name = cat_detail[0].get("name") if cat_detail else None

    target_cat_id, target_cat_name, target_cat_title = resolve_category(title, curr_cat_id, curr_cat_name)

    # 3. Обновление карточки товара (product-data)
    if dry_run:
        append_checkpoint(cp_file, ean, dry_run=dry_run)
        return "DRY_RUN_OK"

    new_attrs = attrs.copy()
    new_attrs["category"] = [target_cat_name]
    new_attrs["category_detail"] = {
        "id": target_cat_id,
        "title": target_cat_title,
        "name": target_cat_name
    }
    if account == "xl":
        new_attrs["mpn"] = [mpn]
    new_attrs["description"] = [rich_desc]

    put_payload = {"ean": [ean], "attributes": new_attrs}
    await request_with_retry(rac, "PUT", "/product-data?locale=de-DE", rate_limiter, json_payload=put_payload)

    # 4. Обновление офферов (units)
    ures = await request_with_retry(rac, "GET", f"/units?storefront=de&ean={ean}", rate_limiter)
    units = ures.data.get("data", []) if isinstance(ures.data, dict) else []

    if units:
        for u in units:
            unit_id = u.get("id_unit")
            curr_amt = u.get("amount")
            curr_ht = u.get("handling_time")
            if curr_amt != target_amount or curr_ht != target_ht:
                await request_with_retry(
                    rac, "PATCH", f"/units/{unit_id}?storefront=de",
                    rate_limiter, json_payload={"amount": target_amount, "handling_time": target_ht}
                )
    else:
        if price > 15.0:
            price_payload = rac.make_price_payload(storefront="de", price=price)
            unit_data = {
                "amount": target_amount,
                "handling_time": target_ht,
                "listing_price": price_payload["listing_price"],
                "minimum_price": price_payload["minimum_price"],
                "ean": ean,
                "id_offer": ean,
                "condition": "NEW",
                "id_shipping_group": 100003,
            }
            try:
                await request_with_retry(rac, "POST", "/units?storefront=de&embedded=eco_participation", rate_limiter, json_payload=unit_data)
            except Exception as e:
                err_logger.warning(f"[{account.upper()}][{ean}] Не удалось создать новый юнит: {e}")

    # Запись в чекпоинт
    append_checkpoint(cp_file, ean, dry_run=dry_run)
    return "SUCCESS"


async def run_pipeline_for_account(
    account: str,
    valid_file: Path,
    cp_file: Path,
    limit: Optional[int],
    concurrency: int,
    dry_run: bool,
    main_logger: logging.Logger,
    err_logger: logging.Logger,
):
    main_logger.info("=" * 80)
    main_logger.info(f"ЗАПУСК ПАЙПЛАЙНА ДЛЯ АККАУНТА: {account.upper()}")
    main_logger.info(f"Файл белого списка: {valid_file}")
    main_logger.info(f"Файл чекпоинта: {cp_file}")
    main_logger.info(f"Воркеров (concurrency): {concurrency}")
    main_logger.info(f"Режим DRY-RUN: {dry_run}")
    main_logger.info("=" * 80)

    if not valid_file.exists():
        main_logger.error(f"Файл {valid_file} не найден!")
        return

    with open(valid_file, "r", encoding="utf-8") as f:
        all_eans = [line.strip() for line in f if re.match(r"^\d{13}$", line.strip())]

    completed_eans = load_checkpoint(cp_file)
    pending_eans = [e for e in all_eans if e not in completed_eans]

    if limit:
        pending_eans = pending_eans[:limit]

    total_all = len(all_eans)
    total_done = len(completed_eans)
    total_pending = len(pending_eans)

    main_logger.info(f"Всего в белом списке: {total_all:,} | Уже сделано: {total_done:,} | К обработке: {total_pending:,}")

    if not pending_eans:
        main_logger.info(f"Все товары для {account.upper()} уже обработаны!")
        return

    af_service = AftercoolService()
    af_cookie = await af_service.login()
    rate_limiter = SafeRateLimiter(60.0)
    stamm_cache: Dict[str, str] = {}

    stats = {
        "success": 0,
        "errors": 0,
        "not_in_af": 0,
    }
    processed_count = 0
    lock = asyncio.Lock()

    queue: asyncio.Queue[Tuple[int, str]] = asyncio.Queue()
    for idx, ean in enumerate(pending_eans, 1):
        queue.put_nowait((idx, ean))

    async with aiohttp.ClientSession() as session:
        rac = RestApiController(account, session)

        async def worker(worker_id: int):
            nonlocal processed_count
            while not queue.empty():
                try:
                    idx, ean = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                try:
                    res = await process_single_item(
                        account=account,
                        ean=ean,
                        af_service=af_service,
                        af_cookie=af_cookie,
                        rac=rac,
                        rate_limiter=rate_limiter,
                        cp_file=cp_file,
                        stamm_cache=stamm_cache,
                        main_logger=main_logger,
                        err_logger=err_logger,
                        dry_run=dry_run,
                    )
                    async with lock:
                        processed_count += 1
                        if res in ("SUCCESS", "DRY_RUN_OK"):
                            stats["success"] += 1
                        elif res == "NOT_IN_AFTERCOOL":
                            stats["not_in_af"] += 1

                        if processed_count % 50 == 0 or processed_count == 1:
                            pct = ((total_done + processed_count) / total_all) * 100
                            main_logger.info(f"[{account.upper()}][{total_done + processed_count:,}/{total_all:,} {pct:.1f}%] EAN {ean} -> {res}")

                except Exception as e:
                    async with lock:
                        processed_count += 1
                        stats["errors"] += 1
                        err_logger.error(f"[{account.upper()}][{ean}] Сбой обработки: {e}", exc_info=True)
                        append_checkpoint(cp_file, ean, dry_run=dry_run)

                finally:
                    queue.task_done()

                if processed_count % 500 == 0:
                    gc.collect()

        workers = [asyncio.create_task(worker(i)) for i in range(concurrency)]
        await asyncio.gather(*workers)

    main_logger.info(f"\n[{account.upper()}] ИТОГИ ПРОГОНА: Успешно: {stats['success']:,}, Ошибок: {stats['errors']:,}, Не найдены в Aftercool: {stats['not_in_af']:,}")


def write_pid(account: str):
    pid_file = BACKEND_DIR / f"sync_{account}.pid"
    with open(pid_file, "w", encoding="utf-8") as f:
        f.write(str(os.getpid()))


def remove_pid(account: str):
    pid_file = BACKEND_DIR / f"sync_{account}.pid"
    if pid_file.exists():
        try:
            pid_file.unlink()
        except Exception:
            pass


async def main():
    parser = argparse.ArgumentParser(description="Автономная синхронизация каталогов JV и XL на Kaufland")
    parser.add_argument("--account", choices=["jv", "xl", "all"], default="all", help="Целевой аккаунт (jv, xl, all)")
    parser.add_argument("--concurrency", type=int, default=15, help="Количество параллельных воркеров (по умолчанию 15)")
    parser.add_argument("--limit", type=int, default=None, help="Ограничение количества товаров")
    parser.add_argument("--dry-run", action="store_true", help="Режим симуляции (без отправки изменений)")
    args = parser.parse_args()

    write_pid(args.account)
    main_logger, err_logger = setup_logger(args.account)

    main_logger.info("=" * 80)
    main_logger.info("ЗАПУСК АВТОНОМНОГО СЕРВИСА СИНХРОНИЗАЦИИ KAUFLAND")
    main_logger.info(f"Аккаунт: {args.account.upper()}")
    main_logger.info(f"PID процесса: {os.getpid()}")
    main_logger.info(f"Время старта: {datetime.now(ZoneInfo('Europe/Berlin')).strftime('%Y-%m-%d %H:%M:%S')}")
    main_logger.info("=" * 80)

    try:
        if args.account in ("jv", "all"):
            await run_pipeline_for_account(
                account="jv",
                valid_file=JV_VALID_FILE,
                cp_file=CP_JV_FILE,
                limit=args.limit,
                concurrency=args.concurrency,
                dry_run=args.dry_run,
                main_logger=main_logger,
                err_logger=err_logger,
            )

        if args.account in ("xl", "all"):
            await run_pipeline_for_account(
                account="xl",
                valid_file=XL_VALID_FILE,
                cp_file=CP_XL_FILE,
                limit=args.limit,
                concurrency=args.concurrency,
                dry_run=args.dry_run,
                main_logger=main_logger,
                err_logger=err_logger,
            )

        main_logger.info("=" * 80)
        main_logger.info("ВСЕ ЗАДАЧИ СИНХРОНИЗАЦИИ ПОЛНОСТЬЮ ВЫПОЛНЕНЫ!")
        main_logger.info("=" * 80)

    finally:
        remove_pid(args.account)


if __name__ == "__main__":
    asyncio.run(main())
