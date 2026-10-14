#!/usr/bin/env python3
"""
Создание и обновление офферов для фабрик SOFORT_JV_OTTO_504251 и SOFORT_JV_2_504255.
Параметры:
- account: jv
- quantity (amount): 1
- handling_time: 7 дней
- исключены мусорные EAN: 4072102286657, 4072102287630
"""
import asyncio
import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Dict, Tuple, Optional

import aiohttp

BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BACKEND_DIR))

from main_api.src.controller.rest_api_controller import RestApiController
from main_api.services.aftercool_service import AftercoolService, AftercoolProductsQuery
from main_api.src.utils.price_utils import parse_price
from scripts.purge_trash_catalog_items import request_with_retry, SafeRateLimiter
from scripts.reupload_aftercool_catalog import (
    get_default_shipping_group,
    SHIPPING_GROUP_CACHE,
    DEFAULT_STOREFRONTS,
)

EXCLUDED_EANS = {"4072102286657", "4072102287630"}
TARGET_FACTORIES = ["504251", "504255"]
HANDLING_TIME = 7
AMOUNT = 1

def setup_logger(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("sofort_offers")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger

async def main():
    parser = argparse.ArgumentParser(description="Create/update SOFORT offers on Kaufland JV")
    parser.add_argument("--dry-run", action="store_true", help="Dry run without modifying units")
    parser.add_argument("--storefronts", nargs="+", default=DEFAULT_STOREFRONTS, help="Target storefronts")
    parser.add_argument("--rate-limit", type=float, default=15.0, help="Max requests per second")
    args = parser.parse_args()

    logs_dir = BACKEND_DIR / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"create_sofort_offers_{ts}.log"
    logger = setup_logger(log_file)

    logger.info("=== ЗАПУСК СОЗДАНИЯ ОФФЕРОВ ДЛЯ ФАБРИК 504251 И 504255 (JV) ===")
    logger.info(f"Параметры: amount={AMOUNT}, handling_time={HANDLING_TIME} дней, dry_run={args.dry_run}")
    logger.info(f"Целевые витрины: {args.storefronts}")
    logger.info(f"Исключены EAN: {EXCLUDED_EANS}")

    # 1. Получение товаров из Aftercool
    af_service = AftercoolService()
    try:
        cookie = await af_service.login()
        logger.info("Успешная авторизация в Aftercool.")
    except Exception as e:
        logger.error(f"Не удалось авторизоваться в Aftercool: {e}")
        return

    items_to_process = []
    for fid in TARGET_FACTORIES:
        try:
            prods = await af_service.get_products(cookie, query=AftercoolProductsQuery(account="jv", factory_id=fid, limit=200))
            logger.info(f"Фабрика {fid}: получено {len(prods)} товаров из Aftercool.")
            for it in prods:
                ean = str(it.get("ean") or "").strip()
                if not ean or ean in EXCLUDED_EANS:
                    logger.info(f"  Пропуск EAN {ean} (мусорный / пустой)")
                    continue
                row = it.get("row") or {}
                price_str = row.get("Startpreis") or it.get("price") or row.get("price")
                price_eur = parse_price(price_str)
                if price_eur <= 0:
                    logger.warning(f"  [EAN {ean}] Некорректная цена: '{price_str}', пропуск!")
                    continue
                items_to_process.append({
                    "ean": ean,
                    "name": it.get("name") or "",
                    "price_eur": price_eur,
                    "factory_id": fid
                })
        except Exception as e:
            logger.error(f"Ошибка получения товаров для фабрики {fid}: {e}")
            return

    logger.info(f"\nВсего товаров к обработке: {len(items_to_process)}")
    rate_limiter = SafeRateLimiter(max_rps=args.rate_limit)

    # 2. Обработка товаров на Kaufland JV
    async with aiohttp.ClientSession() as session:
        rac = RestApiController("jv", session)
        
        # Предзагрузка групп доставки
        for sf in args.storefronts:
            sg_id = await get_default_shipping_group(rac, rate_limiter, sf, SHIPPING_GROUP_CACHE, account="jv", logger=logger)
            logger.info(f"Витрина '{sf}' -> id_shipping_group = {sg_id}")

        success_count = 0
        error_count = 0

        for idx, item in enumerate(items_to_process, 1):
            ean = item["ean"]
            price_eur = item["price_eur"]
            fac_id = item["factory_id"]
            name = item["name"][:35]
            
            logger.info(f"[{idx}/{len(items_to_process)}][Fac {fac_id}][EAN {ean}] Обработка (цена: {price_eur:.2f} € | {name})...")
            
            item_success = True
            for sf in args.storefronts:
                price_payload = rac.make_price_payload(storefront=sf, price=price_eur)
                listing_price = price_payload["listing_price"]
                sg_id = SHIPPING_GROUP_CACHE.get(("jv", sf))
                
                if args.dry_run:
                    logger.info(f"  [DRY-RUN][{sf}] Будет отправлен оффер: amount={AMOUNT}, handling_time={HANDLING_TIME}, price={listing_price}, sg={sg_id}")
                    continue

                # Проверяем существующий юнит
                unit_res = await request_with_retry(rac, "GET", f"/units?storefront={sf}&id_offer={ean}", rate_limiter)
                units = unit_res.data.get("data", []) if isinstance(unit_res.data, dict) else (unit_res.data if isinstance(unit_res.data, list) else [])
                
                if units:
                    unit_id = units[0].get("id_unit")
                    patch_body = {
                        "amount": AMOUNT,
                        "handling_time": HANDLING_TIME,
                        "listing_price": listing_price,
                        "minimum_price": listing_price,
                    }
                    if sg_id:
                        patch_body["id_shipping_group"] = sg_id
                    
                    patch_res = await request_with_retry(rac, "PATCH", f"/units/{unit_id}?storefront={sf}", rate_limiter, json_payload=patch_body)
                    if patch_res.status in (200, 204):
                        logger.info(f"  [{sf}] Юнит {unit_id} ОБНОВЛЕН: amount={AMOUNT}, handling_time={HANDLING_TIME}")
                    else:
                        logger.error(f"  [{sf}] Ошибка PATCH юнита {unit_id}: status={patch_res.status}, body={patch_res.data}")
                        item_success = False
                else:
                    # Создаем новый юнит
                    post_body = {
                        "amount": AMOUNT,
                        "handling_time": HANDLING_TIME,
                        "listing_price": listing_price,
                        "minimum_price": listing_price,
                        "ean": ean,
                        "id_offer": ean,
                        "condition": "NEW",
                    }
                    if sg_id:
                        post_body["id_shipping_group"] = sg_id
                    
                    post_res = await request_with_retry(rac, "POST", f"/units?storefront={sf}&embedded=eco_participation", rate_limiter, json_payload=post_body)
                    if post_res.status in (200, 201, 204):
                        logger.info(f"  [{sf}] Новый юнит СОЗДАН: amount={AMOUNT}, handling_time={HANDLING_TIME}")
                    elif post_res.status == 409 or "already" in str(post_res.data).lower():
                        # Если дубликат/уже создан, делаем повторную попытку найти и запатчить
                        logger.info(f"  [{sf}] Оффер уже существовал (409), поиск юнита для обновления...")
                        u_find = await request_with_retry(rac, "GET", f"/units?storefront={sf}&id_offer={ean}", rate_limiter)
                        u_list = u_find.data.get("data", []) if isinstance(u_find.data, dict) else (u_find.data if isinstance(u_find.data, list) else [])
                        if u_list:
                            u_id = u_list[0].get("id_unit")
                            await request_with_retry(rac, "PATCH", f"/units/{u_id}?storefront={sf}", rate_limiter, json_payload={"amount": AMOUNT, "handling_time": HANDLING_TIME, "listing_price": listing_price, "minimum_price": listing_price})
                            logger.info(f"  [{sf}] Юнит {u_id} успешно обновлен через fallback!")
                        else:
                            logger.error(f"  [{sf}] 409 получен, но юнит не найден: {post_res.data}")
                            item_success = False
                    else:
                        logger.error(f"  [{sf}] Ошибка POST юнита: status={post_res.status}, body={post_res.data}")
                        item_success = False

            if item_success:
                success_count += 1
            else:
                error_count += 1

    logger.info("\n" + "=" * 60)
    logger.info(f"ИТОГ: Успешно обработано товаров: {success_count} / {len(items_to_process)}")
    if error_count > 0:
        logger.warning(f"Ошибок: {error_count}")
    logger.info(f"Подробный лог сохранен в: {log_file}")
    logger.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
