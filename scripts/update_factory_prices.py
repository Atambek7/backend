#!/usr/bin/env python3
"""
Скрипт точечного и массового обновления цен товаров фабрик в Kaufland (JV & XL).

Сценарий использования:
1. Выгрузка актуальных цен (Startpreis) из Aftercool для указанных фабрик (по ID или имени).
2. Валидация принадлежности товара аккаунту через CatalogReconciler (защита от коллизий JV/XL) и фильтрация UK-складов.
3. Быстрый поиск и обновление офферов (юнитов) на всех витринах Kaufland через PATCH /units/{unit_id}.
4. Поддержка --dry-run (симуляция), многопоточности (token-bucket rate limiter) и логирования в CSV/TXT.
"""

import argparse
import asyncio
import csv
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Set, List, Dict, Any, Tuple

import aiohttp

# Add backend directory to sys.path
BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))

import config
from main_api.src.utils.price_utils import parse_price
from main_api.src.utils.ean_utils import normalize_ean, is_valid_ean
from main_api.src.controller.rest_api_controller import RestApiController
from main_api.services.aftercool_service import AftercoolService, AftercoolProductsQuery
from main_api.services.catalog_reconciler import CatalogReconciler, is_uk_source

DEFAULT_STOREFRONTS = ["de", "cz", "sk", "pl", "at", "fr", "it"]


def setup_logger(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("factory_price_updater")
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


class GlobalRateLimiter:
    """Токен-бакет ограничитель запросов для безопасной работы с API Kaufland."""
    def __init__(self, rate: float = 14.0):
        self.rate = rate
        self.tokens = rate
        self.last_time = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.last_time
                self.last_time = now
                self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                needed = (1.0 - self.tokens) / self.rate
                await asyncio.sleep(needed)


async def execute_with_retry(
    func,
    rate_limiter: Optional[GlobalRateLimiter] = None,
    max_retries: int = 5,
    initial_delay: float = 1.5,
):
    delay = initial_delay
    last_exc = None
    for attempt in range(max_retries):
        if rate_limiter:
            await rate_limiter.acquire()
        try:
            return await func()
        except Exception as e:
            last_exc = e
            err_str = str(e).lower()
            is_retryable = any(
                code in err_str for code in ["429", "500", "502", "503", "504", "timeout", "connection reset"]
            )
            if is_retryable and attempt < max_retries - 1:
                wait_sec = (delay * (1.5 ** attempt))
                if "429" in err_str:
                    wait_sec = max(wait_sec, 5.0)
                await asyncio.sleep(wait_sec)
                continue
            raise e
    raise last_exc


@dataclass
class PriceStats:
    total_factory_items: int = 0
    unique_eans: int = 0
    skipped_uk: int = 0
    skipped_invalid_ean: int = 0
    skipped_cross_account: int = 0
    units_checked: int = 0
    units_updated: int = 0
    units_already_equal: int = 0
    units_not_found: int = 0
    errors: int = 0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def inc(self, key: str, val: int = 1):
        async with self._lock:
            curr = getattr(self, key, 0)
            setattr(self, key, curr + val)


async def fetch_factory_items(
    aftercool: AftercoolService,
    session: str,
    account: str,
    factory_id: str,
    logger: logging.Logger,
    page_size: int = 200,
) -> List[Dict[str, Any]]:
    """Постраничная выгрузка товаров фабрики с include_row=1."""
    logger.info(f"[{account.upper()}] Fetching products for factory ID {factory_id} from Aftercool...")
    items = await aftercool.get_products_paginated(
        session,
        account=account,
        dataset="lister",
        factory_id=factory_id,
        include_row=1,
        page_size=page_size,
        max_pages=200,
    )
    logger.info(f"[{account.upper()}] Factory ID {factory_id}: retrieved {len(items):,} items from Aftercool.")
    return items


async def process_ean_prices(
    ean: str,
    target_price: float,
    source_factory: str,
    account: str,
    storefronts: List[str],
    rac: RestApiController,
    rate_limiter: GlobalRateLimiter,
    stats: PriceStats,
    audit_csv: Path,
    cp_file: Path,
    logger: logging.Logger,
    dry_run: bool = False,
    reconciler: Optional[CatalogReconciler] = None,
):
    """Сверка и обновление цен юнитов для одного EAN на всех витринах."""
    # 1. Проверка принадлежности через CatalogReconciler
    if reconciler and reconciler.is_initialized:
        target_acc, status = reconciler.resolve_target_account(ean)
        if target_acc != account:
            logger.warning(
                f"[{account.upper()}][{ean}] Cross-account conflict: item belongs to '{target_acc}' (status={status}), "
                f"skipping price update on '{account.upper()}'."
            )
            await stats.inc("skipped_cross_account")
            return

    async def process_storefront(sf: str):
        await stats.inc("units_checked")
        try:
            # Поиск юнита по id_offer (EAN)
            u_res = await execute_with_retry(
                lambda: rac.send_request("GET", f"/units?storefront={sf}&id_offer={ean}"),
                rate_limiter=rate_limiter,
                max_retries=2,
            )
            units = u_res.get("data", []) if isinstance(u_res, dict) else (u_res if isinstance(u_res, list) else [])
            if not units:
                # Фоллбэк: поиск по ean
                u_res2 = await execute_with_retry(
                    lambda: rac.send_request("GET", f"/units?storefront={sf}&ean={ean}"),
                    rate_limiter=rate_limiter,
                    max_retries=2,
                )
                units = u_res2.get("data", []) if isinstance(u_res2, dict) else (u_res2 if isinstance(u_res2, list) else [])

            if not units:
                await stats.inc("units_not_found")
                return

            for u in units:
                if not isinstance(u, dict):
                    continue
                unit_id = u.get("id_unit")
                if not unit_id:
                    continue

                curr_raw = u.get("price") or u.get("listing_price")
                price_payload = rac.make_price_payload(storefront=sf, price=target_price)
                target_cents = price_payload["listing_price"]
                diff_cents = abs(int(curr_raw or 0) - target_cents)

                # Если цена уже совпадает с точностью до цента, пропускаем
                if diff_cents == 0:
                    await stats.inc("units_already_equal")
                    continue

                curr_display = round(float(curr_raw) / 100.0, 2) if curr_raw else 0.0
                target_display = round(float(target_cents) / 100.0, 2)
                currency = "CZK" if sf == "cz" else ("PLN" if sf == "pl" else "€")
                diff_val = round(target_display - curr_display, 2)

                if dry_run:
                    logger.info(
                        f"[{account.upper()}][{ean}][{sf}] DRY-RUN: unit={unit_id} "
                        f"current={curr_display} {currency} -> new={target_display} {currency} (diff: {diff_val:+} {currency})"
                    )
                    await stats.inc("units_updated")
                else:
                    patch_data = {
                        "listing_price": target_cents,
                        "minimum_price": price_payload["minimum_price"],
                    }
                    await execute_with_retry(
                        lambda: rac.send_request("PATCH", f"/units/{unit_id}?storefront={sf}", json=patch_data),
                        rate_limiter=rate_limiter,
                        max_retries=3,
                    )
                    await stats.inc("units_updated")
                    logger.info(
                        f"[{account.upper()}][{ean}][{sf}] UPDATED: unit={unit_id} "
                        f"{curr_display} {currency} -> {target_display} {currency} ({diff_val:+} {currency})"
                    )

                    # Запись в аудит-лог
                    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    row = [now_str, ean, account.upper(), sf, str(unit_id), curr_display, target_display, diff_val, currency, source_factory]
                    try:
                        with open(audit_csv, "a", newline="", encoding="utf-8") as f:
                            writer = csv.writer(f)
                            writer.writerow(row)
                    except Exception:
                        pass

        except Exception as e:
            logger.error(f"[{account.upper()}][{ean}][{sf}] Failed to process price: {e}")
            await stats.inc("errors")

    await asyncio.gather(*[process_storefront(sf) for sf in storefronts])

    if not dry_run:
        try:
            with open(cp_file, "a", encoding="utf-8") as f:
                f.write(f"{ean}\n")
        except Exception:
            pass


async def main():
    parser = argparse.ArgumentParser(description="Update Kaufland unit prices for specified Aftercool factories")
    parser.add_argument("--account", type=str, default="jv", help="Kaufland account (jv or xl)")
    parser.add_argument("--factories", type=str, required=True, help="Comma-separated factory IDs (e.g. 409886,499428,499258)")
    parser.add_argument("--storefronts", type=str, default=",".join(DEFAULT_STOREFRONTS), help="Storefronts to update")
    parser.add_argument("--dry-run", action="store_true", help="Simulate price updates without modifying Kaufland")
    parser.add_argument("--resume", action="store_true", default=True, help="Skip already processed EANs (default: True)")
    parser.add_argument("--no-resume", dest="resume", action="store_false", help="Do not skip previously processed EANs")
    parser.add_argument("--concurrency", type=int, default=8, help="Worker coroutines count")
    parser.add_argument("--limit", type=int, default=0, help="Max items to process (0 = all)")
    parser.add_argument("--rate-limit", type=float, default=14.0, help="Max requests per second for Kaufland API")
    parser.add_argument("--skip-preindex", action="store_true", help="Skip pre-indexing Aftercool catalogs")
    args = parser.parse_args()

    account = args.account.lower()
    storefronts = [sf.strip().lower() for sf in args.storefronts.split(",") if sf.strip()]
    account_allowed = set(config.get_account_storefronts(account))
    effective_storefronts = [sf for sf in storefronts if sf in account_allowed]
    factory_ids = [f.strip() for f in args.factories.split(",") if f.strip()]

    logs_dir = BACKEND_DIR / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"price_update_{account}_{'_'.join(factory_ids)}_{ts_str}.log"
    logger = setup_logger(log_file)

    logger.info("=" * 70)
    logger.info(f"STARTING FACTORY PRICE UPDATE: account={account.upper()} | factories={factory_ids}")
    logger.info(f"Storefronts: {effective_storefronts} | dry_run={args.dry_run} | concurrency={args.concurrency}")
    logger.info("=" * 70)

    audit_csv = BACKEND_DIR / f"price_updates_audit_{account}.csv"
    if not audit_csv.exists() or audit_csv.stat().st_size == 0:
        with open(audit_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "ean", "account", "storefront", "unit_id", "old_price", "new_price", "diff", "currency", "factory"])

    cp_file = BACKEND_DIR / f"completed_price_updates_{account}.txt"
    completed_eans = set()
    if cp_file.exists():
        with open(cp_file, "r", encoding="utf-8") as f:
            completed_eans = {line.strip() for line in f if line.strip()}
    logger.info(f"Loaded {len(completed_eans):,} previously completed EANs from {cp_file.name}")

    aftercool = AftercoolService()
    try:
        session = await aftercool.login()
    except Exception as e:
        logger.error(f"Failed to login to Aftercool: {e}")
        return

    # Инициализация CatalogReconciler для безопасного разделения аккаунтов
    reconciler = CatalogReconciler(service=aftercool)
    if not args.skip_preindex:
        try:
            logger.info("Pre-indexing Aftercool catalogs for JV and XL to prevent cross-account collisions...")
            await reconciler.preload_indices(session, logger=logger)
        except Exception as e:
            logger.warning(f"CatalogReconciler preload failed: {e}. Continuing without cross-account check.")

    stats = PriceStats()
    rate_limiter = GlobalRateLimiter(rate=args.rate_limit)

    # 1. Сбор целевых EAN и цен по всем указанным фабрикам
    # Если EAN встречается в нескольких строках, выбираем наибольшую цену
    ean_targets: Dict[str, Tuple[float, str]] = {}  # ean -> (target_price, source_factory)

    for fid in factory_ids:
        items = await fetch_factory_items(aftercool, session, account, fid, logger)
        await stats.inc("total_factory_items", len(items))

        for it in items:
            source_file = str(it.get("source_file") or "")
            if is_uk_source(source_file):
                await stats.inc("skipped_uk")
                continue

            raw_ean = AftercoolService._extract_ean(it) or ""
            ean = normalize_ean(raw_ean)
            if not ean or not is_valid_ean(ean):
                await stats.inc("skipped_invalid_ean")
                continue

            row = it.get("row") or {}
            price_raw = row.get("Startpreis") or it.get("price") or "0"
            price = parse_price(price_raw)
            if price <= 0:
                continue

            if ean not in ean_targets:
                ean_targets[ean] = (price, source_file or f"fid:{fid}")
            else:
                # Если уже есть, берем максимальную цену
                prev_price, prev_fac = ean_targets[ean]
                if price > prev_price:
                    ean_targets[ean] = (price, source_file or f"fid:{fid}")

    if args.resume and completed_eans:
        initial_total = len(ean_targets)
        ean_targets = {e: v for e, v in ean_targets.items() if e not in completed_eans}
        skipped_count = initial_total - len(ean_targets)
        logger.info(f"Resume active: skipped {skipped_count:,} already processed EANs. Remaining to process: {len(ean_targets):,}")

    if args.limit > 0:
        ean_targets = dict(list(ean_targets.items())[:args.limit])
        logger.info(f"Limit applied: processing next {len(ean_targets)} unique EANs")

    await stats.inc("unique_eans", len(ean_targets))
    logger.info(f"Total unique valid EANs to update across factories {factory_ids}: {len(ean_targets):,}")

    # 2. Очередь воркеров для параллельной отправки в Kaufland API
    queue = asyncio.Queue()
    for ean, (target_price, fac_name) in ean_targets.items():
        await queue.put((ean, target_price, fac_name))

    async with aiohttp.ClientSession() as aio_session:
        rac = RestApiController(controller=account, session=aio_session)

        async def worker(worker_id: int):
            while True:
                item_tuple = await queue.get()
                if item_tuple is None:
                    queue.task_done()
                    break
                ean, target_price, fac_name = item_tuple
                try:
                    await process_ean_prices(
                        ean=ean,
                        target_price=target_price,
                        source_factory=fac_name,
                        account=account,
                        storefronts=effective_storefronts,
                        rac=rac,
                        rate_limiter=rate_limiter,
                        stats=stats,
                        audit_csv=audit_csv,
                        cp_file=cp_file,
                        logger=logger,
                        dry_run=args.dry_run,
                        reconciler=reconciler,
                    )
                except Exception as exc:
                    logger.error(f"[Worker {worker_id}][{ean}] Unhandled error: {exc}")
                finally:
                    queue.task_done()

        workers = [asyncio.create_task(worker(i)) for i in range(args.concurrency)]
        await queue.join()

        for _ in range(args.concurrency):
            await queue.put(None)
        await asyncio.gather(*workers)

    logger.info("=" * 70)
    logger.info(f"FINAL SUMMARY - FACTORY PRICE UPDATE ({account.upper()}):")
    logger.info(f"Total items fetched from Aftercool: {stats.total_factory_items:,}")
    logger.info(f"Unique valid EANs extracted:       {stats.unique_eans:,}")
    logger.info(f"Skipped UK warehouse items:         {stats.skipped_uk:,}")
    logger.info(f"Skipped invalid EANs:               {stats.skipped_invalid_ean:,}")
    logger.info(f"Skipped cross-account collisions:   {stats.skipped_cross_account:,}")
    logger.info(f"Kaufland units checked:             {stats.units_checked:,}")
    mode_str = "(DRY-RUN)" if args.dry_run else "(EXECUTED)"
    logger.info(f"Kaufland units updated {mode_str}:     {stats.units_updated:,}")
    logger.info(f"Kaufland units already up-to-date:  {stats.units_already_equal:,}")
    logger.info(f"Kaufland units not found:           {stats.units_not_found:,}")
    logger.info(f"API Errors:                         {stats.errors:,}")
    logger.info(f"Audit log:                          {audit_csv.name}")
    logger.info(f"Log file:                           {log_file.name}")
    logger.info("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
