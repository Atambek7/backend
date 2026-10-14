#!/usr/bin/env python3
"""
Скрипт для гарантированного удаления мусорных товаров (Delette, Product removed и файлов Loschen)
из Kaufland (удаление units со всех витрин и удаление product-data по всем локалям).
"""
import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Set

import aiohttp

BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BACKEND_DIR))

from main_api.src.controller.rest_api_controller import RestApiController
from scripts.reupload_aftercool_catalog import (
    GlobalRateLimiter,
    CheckpointManager,
    purge_ean_from_account,
)
import config

handlers = [logging.StreamHandler(sys.stdout)]
log_path = BACKEND_DIR / "logs" / "cleanup_trash_units.log"
try:
    handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
except PermissionError:
    user_log_path = BACKEND_DIR / "logs" / "cleanup_trash_units_run.log"
    handlers.append(logging.FileHandler(user_log_path, encoding="utf-8"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=handlers,
)
logger = logging.getLogger("cleanup_trash")


async def purge_worker(
    queue: asyncio.Queue,
    account: str,
    rac: RestApiController,
    rate_limiter: GlobalRateLimiter,
    cp_mgr: CheckpointManager,
    dry_run: bool,
    completed_file: Path,
    stats: dict,
    lock: asyncio.Lock,
):
    while True:
        ean = await queue.get()
        if ean is None:
            queue.task_done()
            break
        try:
            res = await purge_ean_from_account(
                ean=ean,
                account=account,
                rac=rac,
                rate_limiter=rate_limiter,
                cp_mgr=cp_mgr,
                logger=logger,
                dry_run=dry_run,
                reason="trash_cleanup",
            )
            is_success = res.get("success", False)
            deleted_units = res.get("deleted_units", [])
            async with lock:
                stats["processed"] += 1
                if is_success:
                    stats["success"] += 1
                    stats["units_deleted"] += len(deleted_units)
                    with open(completed_file, "a", encoding="utf-8") as f:
                        f.write(f"{ean}\n")
                else:
                    stats["failed"] += 1
            if stats["processed"] % 50 == 0:
                logger.info(
                    f"[{account}] Progress: processed={stats['processed']}, "
                    f"success={stats['success']}, units_deleted={stats['units_deleted']}, "
                    f"failed={stats['failed']}"
                )
        except Exception as e:
            logger.error(f"[{account}][{ean}] Fatal purge error: {e}")
            async with lock:
                stats["failed"] += 1
        finally:
            queue.task_done()


async def main():
    parser = argparse.ArgumentParser(description="Purge trash units from Kaufland")
    parser.add_argument("--account", type=str, default="jv", help="Account (jv/xl)")
    parser.add_argument("--file", type=str, default="/tmp/jv_trash_eans_to_purge.txt", help="Path to EAN list")
    parser.add_argument("--concurrency", type=int, default=10, help="Concurrency")
    parser.add_argument("--rate-limit", type=float, default=18.0, help="Rate limit req/s")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    parser.add_argument("--limit", type=int, default=0, help="Max EANs to process (0 = all)")
    parser.add_argument("--ignore-completed", action="store_true", help="Ignore completed checkpoint and force purge all EANs in file")
    args = parser.parse_args()

    account = args.account.lower()
    input_path = Path(args.file)
    if not input_path.exists():
        logger.error(f"Input file {input_path} not found!")
        return

    with open(input_path, "r", encoding="utf-8") as f:
        all_eans = [line.strip() for line in f if line.strip()]

    completed_file = BACKEND_DIR / f"completed_trash_purge_{account}.txt"
    completed_eans = set()
    if not args.ignore_completed and completed_file.exists():
        with open(completed_file, "r", encoding="utf-8") as f:
            completed_eans = set(line.strip() for line in f if line.strip())

    # Жесткий фильтр безопасности: никогда не удалять разрешенные товары и фабрики Timur for Otto
    protected_eans = set()
    timur_file = BACKEND_DIR / "reports" / "eans_group2_timur_protected.txt"
    if timur_file.exists():
        with open(timur_file, "r", encoding="utf-8") as f:
            protected_eans.update(line.strip() for line in f if line.strip())
    allowed_file = BACKEND_DIR / "reports" / "eans_group1_allowed_protected.txt"
    if allowed_file.exists():
        with open(allowed_file, "r", encoding="utf-8") as f:
            protected_eans.update(line.strip() for line in f if line.strip())
    verified_file = BACKEND_DIR / "completed_verified_all.txt"
    if verified_file.exists():
        with open(verified_file, "r", encoding="utf-8") as f:
            protected_eans.update(line.strip() for line in f if line.strip())

    filtered_protected = [e for e in all_eans if e in protected_eans]
    if filtered_protected:
        logger.warning(
            f"[{account}] SAFETY GUARD INTERCEPTED {len(filtered_protected)} PROTECTED EANs! "
            f"They will NEVER be purged (Timur for Otto / Allowed factories)."
        )

    pending_eans = [e for e in all_eans if e not in completed_eans and e not in protected_eans]
    if args.limit > 0:
        pending_eans = pending_eans[:args.limit]

    logger.info(
        f"[{account}] Starting cleanup: total={len(all_eans)}, already_done={len(completed_eans)}, "
        f"pending_to_purge={len(pending_eans)}, concurrency={args.concurrency}, dry_run={args.dry_run}"
    )

    if not pending_eans:
        logger.info(f"[{account}] No pending EANs to purge. Exiting.")
        return

    rate_limiter = GlobalRateLimiter(rate=args.rate_limit)
    cp_mgr = CheckpointManager(account=account)
    queue = asyncio.Queue(maxsize=args.concurrency * 2)
    stats = {"processed": 0, "success": 0, "units_deleted": 0, "failed": 0}
    lock = asyncio.Lock()

    async with aiohttp.ClientSession() as session:
        rac = RestApiController(account, session)
        workers = [
            asyncio.create_task(
                purge_worker(
                    queue=queue,
                    account=account,
                    rac=rac,
                    rate_limiter=rate_limiter,
                    cp_mgr=cp_mgr,
                    dry_run=args.dry_run,
                    completed_file=completed_file,
                    stats=stats,
                    lock=lock,
                )
            )
            for _ in range(args.concurrency)
        ]

        for ean in pending_eans:
            await queue.put(ean)

        await queue.join()

        for _ in range(args.concurrency):
            await queue.put(None)
        await asyncio.gather(*workers)

    logger.info(
        f"[{account}] Cleanup finished! Total processed={stats['processed']}, "
        f"success={stats['success']}, units_deleted={stats['units_deleted']}, failed={stats['failed']}"
    )


if __name__ == "__main__":
    asyncio.run(main())
