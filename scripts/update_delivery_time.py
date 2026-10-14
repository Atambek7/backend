#!/usr/bin/env python3
"""
Update Kaufland offer handling time from 6-10 weeks to 4-8 weeks.

In this project, upload logic maps those windows as:
    6-10 weeks -> handling_time 40
    4-8 weeks  -> handling_time 28

The script scans Kaufland units for JV and XL across all configured storefronts,
patches matching units, and writes a dedicated statistics log.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(BACKEND_DIR / ".env")
load_dotenv()

from config import controllers_base, storefronts as configured_storefronts  # noqa: E402


BASE_API_URL = "https://sellerapi.kaufland.com/v2"
DEFAULT_FROM_HANDLING_TIME = 40
DEFAULT_TO_HANDLING_TIME = 28
DEFAULT_LIMIT = 100
DEFAULT_REQUEST_DELAY_SEC = 0.08
RETRY_HTTP_STATUSES = {429, 500, 502, 503, 504}


class KauflandScriptError(Exception):
    pass


@dataclass
class ScopeStats:
    fetched: int = 0
    matched: int = 0
    updated: int = 0
    failed: int = 0
    skipped_missing_unit_id: int = 0
    skipped_no_handling_time: int = 0
    skipped_dry_run: int = 0


@dataclass
class RunStats:
    scopes: dict[str, ScopeStats] = field(default_factory=dict)

    def scope(self, controller: str, storefront: str) -> ScopeStats:
        key = f"{controller}/{storefront}"
        if key not in self.scopes:
            self.scopes[key] = ScopeStats()
        return self.scopes[key]

    def totals(self) -> ScopeStats:
        total = ScopeStats()
        for scope in self.scopes.values():
            total.fetched += scope.fetched
            total.matched += scope.matched
            total.updated += scope.updated
            total.failed += scope.failed
            total.skipped_missing_unit_id += scope.skipped_missing_unit_id
            total.skipped_no_handling_time += scope.skipped_no_handling_time
            total.skipped_dry_run += scope.skipped_dry_run
        return total


class KauflandClient:
    def __init__(
        self,
        *,
        session: aiohttp.ClientSession,
        controller: str,
        request_delay_sec: float,
        max_retries: int,
        retry_base_delay: float,
        logger: logging.Logger,
    ) -> None:
        self.session = session
        self.controller = controller
        self.request_delay_sec = request_delay_sec
        self.max_retries = max(0, max_retries)
        self.retry_base_delay = max(0.0, retry_base_delay)
        self.logger = logger
        self.client_key = controllers_base[controller]["client_key"]
        self.secret_key = controllers_base[controller]["secret_key"]
        self.last_request_at = 0.0

        if not self.client_key or not self.secret_key:
            raise KauflandScriptError(
                f"Missing Kaufland credentials for controller={controller}"
            )

    @staticmethod
    def _serialize_body(body: Any) -> str:
        if body is None or body == "":
            return ""
        if isinstance(body, (dict, list)):
            return json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        return str(body)

    def _sign_request(
        self,
        *,
        method: str,
        url: str,
        body: str,
        timestamp: str,
    ) -> str:
        plain_text = "\n".join([method, url, body, timestamp])
        digest_maker = hmac.new(self.secret_key.encode(), None, hashlib.sha256)
        digest_maker.update(plain_text.encode())
        return digest_maker.hexdigest()

    def _headers(self, *, method: str, url: str, body: str, has_payload: bool) -> dict:
        timestamp = str(int(time.time()))
        headers = {
            "Accept": "application/json",
            "User-Agent": "Inhouse_development",
            "Shop-Timestamp": timestamp,
            "Shop-Client-Key": self.client_key,
            "Shop-Signature": self._sign_request(
                method=method,
                url=url,
                body=body,
                timestamp=timestamp,
            ),
        }
        if has_payload:
            headers["Content-Type"] = "application/json"
        return headers

    async def request(self, method: str, endpoint: str, body: Any = None) -> dict:
        last_error: Exception | None = None

        for attempt in range(self.max_retries + 1):
            await self._apply_delay()

            url = f"{BASE_API_URL}{endpoint}"
            serialized_body = self._serialize_body(body)
            has_payload = body is not None
            headers = self._headers(
                method=method,
                url=url,
                body=serialized_body,
                has_payload=has_payload,
            )
            kwargs = {"headers": headers}
            if has_payload:
                kwargs["data"] = serialized_body

            try:
                async with self.session.request(method, url, **kwargs) as response:
                    response_text = await response.text()
                    if response.status >= 400:
                        error = KauflandScriptError(
                            f"{method} {endpoint} failed: HTTP {response.status} - {response_text}"
                        )
                        if (
                            response.status in RETRY_HTTP_STATUSES
                            and attempt < self.max_retries
                        ):
                            await self._retry_sleep(
                                attempt=attempt,
                                endpoint=endpoint,
                                error=error,
                            )
                            continue
                        raise error
                    if response.status == 204 or not response_text.strip():
                        return {"status": response.status, "data": None}
                    try:
                        data = json.loads(response_text)
                    except json.JSONDecodeError:
                        data = response_text
                    if isinstance(data, dict):
                        data.setdefault("status", response.status)
                        return data
                    return {"status": response.status, "data": data}
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    await self._retry_sleep(
                        attempt=attempt,
                        endpoint=endpoint,
                        error=exc,
                    )
                    continue
                raise KauflandScriptError(
                    f"{method} {endpoint} failed after {attempt + 1} attempts: {exc}"
                )

        raise KauflandScriptError(
            f"{method} {endpoint} failed after retries: {last_error}"
        )

    async def _apply_delay(self) -> None:
        if self.request_delay_sec <= 0:
            return
        now = time.monotonic()
        wait_seconds = (self.last_request_at + self.request_delay_sec) - now
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
        self.last_request_at = time.monotonic()

    async def _retry_sleep(
        self,
        *,
        attempt: int,
        endpoint: str,
        error: Exception,
    ) -> None:
        delay = self.retry_base_delay * (2**attempt)
        self.logger.warning(
            "retry controller=%s endpoint=%s attempt=%s/%s wait_sec=%.2f error=%s",
            self.controller,
            endpoint,
            attempt + 1,
            self.max_retries,
            delay,
            error,
        )
        if delay > 0:
            await asyncio.sleep(delay)


def configure_logger(log_file: Path) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("delivery-time-update")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    formatter.converter = lambda *args: datetime.now(
        ZoneInfo("Asia/Almaty")
    ).timetuple()

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def response_items(response: dict) -> list[dict]:
    data = response.get("data") if isinstance(response, dict) else None
    return data if isinstance(data, list) else []


async def fetch_units(
    *,
    client: KauflandClient,
    storefront: str,
    limit: int,
    logger: logging.Logger,
) -> list[dict]:
    units: list[dict] = []
    offset = 0

    while True:
        response = await client.request(
            "GET",
            f"/units?storefront={storefront}&limit={limit}&offset={offset}",
        )
        chunk = response_items(response)
        units.extend(chunk)
        logger.info(
            "fetch controller=%s storefront=%s offset=%s count=%s total_so_far=%s",
            client.controller,
            storefront,
            offset,
            len(chunk),
            len(units),
        )

        if len(chunk) < limit:
            return units
        offset += limit


def coerce_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return None


def find_handling_time(unit: dict) -> tuple[str | None, int | None]:
    for field_name in ("handling_time", "delivery_time_min", "delivery"):
        value = coerce_int(unit.get(field_name))
        if value is not None:
            return field_name, value
    return None, None


def unit_identity(unit: dict) -> str:
    return str(
        unit.get("ean")
        or unit.get("id_offer")
        or unit.get("id_product")
        or "<unknown-product>"
    )


async def patch_unit(
    *,
    client: KauflandClient,
    storefront: str,
    unit_id: Any,
    to_handling_time: int,
) -> dict:
    return await client.request(
        "PATCH",
        f"/units/{unit_id}?storefront={storefront}&embedded=eco_participation",
        body={"handling_time": to_handling_time},
    )


def patch_succeeded(response: dict) -> bool:
    if response.get("status") in {200, 204}:
        return True
    message = str(response.get("message", "")).lower()
    return message == "success"


async def process_scope(
    *,
    session: aiohttp.ClientSession,
    controller: str,
    storefront: str,
    from_handling_time: int,
    to_handling_time: int,
    limit: int,
    request_delay_sec: float,
    max_retries: int,
    retry_base_delay: float,
    dry_run: bool,
    logger: logging.Logger,
    stats: RunStats,
) -> None:
    scope = stats.scope(controller, storefront)
    client = KauflandClient(
        session=session,
        controller=controller,
        request_delay_sec=request_delay_sec,
        max_retries=max_retries,
        retry_base_delay=retry_base_delay,
        logger=logger,
    )

    logger.info("scope_start controller=%s storefront=%s", controller, storefront)
    units = await fetch_units(
        client=client,
        storefront=storefront,
        limit=limit,
        logger=logger,
    )
    scope.fetched = len(units)

    for unit in units:
        field_name, handling_time = find_handling_time(unit)
        if handling_time is None:
            scope.skipped_no_handling_time += 1
            logger.info(
                "skip_no_handling_time controller=%s storefront=%s unit_id=%s product=%s",
                controller,
                storefront,
                unit.get("id_unit"),
                unit_identity(unit),
            )
            continue

        if handling_time != from_handling_time:
            continue

        scope.matched += 1
        unit_id = unit.get("id_unit")
        if not unit_id:
            scope.skipped_missing_unit_id += 1
            logger.error(
                "skip_missing_unit_id controller=%s storefront=%s product=%s field=%s value=%s",
                controller,
                storefront,
                unit_identity(unit),
                field_name,
                handling_time,
            )
            continue

        if dry_run:
            scope.skipped_dry_run += 1
            logger.info(
                "dry_run_match controller=%s storefront=%s unit_id=%s product=%s field=%s from=%s to=%s",
                controller,
                storefront,
                unit_id,
                unit_identity(unit),
                field_name,
                handling_time,
                to_handling_time,
            )
            continue

        try:
            response = await patch_unit(
                client=client,
                storefront=storefront,
                unit_id=unit_id,
                to_handling_time=to_handling_time,
            )
            if patch_succeeded(response):
                scope.updated += 1
                logger.info(
                    "updated controller=%s storefront=%s unit_id=%s product=%s from=%s to=%s",
                    controller,
                    storefront,
                    unit_id,
                    unit_identity(unit),
                    handling_time,
                    to_handling_time,
                )
            else:
                scope.failed += 1
                logger.error(
                    "update_failed controller=%s storefront=%s unit_id=%s product=%s response=%s",
                    controller,
                    storefront,
                    unit_id,
                    unit_identity(unit),
                    json.dumps(response, ensure_ascii=False, default=str),
                )
        except Exception as exc:
            scope.failed += 1
            logger.exception(
                "update_exception controller=%s storefront=%s unit_id=%s product=%s error=%s",
                controller,
                storefront,
                unit_id,
                unit_identity(unit),
                exc,
            )

    logger.info(
        "scope_done controller=%s storefront=%s fetched=%s matched=%s updated=%s failed=%s dry_run=%s missing_unit_id=%s no_handling_time=%s",
        controller,
        storefront,
        scope.fetched,
        scope.matched,
        scope.updated,
        scope.failed,
        scope.skipped_dry_run,
        scope.skipped_missing_unit_id,
        scope.skipped_no_handling_time,
    )


async def run(args: argparse.Namespace, logger: logging.Logger) -> RunStats:
    stats = RunStats()
    start_seen = args.start_at is None

    timeout = aiohttp.ClientTimeout(total=args.timeout)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for controller in args.controllers:
            if controller not in controllers_base:
                raise KauflandScriptError(f"Unknown controller: {controller}")
            for storefront in args.storefronts:
                scope_key = f"{controller}/{storefront}"
                if not start_seen:
                    if scope_key == args.start_at:
                        start_seen = True
                    else:
                        logger.info("skip_before_start scope=%s", scope_key)
                        continue

                await process_scope(
                    session=session,
                    controller=controller,
                    storefront=storefront,
                    from_handling_time=args.from_handling_time,
                    to_handling_time=args.to_handling_time,
                    limit=args.limit,
                    request_delay_sec=args.request_delay,
                    max_retries=args.max_retries,
                    retry_base_delay=args.retry_base_delay,
                    dry_run=args.dry_run,
                    logger=logger,
                    stats=stats,
                )

    if args.start_at and not start_seen:
        raise KauflandScriptError(f"--start-at scope was not found: {args.start_at}")

    return stats


def parse_csv_arg(value: str) -> list[str]:
    return [part.strip().lower() for part in value.split(",") if part.strip()]


def parse_start_at(value: str) -> str:
    normalized = value.strip().lower()
    if "/" not in normalized:
        raise argparse.ArgumentTypeError("Expected format: controller/storefront")
    controller, storefront = normalized.split("/", 1)
    if not controller or not storefront:
        raise argparse.ArgumentTypeError("Expected format: controller/storefront")
    return normalized


def build_parser() -> argparse.ArgumentParser:
    default_log_file = (
        BACKEND_DIR
        / "logs"
        / f"delivery_time_update_{datetime.now(ZoneInfo('Asia/Almaty')).strftime('%Y%m%d_%H%M%S')}.log"
    )

    parser = argparse.ArgumentParser(
        description=(
            "Scan Kaufland units in JV and XL and change handling_time 40 "
            "(6-10 weeks) to 28 (4-8 weeks)."
        )
    )
    parser.add_argument(
        "--controllers",
        type=parse_csv_arg,
        default=["jv", "xl"],
        help="Comma-separated controllers to process. Default: jv,xl",
    )
    parser.add_argument(
        "--storefronts",
        type=parse_csv_arg,
        default=list(configured_storefronts),
        help=f"Comma-separated storefronts. Default: {','.join(configured_storefronts)}",
    )
    parser.add_argument(
        "--from-handling-time",
        type=int,
        default=DEFAULT_FROM_HANDLING_TIME,
        help="Handling time value to replace. Default: 40",
    )
    parser.add_argument(
        "--to-handling-time",
        type=int,
        default=DEFAULT_TO_HANDLING_TIME,
        help="New handling time value. Default: 28",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="GET /units page size. Default: 100",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=DEFAULT_REQUEST_DELAY_SEC,
        help="Delay between requests per controller in seconds. Default: 0.08",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=8,
        help="Retries for transient HTTP 429/5xx and network errors. Default: 8",
    )
    parser.add_argument(
        "--retry-base-delay",
        type=float,
        default=2.0,
        help="Base retry delay in seconds; doubles each retry. Default: 2.0",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Total aiohttp timeout in seconds. Default: 120",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only scan and log matches; do not PATCH Kaufland units.",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=default_log_file,
        help=f"Dedicated stats log path. Default: {default_log_file}",
    )
    parser.add_argument(
        "--start-at",
        type=parse_start_at,
        help="Skip scopes before this controller/storefront, e.g. jv/cz.",
    )
    return parser


def log_summary(stats: RunStats, logger: logging.Logger) -> None:
    logger.info("overall_statistics_start")
    for key in sorted(stats.scopes):
        scope = stats.scopes[key]
        logger.info(
            "scope_statistics scope=%s fetched=%s matched=%s updated=%s failed=%s dry_run=%s missing_unit_id=%s no_handling_time=%s",
            key,
            scope.fetched,
            scope.matched,
            scope.updated,
            scope.failed,
            scope.skipped_dry_run,
            scope.skipped_missing_unit_id,
            scope.skipped_no_handling_time,
        )

    total = stats.totals()
    logger.info(
        "overall_statistics fetched=%s matched=%s updated=%s failed=%s dry_run=%s missing_unit_id=%s no_handling_time=%s",
        total.fetched,
        total.matched,
        total.updated,
        total.failed,
        total.skipped_dry_run,
        total.skipped_missing_unit_id,
        total.skipped_no_handling_time,
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    logger = configure_logger(args.log_file)

    logger.info(
        "run_start dry_run=%s controllers=%s storefronts=%s from_handling_time=%s to_handling_time=%s start_at=%s max_retries=%s retry_base_delay=%s log_file=%s",
        args.dry_run,
        ",".join(args.controllers),
        ",".join(args.storefronts),
        args.from_handling_time,
        args.to_handling_time,
        args.start_at,
        args.max_retries,
        args.retry_base_delay,
        args.log_file,
    )

    try:
        stats = asyncio.run(run(args, logger))
    except Exception as exc:
        logger.exception("run_failed error=%s", exc)
        return 1

    log_summary(stats, logger)
    return 1 if stats.totals().failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
