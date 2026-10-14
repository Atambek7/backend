import asyncio
import argparse
import csv
import json
import logging
import random
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, Set, List, Dict, Any

import aiohttp

# Add backend directory to sys.path so we can import django/app modules
BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BACKEND_DIR))

import config
from main_api.src.utils.price_utils import parse_price
from main_api.src.utils.ean_utils import normalize_ean, is_valid_ean
from main_api.src.utils.cache_utils import BoundedLRUCache
from main_api.src.utils.text_utils import (
    sanitize_title,
    sanitize_description,
    is_blacklisted_category,
    resolve_fallback_category,
    generate_localized_description,
    CATEGORY_BLACKLIST_PREFIXES,
)
from main_api.src.controller.rest_api_controller import RestApiController
from main_api.services.aftercool_service import AftercoolService, AftercoolProductsQuery
from main_api.services.material_service import (
    resolve_material_attributes,
    parse_item_specifics as _parse_custom_item_specifics,
    extract_explicit_composition as _extract_explicit_composition,
)
from main_api.src.servises.json_mapper import normalize_json_item
from main_api.src.extracter import _normalize_aftercool_html
from main_api.services.catalog_reconciler import (
    CatalogReconciler,
    is_uk_source,
    select_canonical_item,
    is_blacklisted_source,
    is_trash_title,
)

DEFAULT_STOREFRONTS = ["de", "cz", "sk", "pl", "at", "fr", "it", "nl"]
ALLOWED_FACTORIES_FILE = BACKEND_DIR / "allowed_factories.txt"
# Конвенция именования китайских складов в Aftercool (требуют увеличенного handling_time=60 дней)
CN_PATTERNS = ("_CN_", "__CN__", "(CN)")

# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------

def setup_logger(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("reupload")
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

# ---------------------------------------------------------------------------
# Global Rate Limiter (Token Bucket)
# ---------------------------------------------------------------------------

class GlobalRateLimiter:
    """Токен-бакет ограничитель частоты запросов для безопасной работы с API Kaufland."""
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

# ---------------------------------------------------------------------------
# Checkpoints & File Management
# ---------------------------------------------------------------------------

class CheckpointManager:
    """Управление раздельными чекпоинтами для каждого аккаунта с потокобезопасной записью."""
    def __init__(
        self,
        account: str,
        custom_cp: Optional[Path] = None,
        custom_err: Optional[Path] = None,
        multi_account: bool = False
    ):
        self.account = account.lower()
        self.is_custom = custom_cp is not None

        if custom_cp:
            # Защита от смешивания чекпоинтов при одновременном запуске JV + XL
            if multi_account and f"_{self.account}" not in custom_cp.stem:
                self.cp_file = custom_cp.parent / f"{custom_cp.stem}_{self.account}{custom_cp.suffix}"
            else:
                self.cp_file = custom_cp
        else:
            if self.account == "jv":
                jv_file = BACKEND_DIR / "completed_uploads_jv.txt"
                self.cp_file = jv_file if jv_file.exists() else (BACKEND_DIR / "completed_uploads.txt")
            else:
                self.cp_file = BACKEND_DIR / f"completed_uploads_{self.account}.txt"

        if custom_err:
            if multi_account and f"_{self.account}" not in custom_err.stem:
                self.err_file = custom_err.parent / f"{custom_err.stem}_{self.account}{custom_err.suffix}"
            else:
                self.err_file = custom_err
        else:
            self.err_file = BACKEND_DIR / f"upload_errors_{self.account}.txt"

        self.lock = asyncio.Lock()
        self.completed_eans: Set[str] = set()

    @staticmethod
    def _sync_append(path: Path, text: str):
        with open(path, "a", encoding="utf-8") as f:
            f.write(text)

    @staticmethod
    def _sync_load(cp_file: Path, is_custom: bool, account: str) -> Set[str]:
        completed = set()
        if cp_file.exists():
            with open(cp_file, "r", encoding="utf-8") as f:
                completed = set(normalize_ean(line.strip()) for line in f if line.strip())
        # Если это JV и существует старый completed_uploads.txt, подгрузим и его EAN
        if not is_custom and account == "jv":
            old_file = BACKEND_DIR / "completed_uploads.txt"
            if old_file.exists() and old_file != cp_file:
                with open(old_file, "r", encoding="utf-8") as f:
                    completed.update(normalize_ean(line.strip()) for line in f if line.strip())
        return completed

    async def load_completed(self) -> Set[str]:
        self.completed_eans = await asyncio.to_thread(
            self._sync_load, self.cp_file, self.is_custom, self.account
        )
        return self.completed_eans

    async def record_completed(self, ean: str):
        canonical_ean = normalize_ean(ean)
        async with self.lock:
            self.completed_eans.add(canonical_ean)
            try:
                await asyncio.to_thread(self._sync_append, self.cp_file, f"{canonical_ean}\n")
            except (PermissionError, OSError):
                pass

    async def record_error(self, ean: str, country: str, error_msg: str):
        canonical_ean = normalize_ean(ean)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        async with self.lock:
            try:
                line = f"{timestamp} | ean={canonical_ean} | country={country} | {error_msg} | FAILED\n"
                await asyncio.to_thread(self._sync_append, self.err_file, line)
            except (PermissionError, OSError):
                pass

    @staticmethod
    def _sync_remove(path: Path, canonical_ean: str):
        if not path.exists():
            return
        temp_path = path.parent / f"{path.name}.tmp.{time.time_ns()}"
        with open(path, "r", encoding="utf-8", errors="ignore") as f_in, open(temp_path, "w", encoding="utf-8") as f_out:
            for line in f_in:
                if normalize_ean(line.strip()) != canonical_ean:
                    f_out.write(line)
        temp_path.replace(path)

    async def remove_completed(self, ean: str):
        canonical_ean = normalize_ean(ean)
        async with self.lock:
            was_present = canonical_ean in self.completed_eans
            self.completed_eans.discard(canonical_ean)
            if not was_present:
                return  # Fast path: EAN was not in checkpoint, avoid unnecessary disk rewrite
            try:
                await asyncio.to_thread(self._sync_remove, self.cp_file, canonical_ean)
                if not self.is_custom and self.account == "jv":
                    old_file = BACKEND_DIR / "completed_uploads.txt"
                    if old_file.exists() and old_file != self.cp_file:
                        await asyncio.to_thread(self._sync_remove, old_file, canonical_ean)
            except Exception:
                pass

# ---------------------------------------------------------------------------
# Failed / Partial Items Manager (Persistent DLQ for Safe Resume)
# ---------------------------------------------------------------------------

class FailedItemsManager:
    """
    Персистентный менеджер очереди повторной обработки (DLQ) для FAILED и PARTIAL товаров.
    Исключает потерю товаров из-за продвижения offset при --resume:
    1. При возникновении ошибки (FAILED или PARTIAL) товар атомарно сохраняется в .failed_items_{account}.json.
    2. При успешной обработке (SUCCESS, RECONCILED_VALID, PURGED_*) товар удаляется из .failed_items_{account}.json.
    3. При запуске с флагом --resume или --retry-failed все сохранённые проблемные товары
       автоматически загружаются и обрабатываются первыми, до продолжения сканирования каталога.
    4. Файл состояния .reupload_state_{account}.json НЕ удаляется по завершении каталога,
       если в .failed_items_{account}.json ещё остаются неразрешённые товары.
    """
    def __init__(self, account: str):
        self.account = account
        self.file_path = BACKEND_DIR / f".failed_items_{account}.json"
        self._items: Dict[str, dict] = {}
        self._lock = asyncio.Lock()
        self._last_save_time: float = 0.0
        self._dirty_count: int = 0

    async def load(self) -> Dict[str, dict]:
        async with self._lock:
            if self.file_path.exists():
                try:
                    text = await asyncio.to_thread(self.file_path.read_text, encoding="utf-8")
                    self._items = json.loads(text) if text.strip() else {}
                except Exception:
                    self._items = {}
            return dict(self._items)

    async def record_failed(self, ean: str, item: dict, status: str = "FAILED", error_msg: str = ""):
        canonical_key = normalize_ean(ean) or str(item.get("product_id") or "UNKNOWN")
        async with self._lock:
            self._items[canonical_key] = {
                "ean": canonical_key,
                "status": status,
                "error": str(error_msg)[:300],
                "timestamp": datetime.now().isoformat(),
                "item": item
            }
            self._dirty_count += 1
            await self._save_to_disk(force=False)

    def remove_item(self, ean: str):
        canonical_key = normalize_ean(ean)
        if canonical_key in self._items:
            del self._items[canonical_key]
            self._dirty_count += 1

    async def record_resolved(self, ean: str):
        canonical_key = normalize_ean(ean)
        async with self._lock:
            if canonical_key in self._items:
                del self._items[canonical_key]
                self._dirty_count += 1
                await self._save_to_disk(force=False)

    async def flush(self):
        async with self._lock:
            await self._save_to_disk(force=True)

    async def _save_to_disk(self, force: bool = False):
        now = time.monotonic()
        if not force and self._dirty_count < 50 and (now - self._last_save_time) < 10.0:
            return
        try:
            if self._items:
                payload = json.dumps(self._items, ensure_ascii=False)
                await asyncio.to_thread(self.file_path.write_text, payload, encoding="utf-8")
            else:
                if self.file_path.exists():
                    await asyncio.to_thread(self.file_path.unlink, missing_ok=True)
            self._last_save_time = now
            self._dirty_count = 0
        except Exception:
            pass

    def pending_count(self) -> int:
        return len(self._items)

    def get_pending_items(self) -> List[dict]:
        return [entry["item"] for entry in self._items.values() if isinstance(entry, dict) and "item" in entry]

# ---------------------------------------------------------------------------
# In-Flight EAN Deduplication Tracker
# ---------------------------------------------------------------------------

class InFlightTracker:
    """Исключает race condition параллельной обработки одного EAN разными воркерами."""
    def __init__(self):
        self._in_flight: Set[str] = set()
        self._lock = asyncio.Lock()

    async def acquire(self, ean: str) -> bool:
        async with self._lock:
            if ean in self._in_flight:
                return False
            self._in_flight.add(ean)
            return True

    async def release(self, ean: str):
        async with self._lock:
            self._in_flight.discard(ean)

# ---------------------------------------------------------------------------
# Statistics Tracker with Strict Limit Slot Reservation
# ---------------------------------------------------------------------------

@dataclass
class Stats:
    total_fetched: int = 0
    uploaded: int = 0
    partial: int = 0
    already_completed: int = 0
    skipped_factory: int = 0
    skipped_filter: int = 0
    skipped_not_found: int = 0
    skipped_uk: int = 0
    skipped_blacklisted: int = 0
    skipped_trash_title: int = 0
    purged_cross_account: int = 0
    purged_orphan: int = 0
    reconciled_valid: int = 0
    failed: int = 0
    reserved_slots: int = 0
    start_time: float = field(default_factory=time.time)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def try_reserve_slot(self, limit: int, reconcile_only: bool = False) -> bool:
        """Атомарно бронирует слот для строгого соблюдения --limit при многопоточности."""
        if limit <= 0:
            return True
        async with self._lock:
            current = (self.reconciled_valid + self.purged_cross_account + self.purged_orphan) if reconcile_only else self.uploaded
            if (current + self.reserved_slots) >= limit:
                return False
            self.reserved_slots += 1
            return True

    async def release_reserved_slot(self):
        async with self._lock:
            if self.reserved_slots > 0:
                self.reserved_slots -= 1

    async def inc(self, status: str, had_reserved_slot: bool = False):
        async with self._lock:
            if had_reserved_slot and self.reserved_slots > 0:
                self.reserved_slots -= 1

            if status == "SUCCESS":
                self.uploaded += 1
            elif status == "PARTIAL":
                self.partial += 1
            elif status == "SKIPPED_COMPLETED":
                self.already_completed += 1
            elif status == "SKIPPED_FACTORY":
                self.skipped_factory += 1
            elif status == "SKIPPED_EAN_FILTER":
                self.skipped_filter += 1
            elif status == "SKIPPED_NOT_IN_AFTERCOOL":
                self.skipped_not_found += 1
            elif status == "SKIPPED_UK_SOURCE":
                self.skipped_uk += 1
            elif status in ("SKIPPED_BLACKLISTED_FILE", "SKIPPED_BLACKLISTED"):
                self.skipped_blacklisted += 1
            elif status == "SKIPPED_TRASH_TITLE":
                self.skipped_trash_title += 1
            elif status == "PURGED_CROSS_ACCOUNT":
                self.purged_cross_account += 1
            elif status == "PURGED_ORPHAN":
                self.purged_orphan += 1
            elif status == "RECONCILED_VALID":
                self.reconciled_valid += 1
            elif status == "FAILED":
                self.failed += 1

    async def add_fetched(self, count: int):
        async with self._lock:
            self.total_fetched += count

    def summary_line(self, account: str) -> str:
        elapsed = max(1.0, time.time() - self.start_time)
        speed = self.uploaded / (elapsed / 60.0)  # uploads per minute
        filter_str = f", skipped_filter={self.skipped_filter:,}" if self.skipped_filter > 0 else ""
        not_found_str = f", not_found={self.skipped_not_found:,}" if self.skipped_not_found > 0 else ""
        purged_str = f", purged_cross={self.purged_cross_account:,}" if self.purged_cross_account > 0 else ""
        orphan_str = f", purged_orphans={self.purged_orphan:,}" if self.purged_orphan > 0 else ""
        reconciled_str = f", reconciled={self.reconciled_valid:,}" if self.reconciled_valid > 0 else ""
        trash_str = f", skipped_trash={self.skipped_blacklisted + self.skipped_trash_title:,}" if (self.skipped_blacklisted + self.skipped_trash_title) > 0 else ""
        return (
            f"[{account}] Progress: fetched={self.total_fetched:,}, "
            f"uploaded={self.uploaded:,}, "
            f"partial={self.partial:,}, "
            f"already_completed={self.already_completed:,}, "
            f"skipped_factory={self.skipped_factory:,}"
            f"{trash_str}"
            f"{purged_str}{orphan_str}{reconciled_str}"
            f"{filter_str}{not_found_str}, "
            f"failed={self.failed:,} | {speed:.1f} uploads/min"
        )

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def _normalize_name(s: str) -> str:
    s = re.sub(r"\.(csv|xlsx)$", "", str(s or ""), flags=re.I)
    return re.sub(r"[^a-zA-Z0-9]", "", s).lower()

def load_allowed_factories() -> Set[str]:
    if not ALLOWED_FACTORIES_FILE.exists():
        return set()
    with open(ALLOWED_FACTORIES_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def is_factory_allowed(source_file: str, allowed_factories: Set[str], allow_all: bool = False) -> bool:
    if not source_file:
        return False
    # ЖЕСТКИЙ БЛЭКЛИСТ: файлы удалений, служебные дампы и UK-склады ВСЕГДА блокируются
    if is_blacklisted_source(source_file) or is_uk_source(source_file):
        return False
    if allow_all:
        return True
    if not allowed_factories:
        return False
    # Разбиваем имя исходного файла на смысловые токены
    sf_base = re.sub(r"\.(csv|xlsx)$", "", str(source_file or ""), flags=re.I)
    sf_tokens = set(t.lower() for t in re.split(r'[^a-zA-Z0-9]+', sf_base) if t)
    sf_norm = _normalize_name(source_file)

    for factory_raw in allowed_factories:
        f_norm = _normalize_name(factory_raw)
        # 1. Прямое совпадение нормализованных строк
        if f_norm == sf_norm:
            return True
        # 2. Точное сопоставление по токенам (исключает ложное 'lux' в 'deluxe')
        f_base = re.sub(r"\.(csv|xlsx)$", "", str(factory_raw or ""), flags=re.I)
        f_tokens = set(t.lower() for t in re.split(r'[^a-zA-Z0-9]+', f_base) if t)
        if f_tokens and f_tokens.issubset(sf_tokens):
            return True
    return False

def get_handling_time(source_file: str) -> int:
    source_file_upper = (source_file or "").upper()
    if any(p in source_file_upper for p in CN_PATTERNS):
        return 60
    return 30

def extract_status_code(exc: Exception) -> Optional[int]:
    """Извлечение реального HTTP status code из исключения."""
    if hasattr(exc, "status_code") and isinstance(exc.status_code, int):
        return exc.status_code
    if hasattr(exc, "status") and isinstance(exc.status, int):
        return exc.status
    m = re.search(r"\b([45]\d\d)\b", str(exc))
    if m:
        return int(m.group(1))
    return None

async def execute_with_retry(
    func,
    rate_limiter: Optional[GlobalRateLimiter] = None,
    max_retries: int = 5,
    initial_delay: float = 2.0
):
    """Вызов функции с ограничением скорости, джиттером и экспоненциальным backoff."""
    delay = initial_delay
    last_exc = None
    for attempt in range(max_retries):
        if rate_limiter:
            await rate_limiter.acquire()
        try:
            return await func()
        except Exception as e:
            last_exc = e
            status = extract_status_code(e)
            err_str = str(e).lower()
            # Повторяем только при 429, 500, 502, 503, 504 или таймаутах сети
            is_retryable = (status in {429, 500, 502, 503, 504}) or any(
                code in err_str for code in ["timeout", "connection reset", "broken pipe"]
            )
            if is_retryable and attempt < max_retries - 1:
                jitter = random.uniform(0.2, 1.0)
                wait_sec = (delay * (1.5 ** attempt)) + jitter
                if status == 429:
                    wait_sec = max(wait_sec, 5.0)
                await asyncio.sleep(wait_sec)
                continue
            raise e
    raise last_exc

# ---------------------------------------------------------------------------
# Category Resolution & LRU Cache
# ---------------------------------------------------------------------------

CATEGORY_CACHE = BoundedLRUCache(maxsize=3000, ttl_sec=86400)
CATEGORY_LOCK = asyncio.Lock()
CATEGORY_IN_FLIGHT: Dict[str, asyncio.Future] = {}

def _get_category_cache_key(title: str, manufacturer: str = "", description: str = "") -> str:
    norm_title = " ".join(re.findall(r"[a-zA-ZäöüßÄÖÜ0-9]+", (title or "").lower()))
    norm_mfr = (manufacturer or "").strip().lower()
    norm_desc = (description or "").strip().lower()[:80]
    return f"{norm_title}|{norm_mfr}|{norm_desc}"

SHIPPING_GROUP_CACHE: Dict[Tuple[str, str], Optional[int]] = {}
SHIPPING_GROUP_LOCK = asyncio.Lock()

async def get_default_shipping_group(
    rac: RestApiController,
    rate_limiter: GlobalRateLimiter,
    sf: str,
    cache: Dict[Tuple[str, str], Optional[int]],
    account: str = "",
    logger: Optional[logging.Logger] = None,
) -> Optional[int]:
    """
    Получение id_shipping_group по умолчанию для заданной витрины и аккаунта с потокобезопасным кешированием.
    Ключ кеша (account, sf) изолирует группы между разными аккаунтами продавца (JV и XL).
    """
    cache_key = (account or getattr(rac, "controller", ""), sf)
    if cache_key in cache:
        return cache[cache_key]

    async with SHIPPING_GROUP_LOCK:
        if cache_key in cache:
            return cache[cache_key]
        try:
            res = await execute_with_retry(
                lambda: rac.send_request("GET", f"/shipping-groups?storefront={sf}"),
                rate_limiter=rate_limiter,
                max_retries=3
            )
            groups = res.get("data", []) if isinstance(res, dict) else (res if isinstance(res, list) else [])
            default_id = next(
                (g.get("id_shipping_group") for g in groups if isinstance(g, dict) and g.get("is_default") and g.get("id_shipping_group")),
                None
            )
            if not default_id and groups:
                first = groups[0]
                if isinstance(first, dict):
                    default_id = first.get("id_shipping_group")
            if default_id is not None:
                cache[cache_key] = default_id
            if logger and default_id:
                logger.info(f"[{account}] Resolved default shipping group for storefront '{sf}': {default_id}")
            return default_id
        except Exception as e:
            if logger:
                logger.warning(f"[{account}] Failed to fetch shipping groups for storefront '{sf}': {e}")
            # Не кешируем None при исключении сети, чтобы последующие товары могли повторить попытку
            return None

async def get_or_decide_category(
    rac: RestApiController,
    rate_limiter: GlobalRateLimiter,
    title: str,
    description: str,
    manufacturer: str,
    price: float,
    allow_fallback: bool = False,
    account: str = "",
    ean: str = "",
    logger: Optional[logging.Logger] = None,
    category_storefront: str = "de"
) -> Tuple[Optional[int], Optional[str]]:
    """
    Определение категории через Kaufland API.
    Примечание: Kaufland использует единую глобальную таксономию категорий (id_category)
    для всех витрин, поэтому запрос к витрине de с локалью de-DE является эталонным стандартом.
    """
    cache_key = _get_category_cache_key(title, manufacturer=manufacturer, description=description)
    wait_future: Optional[asyncio.Future] = None
    is_leader = False
    if cache_key:
        async with CATEGORY_LOCK:
            cached = CATEGORY_CACHE.get(cache_key)
            if cached:
                return cached
            # In-flight coalescing: если другой воркер уже выполняет этот же запрос, ждем его
            if cache_key in CATEGORY_IN_FLIGHT:
                wait_future = CATEGORY_IN_FLIGHT[cache_key]
            else:
                loop = asyncio.get_running_loop()
                wait_future = loop.create_future()
                CATEGORY_IN_FLIGHT[cache_key] = wait_future
                is_leader = True

    if not is_leader and wait_future is not None:
        try:
            return await asyncio.shield(wait_future)
        except Exception:
            if allow_fallback:
                return resolve_fallback_category(title)
            return (None, None)

    result_pair: Tuple[Optional[int], Optional[str]] = (None, None)
    try:
        price_cents = max(100, int(round(price * 100)))
        payload = {
            "item": {
                "title": (title or "Möbel")[:255],
                "description": (description or title or "Möbel")[:800],
                "manufacturer": manufacturer or "Möbel"
            },
            "price": price_cents
        }
        res = await execute_with_retry(
            lambda: rac.send_request("POST", f"/categories/decide?storefront={category_storefront}&locale=de-DE", json=payload),
            rate_limiter=rate_limiter,
            max_retries=3
        )
        data = res.get("data")
        if data and len(data) > 0:
            cat_id = data[0].get("id_category")
            cat_name = data[0].get("name")

            # Проверка по жесткому черному списку немебельных категорий
            if cat_id and cat_name and is_blacklisted_category(cat_name, title=title):
                if allow_fallback:
                    fallback_id, fallback_name = resolve_fallback_category(title)
                    if logger:
                        logger.warning(
                            f"[{account}][{ean}] Category '{cat_name}' (id={cat_id}) is blacklisted. Resolved fallback category {fallback_id} ('{fallback_name}') based on title."
                        )
                    result_pair = (fallback_id, fallback_name)
                else:
                    if logger:
                        logger.warning(
                            f"[{account}][{ean}] Category '{cat_name}' (id={cat_id}) is blacklisted and fallback is disabled."
                        )
                    result_pair = (None, None)
            elif cat_id and cat_name:
                result_pair = (cat_id, cat_name)
    except Exception as e:
        if logger:
            logger.warning(f"[{account}][{ean}] /categories/decide failed: {e}")
        if allow_fallback:
            result_pair = resolve_fallback_category(title)
    finally:
        if is_leader and cache_key:
            async with CATEGORY_LOCK:
                if result_pair != (None, None):
                    CATEGORY_CACHE.set(cache_key, result_pair)
                inflight_fut = CATEGORY_IN_FLIGHT.pop(cache_key, None)
                if inflight_fut and not inflight_fut.done():
                    inflight_fut.set_result(result_pair)

    if result_pair != (None, None):
        return result_pair

    if allow_fallback:
        fallback_id, fallback_name = resolve_fallback_category(title)
        if logger:
            logger.warning(f"[{account}][{ean}] Using fallback category {fallback_id} ('{fallback_name}') [fallback explicitly allowed]")
        return fallback_id, fallback_name
    
    if logger:
        logger.error(f"[{account}][{ean}] Category could not be decided and fallback is disabled (--allow-cat-fallback=False)")
    return None, None

# ---------------------------------------------------------------------------
PURGE_LOCALES = ("de-DE", "cs-CZ", "sk-SK", "de-AT", "pl-PL", "fr-FR", "it-IT", "nl-NL")

async def purge_ean_from_account(
    ean: str,
    account: str,
    rac: RestApiController,
    rate_limiter: GlobalRateLimiter,
    cp_mgr: CheckpointManager,
    logger: logging.Logger,
    dry_run: bool = False,
    reason: str = "cross_account",
) -> Dict[str, Any]:
    """
    Полное удаление товара с аккаунта-нарушителя:
    1. Поиск и удаление офферов (юнитов) на всех витринах аккаунта.
    2. Удаление локализованных данных карточки товара (product-data).
    3. Исключение EAN из чекпоинта completed_uploads_{account}.txt ТОЛЬКО при 100% успехе.
    4. Запись в лог очистки.
    """
    canonical_ean = normalize_ean(ean)
    storefronts = config.get_account_storefronts(account)
    deleted_units = []
    errors = []

    logger.info(f"[{account}][{canonical_ean}] START PURGE (reason={reason}, dry_run={dry_run})")

    # 1. Поиск и удаление юнитов на каждой витрине
    for sf in storefronts:
        try:
            get_resp = await execute_with_retry(
                lambda: rac.send_request("GET", f"/units?storefront={sf}&id_offer={canonical_ean}"),
                rate_limiter=rate_limiter,
                max_retries=3
            )
            units = get_resp.get("data", []) if isinstance(get_resp, dict) else (get_resp if isinstance(get_resp, list) else [])
            if not units:
                get_resp2 = await execute_with_retry(
                    lambda: rac.send_request("GET", f"/units?storefront={sf}&ean={canonical_ean}"),
                    rate_limiter=rate_limiter,
                    max_retries=3
                )
                units = get_resp2.get("data", []) if isinstance(get_resp2, dict) else (get_resp2 if isinstance(get_resp2, list) else [])

            for u in units:
                if isinstance(u, dict):
                    unit_id = u.get("id_unit")
                    if unit_id:
                        if dry_run:
                            logger.info(f"[{account}][{canonical_ean}][{sf}] DRY-RUN: Would DELETE unit {unit_id}")
                            deleted_units.append(f"{sf}:{unit_id}")
                        else:
                            try:
                                await execute_with_retry(
                                    lambda: rac.send_request("DELETE", f"/units/{unit_id}?storefront={sf}"),
                                    rate_limiter=rate_limiter,
                                    max_retries=3
                                )
                                deleted_units.append(f"{sf}:{unit_id}")
                                logger.info(f"[{account}][{canonical_ean}][{sf}] DELETED unit {unit_id}")
                            except Exception as del_err:
                                status = extract_status_code(del_err)
                                if status == 404:
                                    deleted_units.append(f"{sf}:{unit_id}")
                                else:
                                    err_msg = f"Failed to delete unit {unit_id} on {sf}: {del_err}"
                                    logger.warning(f"[{account}][{canonical_ean}][{sf}] {err_msg}")
                                    errors.append(err_msg)
        except Exception as e:
            err_msg = f"Unit lookup failed on {sf}: {e}"
            logger.warning(f"[{account}][{canonical_ean}][{sf}] {err_msg}")
            errors.append(err_msg)

    # 2. Удаление product-data по локалям
    for loc in PURGE_LOCALES:
        if dry_run:
            logger.debug(f"[{account}][{canonical_ean}][{loc}] DRY-RUN: Would DELETE product-data")
        else:
            try:
                await execute_with_retry(
                    lambda: rac.send_request("DELETE", f"/product-data/{canonical_ean}?locale={loc}"),
                    rate_limiter=rate_limiter,
                    max_retries=3
                )
                logger.debug(f"[{account}][{canonical_ean}][{loc}] DELETED product-data")
            except Exception as pd_err:
                status = extract_status_code(pd_err)
                err_str = str(pd_err).lower()
                # 404: Карточка уже отсутствует в каталоге Kaufland (успешно)
                if status == 404:
                    pass
                else:
                    # 400: Допустимо ТОЛЬКО если витрина/локаль не сконфигурирована для аккаунта
                    is_unsupported_locale = (
                        status == 400 and (
                            "locale" in err_str or "storefront" in err_str
                        ) and any(kw in err_str for kw in (
                            "not configured", "not supported", "not active",
                            "not available", "unknown locale", "invalid locale"
                        ))
                    )
                    if is_unsupported_locale:
                        logger.debug(f"[{account}][{canonical_ean}][{loc}] Locale not configured for account (400), skipping")
                    else:
                        err_msg = f"Failed to delete product-data for locale {loc} (status={status}): {pd_err}"
                        logger.warning(f"[{account}][{canonical_ean}][{loc}] {err_msg}")
                        errors.append(err_msg)

    is_success = (len(errors) == 0)

    # 3. Удаление из чекпоинта ТОЛЬКО при полном успехе без ошибок
    if not dry_run:
        if is_success:
            await cp_mgr.remove_completed(canonical_ean)
        else:
            await cp_mgr.record_error(
                canonical_ean, "PURGE", f"Purge {reason} incomplete ({len(errors)} errors): {'; '.join(errors[:3])}"
            )
            logger.error(
                f"[{account}][{canonical_ean}] PURGE INCOMPLETE: {len(errors)} errors encountered. Checkpoint NOT removed to allow retry."
            )

    # 4. Запись в лог очистки
    log_name = f"purged_cross_account_{account}.txt" if reason == "cross_account" else f"purged_orphans_{account}.txt"
    log_file = BACKEND_DIR / log_name
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mode_tag = "DRY-RUN" if dry_run else ("EXECUTED" if is_success else "PARTIAL_ERROR")
    err_suffix = f" | errors={len(errors)}" if errors else ""
    log_line = f"{ts} | ean={canonical_ean} | account={account} | units={len(deleted_units)} ({','.join(deleted_units)}) | mode={mode_tag} | reason={reason}{err_suffix}\n"
    try:
        await asyncio.to_thread(CheckpointManager._sync_append, log_file, log_line)
    except Exception:
        pass

    if is_success:
        logger.info(f"[{account}][{canonical_ean}] PURGE FINISHED: units_deleted={len(deleted_units)}, reason={reason}")
    else:
        logger.warning(f"[{account}][{canonical_ean}] PURGE FINISHED WITH ERRORS: units_deleted={len(deleted_units)}, errors={len(errors)}")

    return {
        "ean": canonical_ean,
        "deleted_units": deleted_units,
        "dry_run": dry_run,
        "success": is_success,
        "errors": errors,
    }


async def process_item(
    item: dict,
    account: str,
    storefronts: List[str],
    rac: RestApiController,
    rate_limiter: GlobalRateLimiter,
    cp_mgr: CheckpointManager,
    allowed_factories: Set[str],
    logger: logging.Logger,
    dry_run: bool,
    allow_cat_fallback: bool,
    update_product_data: bool = False,
    eans_whitelist: Optional[Set[str]] = None,
    allow_all_factories: bool = False,
    reconciler: Optional[CatalogReconciler] = None,
    reconcile_only: bool = False,
    purge_orphans: bool = False,
    aftercool: Optional[AftercoolService] = None,
    session_holder: Optional[List[str]] = None,
    stamm_cache: Optional[BoundedLRUCache] = None,
) -> str:
    """
    Обработка одного товара с валидацией принадлежности к аккаунту и защитой от UK-складов.
    Возвращает статус:
    - 'SUCCESS', 'PARTIAL', 'SKIPPED_COMPLETED', 'SKIPPED_FACTORY', 'SKIPPED_UK_SOURCE',
    - 'PURGED_CROSS_ACCOUNT', 'PURGED_ORPHAN', 'SKIPPED_ORPHAN', 'RECONCILED_VALID', 'FAILED'.
    """
    if not storefronts:
        logger.error(f"[{account}] No storefronts provided to process_item")
        return "FAILED"

    raw_ean = AftercoolService._extract_ean(item) or ""
    ean = normalize_ean(raw_ean)
    if not ean or not is_valid_ean(ean):
        logger.error(
            f"[{account}] Item missing valid EAN: {item.get('product_id')} (raw='{raw_ean}', normalized='{ean}')"
        )
        report_ean = ean or raw_ean or str(item.get("product_id") or "UNKNOWN")
        await cp_mgr.record_error(report_ean, "ALL", f"Invalid EAN (raw='{raw_ean}', normalized='{ean}')")
        return "FAILED"

    # 0. Фильтр по белому списку EAN (для точечных тестовых прогонов)
    if eans_whitelist and ean not in eans_whitelist:
        return "SKIPPED_EAN_FILTER"

    # 1. Валидация принадлежности к аккаунту через CatalogReconciler
    if reconciler:
        if not reconciler.is_initialized:
            logger.error(
                f"[{account}][{ean}] CatalogReconciler is not initialized! Halting item validation (fail-closed)."
            )
            return "FAILED"
        target_account, ownership_status = reconciler.resolve_target_account(ean)
        if target_account != account:
            if target_account is not None:
                # Товар принадлежит ДРУГОМУ аккаунту (например, XL вместо JV)!
                logger.warning(
                    f"[{account}][{ean}] CROSS-ACCOUNT COLLISION: Item belongs to '{target_account.upper()}' (status={ownership_status}), "
                    f"but is currently on '{account.upper()}'. Triggering PURGE from '{account.upper()}'."
                )
                purge_res = await purge_ean_from_account(
                    ean=ean,
                    account=account,
                    rac=rac,
                    rate_limiter=rate_limiter,
                    cp_mgr=cp_mgr,
                    logger=logger,
                    dry_run=dry_run,
                    reason="cross_account",
                )
                if purge_res.get("success"):
                    return "PURGED_CROSS_ACCOUNT"
                else:
                    return "FAILED"
            else:
                # Товар отсутствует в Aftercool (ORPHAN)
                if purge_orphans:
                    logger.warning(
                        f"[{account}][{ean}] ORPHAN DETECTED: Item not in Aftercool. Purging from '{account.upper()}'."
                    )
                    purge_res = await purge_ean_from_account(
                        ean=ean,
                        account=account,
                        rac=rac,
                        rate_limiter=rate_limiter,
                        cp_mgr=cp_mgr,
                        logger=logger,
                        dry_run=dry_run,
                        reason="orphan",
                    )
                    if purge_res.get("success"):
                        return "PURGED_ORPHAN"
                    else:
                        return "FAILED"
                else:
                    logger.warning(
                        f"[{account}][{ean}] ORPHAN: Item not in Aftercool. (Pass --purge-orphans to delete from Kaufland)."
                    )
                    return "SKIPPED_ORPHAN"

    # 2. Исключение файлов удалений, мусора и британских складов (UK warehouses)
    source_file = str(item.get("source_file") or "").strip()
    if is_blacklisted_source(source_file):
        logger.warning(f"[{account}][{ean}] BLACKLISTED source file detected ({source_file}). Skipping trash item.")
        return "SKIPPED_BLACKLISTED_FILE"

    if is_uk_source(source_file):
        logger.warning(f"[{account}][{ean}] UK source file detected ({source_file}). Skipping UK warehouse item.")
        return "SKIPPED_UK_SOURCE"

    # 3. Если запущен режим reconcile-only, карточка валидна и проверена
    if reconcile_only:
        if not reconciler or not reconciler.is_initialized:
            logger.error(f"[{account}][{ean}] Cannot verify ownership in reconcile_only mode: reconciler is not initialized!")
            return "FAILED"
        logger.info(f"[{account}][{ean}] RECONCILE-ONLY: Ownership verified for '{account.upper()}'. Skipping product-data upload.")
        return "RECONCILED_VALID"

    # 4. Проверка чекпоинта:
    if cp_mgr.is_custom and ean in cp_mgr.completed_eans:
        return "SKIPPED_COMPLETED"
    if not (eans_whitelist and ean in eans_whitelist):
        if (not update_product_data or cp_mgr.is_custom) and ean in cp_mgr.completed_eans:
            return "SKIPPED_COMPLETED"

    # 5. Проверка белого списка фабрик (fail-closed по умолчанию)
    if not is_factory_allowed(source_file, allowed_factories, allow_all=allow_all_factories):
        return "SKIPPED_FACTORY"

    handling_time = get_handling_time(source_file)

    raw_row = item.get("row") or {}
    if not isinstance(raw_row, dict):
        raw_row = {}
    row = normalize_json_item(raw_row) if raw_row else {}

    title = str(item.get("name") or "").strip()
    if not title:
        desc_raw = str(row.get("Artikelbeschreibung") or row.get("TranslatedDescription") or "").strip()
        desc_clean = re.sub(r'<[^>]+>', ' ', desc_raw)
        desc_clean = " ".join(desc_clean.split())
        if desc_clean:
            title = desc_clean[:150]
            
    title = title[:255].strip()
    title = sanitize_title(title, material=str(row.get("Material") or ""))
    description = str(row.get("Artikelbeschreibung") or title).strip()

    raw_name = str(item.get("name") or "").strip()
    raw_desc = str(row.get("Artikelbeschreibung") or "").strip()
    if is_trash_title(title, raw_name=raw_name, desc=raw_desc):
        logger.warning(f"[{account}][{ean}] TRASH TITLE detected ('{title}' / raw='{raw_name}'). Skipping item.")
        return "SKIPPED_TRASH_TITLE"

    price_str = row.get("price") or row.get("Startpreis") or raw_row.get("Startpreis") or item.get("price") or "0"
    price = parse_price(price_str)
    if not update_product_data and price <= 0:
        logger.error(f"[{account}][{ean}] Invalid price '{price_str}' (parsed: {price})")
        await cp_mgr.record_error(ean, "ALL", f"Invalid price: {price_str}")
        return "FAILED"

    pics = []
    if row.get("PictureURL"):
        pics.append(str(row.get("PictureURL")).strip())
    if row.get("pictureurls"):
        sep = "|" if "|" in str(row.get("pictureurls")) else ";"
        extras = str(row.get("pictureurls")).split(sep)
        pics.extend([x.strip() for x in extras if x.strip()])

    # Fallback на поля уровня item, если в row нет изображений
    if not pics:
        for img_key in ("images", "picture", "image", "GalleryURL", "PictureURL"):
            val = item.get(img_key)
            if val:
                if isinstance(val, list):
                    pics.extend([str(v).strip() for v in val if str(v).strip()])
                elif isinstance(val, str) and val.strip():
                    pics.append(val.strip())
                if pics:
                    break

    # Дедупликация с сохранением порядка и нормализация http -> https
    pics = list(dict.fromkeys(pics))
    pics = [p.replace("http://", "https://", 1) if p.startswith("http://") else p for p in pics]

    size = str(row.get("size") or "").strip()
    color = str(row.get("color") or "").strip()

    # Точный поиск stock без ложного срабатывания falsy при stock=0
    stock_val = None
    stock_keys = ("Menge", "menge", "Bestand", "bestand", "stock", "Stock", "amount", "Amount")
    for k in stock_keys:
        if isinstance(row, dict) and k in row and row[k] is not None and str(row[k]).strip() != "":
            stock_val = row[k]
            break
        elif isinstance(item, dict) and k in item and item[k] is not None and str(item[k]).strip() != "":
            stock_val = item[k]
            break

    unit_amount = 20
    if stock_val is not None:
        try:
            parsed_val = int(round(float(str(stock_val).strip().replace(",", "."))))
            if parsed_val >= 0:
                unit_amount = parsed_val
            else:
                logger.warning(f"[{account}][{ean}] Negative stock {parsed_val}, clamping to 0")
                unit_amount = 0
        except (ValueError, TypeError):
            unit_amount = 20

    # Юридические данные и производитель
    manufacturer = str(item.get("manufacturer") or row.get("Hersteller") or row.get("Manufacturer") or "").strip()
    if not manufacturer:
        if account == "jv":
            manufacturer = getattr(config, "JV_MANUFACTURER", "AEA GmbH & Co. KG") or "AEA GmbH & Co. KG"
        else:
            manufacturer = getattr(config, "XL_MANUFACTURER", "XL MOEBEL GmbH") or "XL MOEBEL GmbH"
            
    contact_data = getattr(config, f"{account.upper()}_CONTACT_DATA", {})

    cat_id, cat_name = await get_or_decide_category(
        rac=rac,
        rate_limiter=rate_limiter,
        title=title,
        description=description,
        manufacturer=manufacturer,
        price=price,
        allow_fallback=allow_cat_fallback,
        account=account,
        ean=ean,
        logger=logger
    )
    if not cat_name:
        logger.error(f"[{account}][{ean}] Category resolution failed and fallback disabled")
        await cp_mgr.record_error(ean, "de-DE", "Category resolution failed")
        return "FAILED"

    if not title:
        title = f"{cat_name} {manufacturer} {ean}"[:255].strip()

    # Корректное определение материала и состава ткани (material_composition)
    mat_res = resolve_material_attributes(
        row=row,
        title=title,
        description=description,
        cat_name=cat_name
    )
    norm_material, material_composition, parts_animal = mat_res
    raw_materials = getattr(mat_res, "materials", [norm_material] if norm_material else [])

    # Повторная точная санитаризация заголовка с учетом определенного материала (устраняет Leder Textil)
    title = sanitize_title(title, material=norm_material)
    if not title:
        title = f"{cat_name} {manufacturer} {ean}"[:255].strip()
        title = sanitize_title(title, material=norm_material)

    # Извлечение полноценного описания товара (Beschreibung из мастер-статьи или фида)
    raw_beschreibung = str(row.get("Beschreibung") or "").strip()
    stamm_id = str(row.get("I_stammartikel") or item.get("I_stammartikel") or "").strip()

    if not raw_beschreibung and stamm_id and aftercool and session_holder:
        if stamm_cache is not None and stamm_id in stamm_cache:
            raw_beschreibung = str(stamm_cache.get(stamm_id) or "")
        else:
            try:
                raw_beschreibung = await aftercool.get_stammartikel_beschreibung(session_holder[0], stamm_id)
                if stamm_cache is not None and raw_beschreibung:
                    stamm_cache.set(stamm_id, raw_beschreibung)
            except Exception as stamm_err:
                logger.debug(f"[{account}][{ean}] Failed to fetch stammartikel {stamm_id}: {stamm_err}")

    if raw_beschreibung:
        norm_desc = _normalize_aftercool_html(raw_beschreibung)
        plain_len = len(re.sub(r"<[^>]+>", "", norm_desc).strip())
        if plain_len >= 80:
            desc_source = norm_desc
        else:
            desc_source = str(row.get("Artikelbeschreibung") or "").strip() or title
    else:
        desc_source = str(row.get("Artikelbeschreibung") or "").strip() or title

    clean_description = sanitize_description(desc_source, material=norm_material)
    if not bool(re.search(r"<strong>(?:Farbe|Ma[ßs]e|Produktbeschreibung|Produktmerkmale):", clean_description, re.I)):
        if title and title.lower() not in clean_description.lower()[:200]:
            clean_description = f"<p><strong>{title}</strong></p>\n{clean_description}"

    if is_trash_title(title, raw_name=raw_name, desc=clean_description):
        logger.warning(f"[{account}][{ean}] Final title is TRASH ('{title}'). Skipping item.")
        return "SKIPPED_TRASH_TITLE"

    # Для натуральной кожи Kaufland требует 'Echtleder' в атрибуте material
    kaufland_material = "Echtleder" if norm_material == "Leder" else norm_material
    kaufland_materials = [
        "Echtleder" if m == "Leder" else m
        for m in dict.fromkeys(raw_materials)
        if m
    ]

    attributes: Dict[str, Any] = {
        "ean": [ean],
        "title": [title],
        "description": [clean_description],
        "manufacturer": [manufacturer],
        "product_safety_contact": contact_data,
    }
    if pics:
        attributes["picture"] = pics
    if cat_name:
        attributes["category"] = [cat_name]
    if cat_id and cat_name:
        attributes["category_detail"] = {
            "id": cat_id,
            "title": cat_name,
            "name": cat_name,
        }
    color = str(row.get("Farbe") or row.get("color") or "").strip()
    width = str(row.get("Breite") or "").strip()
    height = str(row.get("Höhe") or "").strip()
    depth = str(row.get("Sitztiefe") or row.get("Tiefe") or "").strip()
    length = str(row.get("Länge") or "").strip()
    dims = str(row.get("Maße") or "").strip()
    mpn = str(row.get("Herstellernummer") or row.get("MPN") or "").strip()

    if size:
        attributes["size"] = [size]
    if color:
        attributes["colour"] = [color]
    if width:
        attributes["width"] = [width if any(width.endswith(u) for u in ("cm", "m", "mm")) else f"{width} cm"]
    if height:
        attributes["height"] = [height if any(height.endswith(u) for u in ("cm", "m", "mm")) else f"{height} cm"]
    if depth:
        attributes["depth"] = [depth if any(depth.endswith(u) for u in ("cm", "m", "mm")) else f"{depth} cm"]
    if length:
        attributes["length"] = [length if any(length.endswith(u) for u in ("cm", "m", "mm")) else f"{length} cm"]
    if dims:
        attributes["product_dimensions"] = [dims]
    if mpn:
        attributes["mpn"] = [mpn]
    if kaufland_materials:
        attributes["material"] = kaufland_materials
    elif kaufland_material:
        attributes["material"] = [kaufland_material]
    if material_composition:
        attributes["material_composition"] = [material_composition]
    if parts_animal:
        attributes["parts_of_animal_origin"] = [parts_animal]

    payload = {"ean": [ean], "attributes": attributes}

    if dry_run:
        logger.info(f"[{account}][{ean}] DRY-RUN: OK (title={repr(title[:40])}, cat={cat_name}, mat={kaufland_materials}, comp={material_composition})")
        return "SUCCESS"

    STOREFRONT_TO_LOCALE = {
        "de": "de-DE",
        "at": "de-AT",
        "cz": "cs-CZ",
        "sk": "sk-SK",
        "pl": "pl-PL",
        "fr": "fr-FR",
        "it": "it-IT",
        "nl": "nl-NL",
    }
    
    # Собираем список уникальных требуемых локалей для всех активных витрин
    required_locales = list(set(STOREFRONT_TO_LOCALE.get(sf, "de-DE") for sf in storefronts))
    
    # Шаг 1: PUT /product-data для всех необходимых локалей
    successful_locales = set()
    failed_locales = []
    
    for locale in required_locales:
        locale_attrs = attributes.copy()
        
        # Локализация описания (и тайтла, если добавим)
        localized_desc = generate_localized_description(
            title=title, 
            locale=locale, 
            base_description=clean_description, 
            material=norm_material
        )
        locale_attrs["description"] = [localized_desc]
        
        locale_payload = {"ean": [ean], "attributes": locale_attrs}
        
        try:
            await execute_with_retry(
                lambda loc=locale, p=locale_payload: rac.send_request("PUT", f"/product-data?locale={loc}", json=p),
                rate_limiter=rate_limiter,
                max_retries=5
            )
            successful_locales.add(locale)
        except Exception as e:
            logger.error(f"[{account}][{ean}] PUT product-data error for {locale}: {e}")
            await cp_mgr.record_error(ean, locale, f"PUT product-data failed: {e}")
            failed_locales.append(locale)
            
    if failed_locales:
        logger.error(f"[{account}][{ean}] Failed to upload product-data for locales: {failed_locales}")
        return "FAILED"

    if update_product_data:
        await cp_mgr.record_completed(ean)
        logger.info(
            f"[{account}][{ean}] UPDATED product-data for locales {list(successful_locales)}: title={repr(title[:30])} | mat={norm_material} | comp={material_composition}"
        )
        return "SUCCESS"

    # Шаг 2: POST /units параллельно для всех целевых витрин
    async def upload_single_storefront(sf: str):
        price_payload = rac.make_price_payload(storefront=sf, price=price)
        listing_price = price_payload["listing_price"]
        unit_data = {
            "amount": unit_amount,
            "handling_time": handling_time,
            "listing_price": listing_price,
            "minimum_price": listing_price,
            "ean": ean,
            "id_offer": ean,
            "condition": "NEW",
        }
        sg_id = await get_default_shipping_group(rac, rate_limiter, sf, SHIPPING_GROUP_CACHE, account=account, logger=logger)
        if sg_id:
            unit_data["id_shipping_group"] = sg_id

        # Ретраи на случай асинхронной задержки индексации карточки товара в каталоге Kaufland
        item_indexing_retries = 2
        last_post_exc = None
        for item_attempt in range(item_indexing_retries + 1):
            try:
                # Ограничиваем ретраи POST для защиты от дублирования офферов при сетевых сбоях
                await execute_with_retry(
                    lambda: rac.send_request("POST", f"/units?storefront={sf}&embedded=eco_participation", json=unit_data),
                    rate_limiter=rate_limiter,
                    max_retries=2
                )
                return ("SUCCESS", sf, None)
            except Exception as post_exc:
                last_post_exc = post_exc
                err_str = str(post_exc).lower()
                is_item_indexing = (
                    "parameter [item]" in err_str
                    or ("item" in err_str and "is missing or has wrong value" in err_str)
                )
                if is_item_indexing and item_attempt < item_indexing_retries:
                    wait_sec = (item_attempt + 1) * 3.0
                    logger.warning(
                        f"[{account}][{ean}][{sf}] Catalog indexing delay (Parameter [item] is missing). Waiting {wait_sec:.1f}s before retry ({item_attempt + 1}/{item_indexing_retries})..."
                    )
                    await asyncio.sleep(wait_sec)
                    continue
                break

        post_exc = last_post_exc
        err_str = str(post_exc).lower()
        post_status = extract_status_code(post_exc)

        # Fallback PATCH ТОЛЬКО при подтверждённом дубликате (409 или "already exists")
        is_duplicate = (
            post_status == 409
            or "already exists" in err_str
            or "already-exists" in err_str
        )

        if is_duplicate:
            try:
                # 1. Поиск по id_offer
                get_resp = await execute_with_retry(
                    lambda: rac.send_request("GET", f"/units?storefront={sf}&id_offer={ean}"),
                    rate_limiter=rate_limiter,
                    max_retries=2
                )
                units = get_resp.get("data", []) if isinstance(get_resp, dict) else (get_resp if isinstance(get_resp, list) else [])
                # 2. Fallback: поиск по ean
                if not units:
                    get_resp2 = await execute_with_retry(
                        lambda: rac.send_request("GET", f"/units?storefront={sf}&ean={ean}"),
                        rate_limiter=rate_limiter,
                        max_retries=2
                    )
                    units = get_resp2.get("data", []) if isinstance(get_resp2, dict) else (get_resp2 if isinstance(get_resp2, list) else [])

                target_unit_id = None
                for u in units:
                    if isinstance(u, dict):
                        u_offer = str(u.get("id_offer") or "")
                        u_ean = u.get("ean")
                        u_ean_str = u_ean[0] if isinstance(u_ean, list) and u_ean else str(u_ean or "")
                        if u_offer == ean or str(u_ean_str) == ean:
                            target_unit_id = u.get("id_unit")
                            break

                # ВАЖНО: исключен слепой фоллбэк на units[0]!
                if not target_unit_id:
                    logger.warning(
                        f"[{account}][{ean}][{sf}] Duplicate 409 detected, but no matching unit found by id_offer/ean"
                    )
                else:
                    patch_data = {
                        "listing_price": listing_price,
                        "minimum_price": listing_price,
                        "handling_time": handling_time,
                        "amount": unit_amount,
                    }
                    if sg_id:
                        patch_data["id_shipping_group"] = sg_id
                    await execute_with_retry(
                        lambda: rac.send_request("PATCH", f"/units/{target_unit_id}?storefront={sf}", json=patch_data),
                        rate_limiter=rate_limiter,
                        max_retries=3
                    )
                    return ("SUCCESS", sf, None)
            except Exception as patch_exc:
                logger.warning(
                    f"[{account}][{ean}][{sf}] Fallback PATCH failed after 409 duplicate: {patch_exc}"
                )

        status = extract_status_code(post_exc)
        is_unconfigured_sf = (
            status == 400
            and (
                ("storefront" in err_str and (
                    "not configured" in err_str
                    or "not active" in err_str
                    or "not available" in err_str
                ))
                or (
                    "locale" in err_str and (
                        "not configured" in err_str
                        or "not active" in err_str
                        or "not available" in err_str
                    )
                )
            )
        )

        if is_unconfigured_sf:
            logger.warning(
                f"[{account}][{ean}][{sf}] Storefront skipped: not configured/inactive | {post_exc}"
            )
            await cp_mgr.record_error(
                ean, sf, f"Storefront unconfigured/inactive (400): {post_exc}"
            )
            return ("SKIPPED", sf, post_exc)
        else:
            logger.error(f"[{account}][{ean}][{sf}] Unit fatal error: {post_exc}")
            await cp_mgr.record_error(ean, sf, f"Unit error: {post_exc}")
            return ("FAILED", sf, post_exc)

    unit_results = await asyncio.gather(*[upload_single_storefront(sf) for sf in storefronts])
    successful_storefronts = [sf for st, sf, _ in unit_results if st == "SUCCESS"]
    skipped_storefronts = [sf for st, sf, _ in unit_results if st == "SKIPPED"]
    failed_storefronts = [sf for st, sf, _ in unit_results if st == "FAILED"]

    all_active_succeeded = (
        len(failed_storefronts) == 0
        and len(successful_storefronts) > 0
        and (len(successful_storefronts) + len(skipped_storefronts) == len(storefronts))
    )

    # Товар считается завершенным, если все активные витрины созданы успешно (без fatal errors)
    if storefronts and (len(successful_storefronts) == len(storefronts) or all_active_succeeded):
        await cp_mgr.record_completed(ean)
        skip_msg = f" (skipped unconfigured: {skipped_storefronts})" if skipped_storefronts else ""
        logger.info(
            f"[{account}][{ean}] SUCCESS: created on active storefronts {successful_storefronts}{skip_msg}"
        )
        return "SUCCESS"
    elif len(successful_storefronts) > 0:
        logger.warning(
            f"[{account}][{ean}] PARTIAL: created on {successful_storefronts}, but skipped={skipped_storefronts}, failed={failed_storefronts}"
        )
        return "PARTIAL"
    
    return "FAILED"

# ---------------------------------------------------------------------------
# Worker & Queue Architecture
# ---------------------------------------------------------------------------

async def worker(
    worker_id: int,
    queue: asyncio.Queue,
    account: str,
    storefronts: List[str],
    rac: RestApiController,
    rate_limiter: GlobalRateLimiter,
    cp_mgr: CheckpointManager,
    allowed_factories: Set[str],
    stats: Stats,
    logger: logging.Logger,
    dry_run: bool,
    allow_cat_fallback: bool,
    limit: int,
    in_flight_tracker: InFlightTracker,
    update_product_data: bool = False,
    eans_whitelist: Optional[Set[str]] = None,
    allow_all_factories: bool = False,
    reconciler: Optional[CatalogReconciler] = None,
    reconcile_only: bool = False,
    purge_orphans: bool = False,
    failed_items_mgr: Optional[FailedItemsManager] = None,
    aftercool: Optional[AftercoolService] = None,
    session_holder: Optional[List[str]] = None,
    stamm_cache: Optional[BoundedLRUCache] = None,
):
    """Потребитель очереди с гарантированным строгим лимитом и дедупликацией in-flight."""
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        # Проверяем и бронируем слот для лимита
        has_slot = await stats.try_reserve_slot(limit, reconcile_only=reconcile_only)
        if not has_slot:
            queue.task_done()
            continue

        raw_ean = AftercoolService._extract_ean(item) or ""
        ean = normalize_ean(raw_ean)

        # In-flight дедупликация: предотвращаем гонку, если один EAN пришел в разных пачках
        if ean:
            acquired = await in_flight_tracker.acquire(ean)
            if not acquired:
                logger.debug(f"[{account}][{ean}] EAN already in flight, skipping duplicate queue entry")
                await stats.release_reserved_slot()
                queue.task_done()
                continue
        else:
            acquired = False

        try:
            status = await process_item(
                item=item,
                account=account,
                storefronts=storefronts,
                rac=rac,
                rate_limiter=rate_limiter,
                cp_mgr=cp_mgr,
                allowed_factories=allowed_factories,
                logger=logger,
                dry_run=dry_run,
                allow_cat_fallback=allow_cat_fallback,
                update_product_data=update_product_data,
                eans_whitelist=eans_whitelist,
                allow_all_factories=allow_all_factories,
                reconciler=reconciler,
                reconcile_only=reconcile_only,
                purge_orphans=purge_orphans,
                aftercool=aftercool,
                session_holder=session_holder,
                stamm_cache=stamm_cache,
            )
            # В reconcile_only успех - это RECONCILED_VALID, PURGED_CROSS_ACCOUNT или PURGED_ORPHAN
            is_counted = (status == "SUCCESS") or (reconcile_only and status in ("RECONCILED_VALID", "PURGED_CROSS_ACCOUNT", "PURGED_ORPHAN"))
            if not is_counted:
                await stats.release_reserved_slot()
                await stats.inc(status, had_reserved_slot=False)
            else:
                await stats.inc(status, had_reserved_slot=True)

            # Синхронизация персистентной очереди повторов (DLQ):
            if status in ("SUCCESS", "RECONCILED_VALID", "PURGED_CROSS_ACCOUNT", "PURGED_ORPHAN", "SKIPPED_COMPLETED"):
                if ean and failed_items_mgr:
                    await failed_items_mgr.record_resolved(ean)
            elif status in ("FAILED", "PARTIAL"):
                if failed_items_mgr:
                    await failed_items_mgr.record_failed(ean=ean, item=item, status=status)
        except Exception as e:
            logger.error(f"Worker {worker_id} unexpected error: {e}", exc_info=True)
            await stats.release_reserved_slot()
            await stats.inc("FAILED", had_reserved_slot=False)
            if failed_items_mgr:
                await failed_items_mgr.record_failed(ean=ean, item=item, status="FAILED", error_msg=str(e))
        finally:
            if acquired and ean:
                await in_flight_tracker.release(ean)
            queue.task_done()

async def fetch_chunk_with_retry(
    aftercool: AftercoolService,
    session_holder: List[str],
    account: str,
    offset: int,
    page_size: int,
    logger: logging.Logger,
    max_retries: int = 5
) -> List[dict]:
    """Получение пачки из Aftercool с надежными повторными попытками и re-login."""
    for attempt in range(max_retries):
        try:
            chunk = await aftercool.get_products(
                session_holder[0],
                query=AftercoolProductsQuery(
                    account=account,
                    dataset="lister",
                    include_row=1,
                    limit=page_size,
                    offset=offset
                )
            )
            return chunk
        except Exception as e:
            logger.warning(f"[{account}] Aftercool fetch failed at offset {offset} (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(3.0 * (attempt + 1))
                try:
                    session_holder[0] = await aftercool.login()
                    logger.info(f"[{account}] Successfully refreshed Aftercool session")
                except Exception as login_err:
                    logger.error(f"[{account}] Re-login to Aftercool failed: {login_err}")
                continue
            raise e
    return []

# ---------------------------------------------------------------------------
# Account Orchestrator
# ---------------------------------------------------------------------------

async def process_account(
    account: str,
    limit: int,
    storefronts: List[str],
    concurrency: int,
    dry_run: bool,
    allow_cat_fallback: bool,
    custom_checkpoint: Optional[Path],
    custom_errors: Optional[Path],
    logger: logging.Logger,
    offset: int = 0,
    update_product_data: bool = False,
    eans_whitelist: Optional[Set[str]] = None,
    allow_all_factories: bool = False,
    multi_account: bool = False,
    resume: bool = False,
    rate_limit: float = 19.0,
    reconcile_only: bool = False,
    purge_orphans: bool = False,
    skip_preindex: bool = False,
    reconciler: Optional[CatalogReconciler] = None,
    retry_failed: bool = False,
):
    account_allowed = set(config.get_account_storefronts(account))
    effective_storefronts = [sf for sf in storefronts if sf in account_allowed]
    if len(effective_storefronts) < len(storefronts):
        ignored = [sf for sf in storefronts if sf not in account_allowed]
        logger.info(f"[{account}] Filtered out unsupported storefronts: {ignored} -> using {effective_storefronts}")

    # Защита от запуска с пустым списком витрин
    if not effective_storefronts:
        logger.error(
            f"[{account}] FATAL: No valid storefronts configured! "
            f"Requested: {storefronts}, Allowed for {account}: {account_allowed}. Aborting."
        )
        return

    # Превентивная валидация GPSR контактов перед стартом (предотвращает ошибки 422 от Kaufland API)
    contact_data = getattr(config, f"{account.upper()}_CONTACT_DATA", {})
    gpsr_required = {"name", "address", "email_address"}
    missing_gpsr = gpsr_required - set(contact_data.keys())
    if missing_gpsr:
        logger.error(
            f"[{account}] FATAL: GPSR contact data incomplete! "
            f"Missing fields: {missing_gpsr}. Fix config.{account.upper()}_CONTACT_DATA"
        )
        return

    logger.info(
        f"[{account}] Starting catalog processing | storefronts={effective_storefronts} | "
        f"concurrency={concurrency} | limit={limit} | update_product_data={update_product_data} | "
        f"reconcile_only={reconcile_only} | purge_orphans={purge_orphans}"
    )
    
    aftercool = reconciler.service if reconciler else AftercoolService()
    try:
        session_cookie = await aftercool.login()
    except Exception as e:
        logger.error(f"[{account}] Failed to initial login to Aftercool: {e}")
        return

    session_holder = [session_cookie]

    # Инициализация CatalogReconciler (быстрая индексация каталогов Aftercool)
    if not skip_preindex:
        if reconciler is None:
            reconciler = CatalogReconciler(service=aftercool)
        if not reconciler.is_initialized:
            try:
                logger.info(f"[{account}] Pre-indexing Aftercool catalogs for JV and XL...")
                await reconciler.preload_indices(session_holder[0], logger=logger)
            except Exception as e:
                logger.error(f"[{account}] Failed to preload Aftercool indices: {e}")
                if not dry_run or reconcile_only or purge_orphans:
                    logger.error(f"[{account}] Halting execution to prevent accidental purge or unverified actions!")
                    return
    else:
        reconciler = None

    if (reconcile_only or purge_orphans) and not reconciler.is_initialized:
        logger.error(
            f"[{account}] FATAL: CatalogReconciler is not initialized, but reconcile_only or purge_orphans was requested! "
            f"Halting execution to prevent unverified actions."
        )
        return

    allowed_factories = await asyncio.to_thread(load_allowed_factories)

    # Fail-closed защита белого списка фабрик
    if not allowed_factories and not allow_all_factories:
        logger.error(
            f"[{account}] FATAL: {ALLOWED_FACTORIES_FILE.name} is missing or empty! "
            f"Bulk upload halted (fail-closed). Use --allow-all-factories to override."
        )
        return

    logger.info(f"[{account}] Loaded {len(allowed_factories)} allowed factories from {ALLOWED_FACTORIES_FILE.name}")

    cp_mgr = CheckpointManager(
        account,
        custom_cp=custom_checkpoint,
        custom_err=custom_errors,
        multi_account=multi_account
    )
    completed_eans = await cp_mgr.load_completed()
    logger.info(f"[{account}] Loaded {len(completed_eans):,} completed EANs from {cp_mgr.cp_file.name}")

    rate_limiter = GlobalRateLimiter(rate=rate_limit)
    in_flight_tracker = InFlightTracker()
    stats = Stats()
    queue = asyncio.Queue(maxsize=1000)

    failed_items_mgr = FailedItemsManager(account)
    pending_failed_map = await failed_items_mgr.load()
    if pending_failed_map:
        logger.info(
            f"[{account}] Loaded {len(pending_failed_map)} pending FAILED/PARTIAL items from {failed_items_mgr.file_path.name}"
        )

    state_file = BACKEND_DIR / f".reupload_state_{account}.json"
    if resume and state_file.exists():
        try:
            state_data = json.loads(state_file.read_text(encoding="utf-8"))
            saved_offset = state_data.get("last_offset")
            if saved_offset is not None and isinstance(saved_offset, int):
                offset = saved_offset
                logger.warning(
                    f"[{account}] RESUME ACTIVE: Resuming from offset {offset}. "
                    f"Note: new items added before this offset in Aftercool will not be scanned."
                )
        except Exception as err:
            logger.error(f"[{account}] Failed to read resume state from {state_file.name}: {err}")

    async with aiohttp.ClientSession() as aio_session:
        rac = RestApiController(controller=account, session=aio_session)

        # Запускаем пул воркеров
        stamm_cache = BoundedLRUCache(maxsize=10000, ttl_sec=86400.0)
        workers = []
        for i in range(concurrency):
            w = asyncio.create_task(
                worker(
                    worker_id=i,
                    queue=queue,
                    account=account,
                    storefronts=effective_storefronts,
                    rac=rac,
                    rate_limiter=rate_limiter,
                    cp_mgr=cp_mgr,
                    allowed_factories=allowed_factories,
                    stats=stats,
                    logger=logger,
                    dry_run=dry_run,
                    allow_cat_fallback=allow_cat_fallback,
                    limit=limit,
                    in_flight_tracker=in_flight_tracker,
                    update_product_data=update_product_data,
                    eans_whitelist=eans_whitelist,
                    allow_all_factories=allow_all_factories,
                    reconciler=reconciler,
                    reconcile_only=reconcile_only,
                    purge_orphans=purge_orphans,
                    failed_items_mgr=failed_items_mgr,
                    aftercool=aftercool,
                    session_holder=session_holder,
                    stamm_cache=stamm_cache,
                )
            )
            workers.append(w)

        # При --resume или --retry-failed: в первую очередь отправляем в очередь все ранее
        # упавшие/частичные товары из персистентного DLQ файла .failed_items_{account}.json,
        # предварительно отфильтровывая любые мусорные товары и удаленные файлы
        if (resume or retry_failed) and failed_items_mgr.pending_count() > 0:
            pending_items = failed_items_mgr.get_pending_items()
            clean_pending = []
            for p_item in pending_items:
                sf = str(p_item.get("source_file") or "").strip()
                p_name = str(p_item.get("name") or "").strip()
                p_desc = str((p_item.get("row") or {}).get("Artikelbeschreibung") or "").strip()
                p_ean = normalize_ean(AftercoolService._extract_ean(p_item) or "")
                if is_blacklisted_source(sf) or is_trash_title(p_name, raw_name=p_name, desc=p_desc):
                    if p_ean:
                        failed_items_mgr.remove_item(p_ean)
                    continue
                clean_pending.append(p_item)

            if len(clean_pending) < len(pending_items):
                logger.info(
                    f"[{account}] Purged {len(pending_items) - len(clean_pending)} blacklisted/trash items from persistent DLQ"
                )
                await failed_items_mgr.flush()

            logger.info(
                f"[{account}] Prioritizing {len(clean_pending)} previously FAILED/PARTIAL items from persistent DLQ before catalog scan"
            )
            for p_item in clean_pending:
                await stats.add_fetched(1)
                await queue.put(p_item)

        reached_limit = False
        if retry_failed:
            logger.info(
                f"[{account}] --retry-failed mode active: only retrying {failed_items_mgr.pending_count()} failed items, catalog scan skipped"
            )
        # Продюсер: при указании конкретных EAN обрабатывает их напрямую
        elif eans_whitelist:
            logger.info(f"[{account}] Targeted EAN mode: processing {len(eans_whitelist)} EAN(s)")
            for target_ean in sorted(eans_whitelist):
                if reconciler and reconciler.is_initialized:
                    target_acc, ownership_status = reconciler.resolve_target_account(target_ean)
                    if target_acc != account:
                        if target_acc is not None:
                            # Товар принадлежит другому аккаунту (коллизия)!
                            synthetic_item = {
                                "product_id": target_ean,
                                "ean": target_ean,
                                "source_file": "cross_account_collision",
                            }
                            await stats.add_fetched(1)
                            await queue.put(synthetic_item)
                            continue
                        else:
                            # Сирота (нет ни в JV, ни в XL)
                            if purge_orphans:
                                synthetic_item = {
                                    "product_id": target_ean,
                                    "ean": target_ean,
                                    "source_file": "orphan",
                                }
                                await stats.add_fetched(1)
                                await queue.put(synthetic_item)
                                continue
                            else:
                                logger.warning(f"[{account}] EAN {target_ean} is ORPHAN. Pass --purge-orphans to delete from Kaufland.")
                                await stats.inc("SKIPPED_NOT_IN_AFTERCOOL")
                                continue

                # Если товар принадлежит текущему аккаунту:
                try:
                    target_chunk = await aftercool.get_products(
                        session_holder[0],
                        query=AftercoolProductsQuery(
                            account=account,
                            dataset="lister",
                            include_row=1,
                            limit=10,
                            q=target_ean,
                        )
                    )
                    matched = [it for it in target_chunk if normalize_ean(AftercoolService._extract_ean(it) or "") == target_ean]
                    canonical = select_canonical_item(matched, allowed_factories, allow_all_factories)
                    if canonical:
                        await stats.add_fetched(1)
                        await queue.put(canonical)
                    else:
                        if any(is_uk_source(it.get("source_file")) for it in matched):
                            logger.warning(f"[{account}] EAN {target_ean} found only in UK warehouse files. Skipped.")
                            await stats.inc("SKIPPED_UK_SOURCE")
                        else:
                            logger.warning(f"[{account}] EAN {target_ean} NOT FOUND in Aftercool ({account}), skipping.")
                            await stats.inc("SKIPPED_NOT_IN_AFTERCOOL")
                            skipped_file = BACKEND_DIR / "skipped_not_in_aftercool.txt"
                            try:
                                await asyncio.to_thread(
                                    CheckpointManager._sync_append,
                                    skipped_file,
                                    f"{target_ean} # [{account}] Not found in Aftercool\n"
                                )
                            except Exception as skip_err:
                                logger.error(f"[{account}] Failed to record skipped EAN {target_ean}: {skip_err}")
                except Exception as e:
                    logger.warning(f"[{account}] Failed to fetch targeted EAN {target_ean}: {e}")
        else:
            # Иначе: постранично загружает товары из Aftercool с защитой от зацикливания
            page_size = 500
            reached_limit = False
            consecutive_same_chunk_count = 0
            last_chunk_hash = None
            max_pages = 5000
            page_num = 0
            remaining_whitelist = set(eans_whitelist) if eans_whitelist else None

            while not reached_limit and page_num < max_pages:
                page_num += 1
                try:
                    chunk = await fetch_chunk_with_retry(
                        aftercool=aftercool,
                        session_holder=session_holder,
                        account=account,
                        offset=offset,
                        page_size=page_size,
                        logger=logger,
                        max_retries=5
                    )
                except Exception as e:
                    logger.error(f"[{account}] Fatal Aftercool fetch error at offset {offset}: {e}. Halting producer.")
                    break

                if not chunk:
                    logger.info(f"[{account}] Reached end of catalog at offset {offset}")
                    break

                # Защита от бесконечного цикла при зависании offset
                chunk_fingerprint = hash(tuple(str(x.get("product_id") or x.get("id") or "") for x in chunk[:10]))
                if chunk_fingerprint == last_chunk_hash:
                    consecutive_same_chunk_count += 1
                    if consecutive_same_chunk_count >= 3:
                        logger.error(f"[{account}] Pagination stuck: received identical chunk 3 times at offset {offset}. Halting.")
                        break
                else:
                    consecutive_same_chunk_count = 0
                    last_chunk_hash = chunk_fingerprint

                await stats.add_fetched(len(chunk))
                offset += len(chunk)

                for item in chunk:
                    sf = str(item.get("source_file") or "").strip()
                    if is_uk_source(sf):
                        await stats.inc("SKIPPED_UK_SOURCE")
                        continue
                    if is_blacklisted_source(sf):
                        await stats.inc("SKIPPED_BLACKLISTED_FILE")
                        continue
                    raw_name = str(item.get("name") or "").strip()
                    raw_desc = str((item.get("row") or {}).get("Artikelbeschreibung") or "").strip()
                    if is_trash_title(raw_name, raw_name=raw_name, desc=raw_desc):
                        await stats.inc("SKIPPED_TRASH_TITLE")
                        continue
                    await queue.put(item)
                    if remaining_whitelist:
                        it_ean = normalize_ean(AftercoolService._extract_ean(item) or "")
                        remaining_whitelist.discard(it_ean)
                        if not remaining_whitelist:
                            logger.info(f"[{account}] All {len(eans_whitelist)} target EANs found in Aftercool, stopping scan early!")
                            reached_limit = True
                            break
                    current_processed = (
                        (stats.reconciled_valid + stats.purged_cross_account + stats.purged_orphan + stats.reserved_slots)
                        if reconcile_only
                        else (stats.uploaded + stats.reserved_slots)
                    )
                    if limit > 0 and current_processed >= limit:
                        logger.info(f"[{account}] Target limit of {limit} processed items reached!")
                        reached_limit = True
                        break

                # Периодическая безопасная фиксация оффсета (каждые 5 страниц):
                # Дожидаемся обработки текущей очереди, чтобы зафиксированный оффсет
                # гарантированно соответствовал уже завершённым товарам
                if page_num % 5 == 0 or reached_limit:
                    await queue.join()
                    await failed_items_mgr.flush()
                    try:
                        state_payload = json.dumps({
                            "account": account,
                            "last_offset": offset,
                            "pending_failed_count": failed_items_mgr.pending_count(),
                            "timestamp": datetime.now().isoformat()
                        }, ensure_ascii=False, indent=2)
                        await asyncio.to_thread(state_file.write_text, state_payload, encoding="utf-8")
                        logger.info(
                            f"[{account}] Safely committed offset checkpoint: {offset} "
                            f"(pending failed in DLQ: {failed_items_mgr.pending_count()})"
                        )
                    except Exception as state_err:
                        logger.warning(f"[{account}] Failed to save state checkpoint: {state_err}")

                logger.info(stats.summary_line(account))

        # 1. Дожидаемся полного завершения обработки всех оставшихся в очереди товаров
        await queue.join()

        # 2. Финальный повторный проход для оставшихся FAILED/PARTIAL товаров (на случай временных сбоев API)
        if not retry_failed and failed_items_mgr.pending_count() > 0:
            remaining_failed = failed_items_mgr.get_pending_items()
            logger.info(
                f"[{account}] Catalog scan finished. Running final retry pass for {len(remaining_failed)} FAILED/PARTIAL items..."
            )
            for it in remaining_failed:
                await queue.put(it)
            await queue.join()
            await failed_items_mgr.flush()

        # 3. Корректно завершаем воркеры
        for _ in range(concurrency):
            await queue.put(None)
        await asyncio.gather(*workers)

        # 4. Финализация состояния чекпоинта оффсета
        pending_failed = failed_items_mgr.pending_count()
        if reached_limit or pending_failed > 0:
            # Сохраняем финальный оффсет и количество оставшихся ошибок для безопасного --resume в будущем
            try:
                state_payload = json.dumps({
                    "account": account,
                    "last_offset": offset,
                    "pending_failed_count": pending_failed,
                    "timestamp": datetime.now().isoformat()
                }, ensure_ascii=False, indent=2)
                await asyncio.to_thread(state_file.write_text, state_payload, encoding="utf-8")
                if pending_failed > 0:
                    logger.warning(
                        f"[{account}] Checkpoint PRESERVED at offset {offset} because {pending_failed} item(s) "
                        f"remain FAILED/PARTIAL in {failed_items_mgr.file_path.name}. "
                        f"Run with --resume or --retry-failed to re-attempt them."
                    )
                else:
                    logger.info(f"[{account}] Preserved limit checkpoint at offset {offset}")
            except Exception as state_err:
                logger.warning(f"[{account}] Failed to save final state checkpoint: {state_err}")
        else:
            # Каталог пройден до конца и все товары успешны — очищаем файл состояния
            if state_file.exists():
                try:
                    await asyncio.to_thread(state_file.unlink, missing_ok=True)
                except Exception:
                    pass

    logger.info("=" * 70)
    logger.info(f"[{account}] FINAL SUMMARY:")
    logger.info(f"[{account}] Total fetched from Aftercool: {stats.total_fetched:,}")
    logger.info(f"[{account}] Successfully uploaded (NEW): {stats.uploaded:,}")
    logger.info(f"[{account}] Partially uploaded:          {stats.partial:,}")
    logger.info(f"[{account}] Already completed (SKIPPED): {stats.already_completed:,}")
    logger.info(f"[{account}] Skipped factory:            {stats.skipped_factory:,}")
    if stats.skipped_uk > 0:
        logger.info(f"[{account}] Skipped (UK warehouse):     {stats.skipped_uk:,}")
    if stats.purged_cross_account > 0:
        logger.info(f"[{account}] Purged cross-account items: {stats.purged_cross_account:,}")
    if stats.purged_orphan > 0:
        logger.info(f"[{account}] Purged orphan items:        {stats.purged_orphan:,}")
    if stats.reconciled_valid > 0:
        logger.info(f"[{account}] Reconciled valid (checked):  {stats.reconciled_valid:,}")
    if stats.skipped_filter > 0:
        logger.info(f"[{account}] Skipped (EAN filter):       {stats.skipped_filter:,}")
    if stats.skipped_not_found > 0:
        logger.info(f"[{account}] Not found in Aftercool:     {stats.skipped_not_found:,}")
    logger.info(f"[{account}] Failed items:               {stats.failed:,}")
    if failed_items_mgr.pending_count() > 0:
        logger.warning(
            f"[{account}] ATTENTION: {failed_items_mgr.pending_count()} items remain FAILED/PARTIAL! "
            f"Saved in {failed_items_mgr.file_path.name} (run with --retry-failed or --resume to re-attempt)"
        )
    logger.info(f"[{account}] Checkpoint file:            {cp_mgr.cp_file.name} (total {len(cp_mgr.completed_eans):,} EANs)")
    logger.info(f"[{account}] Errors file:                {cp_mgr.err_file.name}")
    logger.info("=" * 70)

# ---------------------------------------------------------------------------
# CLI Main
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(description="Reupload Aftercool catalog to Kaufland (JV & XL)")
    parser.add_argument("--accounts", type=str, default="jv", help="Comma-separated list of accounts (e.g. jv, xl)")
    parser.add_argument("--storefronts", type=str, default=",".join(DEFAULT_STOREFRONTS), help="Comma-separated storefronts")
    parser.add_argument("--limit", type=int, default=0, help="Max new items to upload (0 = unlimited)")
    parser.add_argument("--concurrency", type=int, default=8, help="Worker coroutines count per account")
    parser.add_argument("--dry-run", action="store_true", help="Scan and parse without sending to Kaufland API")
    parser.add_argument("--checkpoint", type=str, default=None, help="Custom checkpoint file path")
    parser.add_argument("--errors-file", type=str, default=None, help="Custom errors file path")
    parser.add_argument("--allow-cat-fallback", action="store_true", default=False, help="Allow 46001 Möbel fallback if decide API fails")
    parser.add_argument("--allow-all-factories", action="store_true", default=False, help="Allow uploading items from any factory, ignoring allowed_factories.txt")
    parser.add_argument("--offset", type=int, default=0, help="Starting offset in Aftercool catalog (default: 0)")
    parser.add_argument("--resume", action="store_true", default=False, help="Resume from last saved offset in .reupload_state_{account}.json")
    parser.add_argument("--rate-limit", type=float, default=19.0, help="Max requests per second for Kaufland API token bucket (default: 19.0)")
    parser.add_argument("--update-product-data", action="store_true", default=False, help="Only update product-data without re-uploading units")
    parser.add_argument("--eans", type=str, default="", help="Comma-separated EAN whitelist for test runs (e.g. 4069943370027,4069943370201)")
    parser.add_argument("--from-csv", type=str, default=None, help="Path to Kaufland Invalid Items CSV to extract EANs from")
    parser.add_argument("--reconcile-only", action="store_true", default=False, help="Only reconcile and purge cross-account and orphan items without updating product-data")
    parser.add_argument("--purge-orphans", action="store_true", default=False, help="Purge items not found in Aftercool from Kaufland")
    parser.add_argument("--skip-preindex", action="store_true", default=False, help="Skip full catalog preload (useful for quick single-EAN debugging)")
    parser.add_argument("--retry-failed", action="store_true", default=False, help="Only retry previously FAILED/PARTIAL items without scanning catalog")
    args = parser.parse_args()

    accounts = [x.strip().lower() for x in args.accounts.split(",") if x.strip()]
    storefronts = [x.strip().lower() for x in args.storefronts.split(",") if x.strip()]

    logs_dir = BACKEND_DIR / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp_str = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"reupload_aftercool_{'_'.join(accounts)}_{timestamp_str}.log"
    logger = setup_logger(log_file)

    logger.info(
        f"Starting reupload with parameters: accounts={accounts}, storefronts={storefronts}, "
        f"limit={args.limit}, concurrency={args.concurrency}, dry_run={args.dry_run}, "
        f"update_product_data={args.update_product_data}, allow_all_factories={args.allow_all_factories}, "
        f"resume={args.resume}, rate_limit={args.rate_limit}, reconcile_only={args.reconcile_only}, "
        f"purge_orphans={args.purge_orphans}, skip_preindex={args.skip_preindex}"
    )

    custom_cp = Path(args.checkpoint) if args.checkpoint else None
    custom_err = Path(args.errors_file) if args.errors_file else None

    eans_whitelist: Optional[Set[str]] = None
    if args.eans:
        eans_whitelist = {normalize_ean(e) for e in args.eans.split(",") if normalize_ean(e)}
        
    if args.from_csv:
        try:
            csv_eans = set()
            with open(args.from_csv, "r", encoding="utf-8", errors="ignore") as f:
                sample = f.read(4096)
                f.seek(0)
                delimiter = ","
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
                    delimiter = dialect.delimiter
                except Exception:
                    pass
                reader = csv.DictReader(f, delimiter=delimiter)
                ean_col = None
                ean_headers = {"ean", "ean_code", "barcode", "gtin", "gtin_code", "item_ean", "product_ean"}
                if reader.fieldnames:
                    for h in reader.fieldnames:
                        if h and h.strip().lower() in ean_headers:
                            ean_col = h
                            break
                    if not ean_col:
                        ean_col = reader.fieldnames[0]

                for row in reader:
                    val = (row.get(ean_col) if ean_col else None) or ""
                    norm = normalize_ean(str(val).strip())
                    if norm and is_valid_ean(norm):
                        csv_eans.add(norm)

            if not csv_eans:
                # Фоллбэк: если DictReader ничего не нашел (файл без заголовков или текстовый список)
                with open(args.from_csv, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        for token in re.split(r'[,;\s\t"]+', line):
                            norm = normalize_ean(token.strip())
                            if norm and is_valid_ean(norm):
                                csv_eans.add(norm)

            if eans_whitelist is None:
                eans_whitelist = csv_eans
            else:
                eans_whitelist.update(csv_eans)
            logger.info(f"Loaded {len(csv_eans)} valid normalized EANs from CSV {args.from_csv}")
        except Exception as e:
            logger.error(f"Failed to read CSV {args.from_csv}: {e}")

    if eans_whitelist is not None:
        logger.info(f"EAN whitelist active: {len(eans_whitelist)} total normalized EANs")

    shared_reconciler = CatalogReconciler(service=AftercoolService())
    if not args.skip_preindex:
        try:
            session_cookie = await shared_reconciler.service.login()
            logger.info("Pre-indexing Aftercool catalogs for JV and XL (shared reconciler)...")
            await shared_reconciler.preload_indices(session_cookie, logger=logger)
        except Exception as e:
            logger.error(f"Failed to preload Aftercool indices in main: {e}")
            if not args.dry_run or args.reconcile_only or args.purge_orphans:
                logger.error("Halting execution to prevent accidental purge or unverified actions!")
                return

    if (args.reconcile_only or args.purge_orphans) and not shared_reconciler.is_initialized:
        logger.error(
            "FATAL: CatalogReconciler is not initialized, but --reconcile-only or --purge-orphans was requested! "
            "Halting execution to prevent unverified actions."
        )
        return

    for account in accounts:
        await process_account(
            account=account,
            limit=args.limit,
            storefronts=storefronts,
            concurrency=args.concurrency,
            dry_run=args.dry_run,
            allow_cat_fallback=args.allow_cat_fallback,
            custom_checkpoint=custom_cp,
            custom_errors=custom_err,
            logger=logger,
            offset=args.offset,
            update_product_data=args.update_product_data,
            eans_whitelist=eans_whitelist,
            allow_all_factories=args.allow_all_factories,
            multi_account=len(accounts) > 1,
            resume=args.resume,
            rate_limit=args.rate_limit,
            reconcile_only=args.reconcile_only,
            purge_orphans=args.purge_orphans,
            skip_preindex=args.skip_preindex,
            reconciler=None if args.skip_preindex else shared_reconciler,
            retry_failed=args.retry_failed,
        )

if __name__ == "__main__":
    asyncio.run(main())
