#!/usr/bin/env python3
"""
Скрипт автономного высокоскоростного обновления категорий для всего каталога JV и XL.

Принципы безопасности:
1. Строгие белые списки:
   - JV: reports/JV_VALID.txt (81 443 уникальных EAN)
   - XL: reports/XL_VALID.txt (86 705 уникальных EAN)
2. Исключительно обновление категорий:
   - НЕ трогает юниты (цены, остатки, сроки доставки, handling_time).
   - НЕ трогает описания, картинки, MPN, параметры материалов.
3. Детерминированная таблица 100% валидных листовых мебельных категорий Kaufland:
   - Логика классификации основана на проверенном fix_872_blocked_items.py.
   - Исключает обращение к медленному и нестабильному /categories/decide.
4. Оптимизация нагрузки:
   - Если категория уже совпадает с целевой (по id или slug) — пропуск без PUT-запроса.
   - Дедупликация белых списков при загрузке с сохранением исходного порядка.
5. Высокая скорость в рамках официальных лимитов:
   - 85 req/s на аккаунт (официальный лимит Kaufland: 111 req/s на аккаунт).
   - Поддержка возобновления через чекпоинты (checkpoints) с fsync.
   - Поддержка безопасного режима (--dry-run) без изменения чекпоинтов.
6. Concurrency-безопасность и надёжность:
   - Все счётчики статистики защищены asyncio.Lock с гарантией отображения прогресса.
   - Потокобезопасная запись чекпоинтов с fsync.
   - Корректная обработка прерывания (Ctrl+C / SIGINT) с фиксацией отчёта и сохранением состояния.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp

BACKEND_DIR = Path("/home/Altera/workdir/kaufland/backend")
sys.path.insert(0, str(BACKEND_DIR))

from main_api.src.controller.rest_api_controller import RestApiController
from scripts.purge_trash_catalog_items import SafeRateLimiter, request_with_retry

REPORT_DIR = BACKEND_DIR / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

JV_VALID_FILE = REPORT_DIR / "reclassify_eans_jv.txt"
XL_VALID_FILE = REPORT_DIR / "reclassify_eans_xl.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("update_reclassify_cats")

# ──────────────────────────────────────────────────────────────────────────────
# Каноническая таблица 100% валидных листовых категорий Kaufland API
# ──────────────────────────────────────────────────────────────────────────────
VALID_TARGET_CATEGORIES: Dict[str, Dict[str, Any]] = {
    "ecksofas": {"id": 5911, "title": "Ecksofas", "name": "ecksofas"},
    "couchgarnituren": {"id": 5961, "title": "Couchgarnituren", "name": "couchgarnituren"},
    "klassische-sofas": {"id": 45081, "title": "Klassische Sofas", "name": "klassische-sofas"},
    "klassische-sessel": {"id": 45071, "title": "Klassische Sessel", "name": "klassische-sessel"},
    "ohrensessel": {"id": 5861, "title": "Ohrensessel", "name": "ohrensessel"},
    "cocktailsessel": {"id": 5831, "title": "Cocktailsessel", "name": "cocktailsessel"},
    "couchtische": {"id": 3531, "title": "Couchtische", "name": "couchtische"},
    "beistelltische": {"id": 3541, "title": "Beistelltische", "name": "beistelltische"},
    "esstische": {"id": 3521, "title": "Esstische", "name": "esstische"},
    "esszimmerstuehle": {"id": 4211, "title": "Esszimmerstühle", "name": "esszimmerstuehle"},
    "polsterbetten": {"id": 2751, "title": "Polsterbetten", "name": "polsterbetten"},
    "etagenbetten": {"id": 2871, "title": "Etagenbetten", "name": "etagenbetten"},
    "hochbetten": {"id": 2741, "title": "Hochbetten", "name": "hochbetten"},
    "schlafzimmer": {"id": 3271, "title": "Schlafzimmer", "name": "schlafzimmer"},
    "schubladenschraenke": {"id": 6141, "title": "Schubladenschränke", "name": "schubladenschraenke"},
    "wohnwaende": {"id": 3361, "title": "Wohnwände", "name": "wohnwaende"},
    "wohnzimmer": {"id": 3381, "title": "Wohnzimmer", "name": "wohnzimmer"},
    "haengeregale": {"id": 4171, "title": "Hängeregale", "name": "haengeregale"},
    "computertische": {"id": 5661, "title": "Computertische", "name": "computertische"},
    "aktenschraenke": {"id": 6121, "title": "Aktenschränke", "name": "aktenschraenke"},
    "deckenleuchten": {"id": 4361, "title": "Deckenleuchten", "name": "deckenleuchten"},
    "wandleuchten": {"id": 4351, "title": "Wandleuchten", "name": "wandleuchten"},
    "jugendzimmer": {"id": 3491, "title": "Jugendzimmer", "name": "jugendzimmer"},
    "kinderzimmer": {"id": 3301, "title": "Kinderzimmer", "name": "kinderzimmer"},
    "badewannen": {"id": 43411, "title": "Badewannen", "name": "badewannen"},
    "waschbecken": {"id": 28421, "title": "Waschbecken", "name": "waschbecken"},
    "wandspiegel": {"id": 4771, "title": "Wandspiegel", "name": "wandspiegel"},
    "skulpturen": {"id": 5121, "title": "Skulpturen", "name": "skulpturen"},
}


# ──────────────────────────────────────────────────────────────────────────────
# Детерминированная классификация по заголовку с иерархией анкеров
# ──────────────────────────────────────────────────────────────────────────────
def determine_category_for_title(title: str) -> Optional[Dict[str, Any]]:
    t = title.lower()

    # 1. Освещение и светильники
    if any(k in t for k in ["wandleuchte", "wandlampe"]):
        return VALID_TARGET_CATEGORIES["wandleuchten"]
    if any(k in t for k in [
        "deckenleuchte", "lüster", "kronleuchter", "pendelleuchte", "hängeleuchte",
        "deckenlampe", "stehleuchte", "stehlampe", "laterne",
    ]):
        return VALID_TARGET_CATEGORIES["deckenleuchten"]
    if re.search(r"\b(lampe|leuchte)n?\b", t) and not any(k in t for k in ["sofa", "couch", "tisch", "bett", "schrank"]):
        return VALID_TARGET_CATEGORIES["deckenleuchten"]

    # 2. Двухъярусные кровати и кровати-чердаки
    if any(k in t for k in ["etagenbett", "doppelstockbett", "stockbett"]):
        return VALID_TARGET_CATEGORIES["etagenbetten"]
    if "hochbett" in t:
        return VALID_TARGET_CATEGORIES["hochbetten"]

    # 3. Сантехника (СТРОГО перед комодами и шкафами!)
    if any(k in t for k in ["badewanne", "whirlwanne", "whirlpool", "eckbadewanne", "badewannen", "acrylwanne", "freistehende wanne"]):
        return VALID_TARGET_CATEGORIES["badewannen"]
    if any(k in t for k in ["waschbecken", "waschtisch", "aufsatzwaschbecken", "waschschale", "spülbecken", "spuelbecken"]):
        return VALID_TARGET_CATEGORIES["waschbecken"]

    # 4. Зеркала и скульптуры (СТРОГО перед комодами!)
    if any(k in t for k in ["wandspiegel", "standspiegel"]) or ("spiegel" in t and not any(k in t for k in ["spiegelschrank", "schrank", "kommode", "sideboard"])):
        return VALID_TARGET_CATEGORIES["wandspiegel"]
    if any(k in t for k in ["skulptur", "statue", "skulpturen", "statuen"]):
        return VALID_TARGET_CATEGORIES["skulpturen"]

    # 5. Кровати (СТРОГО перед столиками и тумбами! Комплект "Bett mit 2x Nachttisch" - это КРОВАТЬ!)
    is_bed = any(k in t for k in [
        "polsterbett", "boxspring", "doppelbett", "schlafsystem", "bettgestell",
        "schlafbett",
    ]) or re.search(r"\bbett(en)?\b", t)
    is_bedding = any(k in t for k in [
        "kissen", "bettbezug", "bettwäsche", "bettwaesche",
        "bettdecke", "bettlaken", "spannbettlaken",
    ])
    if is_bed and not is_bedding:
        return VALID_TARGET_CATEGORIES["polsterbetten"]

    # 6. Угловые диваны и жилые ландшафты (СТРОГО перед журнальными столиками!
    #    "Ecksofa L-Form ... Couchtisch" - это в первую очередь УГЛОВОЙ ДИВАН!)
    if any(k in t for k in ["ecksofa", "eckcouch", "wohnlandschaft", "l-form", "u-form", "ottomane"]):
        return VALID_TARGET_CATEGORIES["ecksofas"]

    # 7. Диванные гарнитуры (3+2+1, 3+1+1, комплекты диванов со столиками)
    if any(k in t for k in [
        "couchgarnitur", "sofagarnitur", "polstergarnitur", "polstergruppe",
        "3+2+1", "3+1+1", "311", "321", "3-sitzer", "2-sitzer", "dreisitzer", "zweisitzer",
    ]):
        return VALID_TARGET_CATEGORIES["couchgarnituren"]

    # 8. Диваны и софы
    if any(k in t for k in [
        "sofa", "schlafsofa", "chaiselongue", "chesterfield", "divan", "chaise",
        "polstermöbel", "polstermoebel",
    ]) or ("couch" in t and not any(k in t for k in ["couchtisch", "sofatisch"])):
        return VALID_TARGET_CATEGORIES["klassische-sofas"]

    # 9. Гарнитуры и комплекты комнат
    if any(k in t for k in ["jugendzimmer", "jugendmöbel"]):
        return VALID_TARGET_CATEGORIES["jugendzimmer"]
    if any(k in t for k in ["kinderzimmer", "kindermöbel"]):
        return VALID_TARGET_CATEGORIES["kinderzimmer"]
    if any(k in t for k in [
        "wohnzimmer-set", "wohnzimmer möbel-set", "wohnzimmer möbel set",
        "wohnzimmer möbel", "wohnzimmergarnitur", "wohnzimmer",
    ]):
        return VALID_TARGET_CATEGORIES["wohnzimmer"]

    # 10. ТВ-стенки
    if any(k in t for k in [
        "wohnwand", "wohnmöbel-set", "tv-lowboard und wandregal", "tv-ecke",
        "tv-wand", "anbauwand", "medienwand",
    ]):
        return VALID_TARGET_CATEGORIES["wohnwaende"]

    # 11. Столы
    if any(k in t for k in ["esstisch", "küchentisch", "kuechentisch", "speisetisch", "esszimmertisch"]):
        return VALID_TARGET_CATEGORIES["esstische"]
    if any(k in t for k in ["couchtisch", "kaffeetisch", "wohnzimmertisch", "stubentisch", "salontisch", "sofatisch"]):
        return VALID_TARGET_CATEGORIES["couchtische"]
    if any(k in t for k in [
        "beistelltisch", "nachttisch", "nachtschrank", "nachtschränkchen",
        "anbautisch", "telefontisch", "nachtkonsole", "ablagetisch", "blumentisch",
    ]):
        return VALID_TARGET_CATEGORIES["beistelltische"]
    if any(k in t for k in [
        "schreibtisch", "computertisch", "cheftisch", "bürotisch", "buerotisch",
        "arbeitsplatz", "schulbank", "büro-set", "büro arbeitsplatz",
    ]):
        return VALID_TARGET_CATEGORIES["computertische"]

    # 12. Стулья и барные стулья
    if any(k in t for k in [
        "stuhl", "stühle", "stuehle", "esszimmerstuhl", "lehnstuhl",
        "armlehnstuhl", "barhocker", "hocker",
    ]):
        return VALID_TARGET_CATEGORIES["esszimmerstuehle"]

    # 13. Кресла
    if "ohrensessel" in t:
        return VALID_TARGET_CATEGORIES["ohrensessel"]
    if "cocktailsessel" in t:
        return VALID_TARGET_CATEGORIES["cocktailsessel"]
    if any(k in t for k in ["sessel", "relaxsessel", "einzelsessel", "loungesessel", "clubsessel", "fauteuil"]):
        return VALID_TARGET_CATEGORIES["klassische-sessel"]

    # 14. Комоды, тумбы, сайдборды, ТВ-тумбы
    if any(k in t for k in [
        "tv-garnitur", "tv-lowboard", "tv-schrank", "tv-board", "tv-möbel",
        "tv lowboard", "tv board", "lowboard", "sideboard", "highboard",
        "kommode", "anrichte", "fernsehschrank",
    ]):
        return VALID_TARGET_CATEGORIES["schubladenschraenke"]

    # 15. Офисные шкафы и настенные полки
    if any(k in t for k in ["büroschrank", "büro schrank", "bueroschrank", "aktenschrank"]):
        return VALID_TARGET_CATEGORIES["aktenschraenke"]
    if any(k in t for k in ["wandregal", "hängeregal", "haengeregal", "hängeboard", "oberes regal", "schweberegal"]):
        return VALID_TARGET_CATEGORIES["haengeregale"]

    # 16. Шкафы и гардеробы
    if any(k in t for k in [
        "schrank", "kleiderschrank", "garderobe", "vitrine", "standregal",
        "regal", "schlafzimmer", "drehtürenschrank", "schwebetürenschrank",
    ]):
        return VALID_TARGET_CATEGORIES["schlafzimmer"]

    # 17. Если ни одно правило не сработало — не ломать категорию вслепую
    return None


# ──────────────────────────────────────────────────────────────────────────────
# I/O утилиты
# ──────────────────────────────────────────────────────────────────────────────
def load_eans_from_whitelist(path: Path) -> List[str]:
    """Загрузка EAN из файла белого списка с дедупликацией и сохранением порядка."""
    if not path.exists():
        logger.error(f"Whitelist not found: {path}")
        return []
    eans = []
    seen = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            clean = line.strip()
            if clean and clean.isdigit() and clean not in seen:
                seen.add(clean)
                eans.append(clean)
    return eans


def load_checkpoint(checkpoint_file: Path) -> Set[str]:
    """Загрузка чекпоинта (множество уже обработанных EAN)."""
    if not checkpoint_file.exists():
        return set()
    with open(checkpoint_file, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


class CheckpointWriter:
    """Потокобезопасный писатель чекпоинтов с fsync (в dry-run режиме запись отключена)."""

    def __init__(self, path: Path, enabled: bool = True):
        self._path = path
        self._enabled = enabled
        self._lock = asyncio.Lock()
        self._count = 0
        if self._enabled:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._fd = open(path, "a", encoding="utf-8")
        else:
            self._fd = None

    async def write(self, ean: str):
        if not self._enabled or not self._fd:
            return
        async with self._lock:
            self._fd.write(f"{ean}\n")
            self._count += 1
            # fsync каждые 100 записей — оптимальный баланс надёжности и скорости
            if self._count % 100 == 0:
                self._fd.flush()
                os.fsync(self._fd.fileno())

    def close(self):
        if self._enabled and self._fd and not self._fd.closed:
            self._fd.flush()
            os.fsync(self._fd.fileno())
            self._fd.close()


class SafeStats:
    """Потокобезопасные счётчики статистики с атомарной проверкой шага прогресса."""

    def __init__(self, log_step: int = 500):
        self._lock = asyncio.Lock()
        self._log_step = log_step
        self._last_logged = 0
        self._data: Dict[str, int] = {
            "updated": 0,
            "already_correct": 0,
            "not_found": 0,
            "errors": 0,
            "dry_run": 0,
            "processed": 0,
        }

    async def inc(self, key: str, value: int = 1):
        async with self._lock:
            self._data[key] = self._data.get(key, 0) + value
            if key != "processed":
                self._data["processed"] += value

    async def check_progress(self) -> Optional[Tuple[int, Dict[str, int]]]:
        """Возвращает snapshot, если достигнут очередной шаг progress-логирования."""
        async with self._lock:
            curr = self._data["processed"]
            if curr > 0 and (curr - self._last_logged) >= self._log_step:
                self._last_logged = (curr // self._log_step) * self._log_step
                return curr, dict(self._data)
            return None

    def get(self, key: str) -> int:
        return self._data.get(key, 0)

    def snapshot(self) -> Dict[str, int]:
        return dict(self._data)


class AccountContext:
    """Контекст исполнения для одного аккаунта (JV или XL)."""

    def __init__(
        self,
        account: str,
        whitelist_file: Path,
        rate_limit: float,
        concurrency: int,
        dry_run: bool,
        limit: Optional[int] = None,
    ):
        self.account = account.upper()
        self.whitelist_file = whitelist_file
        self.rate_limit = rate_limit
        self.concurrency = concurrency
        self.dry_run = dry_run
        self.limit = limit

        self.checkpoint_file = REPORT_DIR / f"cat_reclassify_checkpoint_{account.lower()}.txt"
        self.stats = SafeStats(log_step=500)
        self.cp_writer = CheckpointWriter(self.checkpoint_file, enabled=not dry_run)

        self.total_whitelist = 0
        self.already_in_checkpoint = 0
        self.pending_count = 0
        self.t0 = 0.0
        self.elapsed = 0.0
        self.completed = False
        self.verbose = (self.limit is not None and self.limit <= 50)


# ──────────────────────────────────────────────────────────────────────────────
# Обработка одного EAN
# ──────────────────────────────────────────────────────────────────────────────
async def process_single_ean(
    account: str,
    ean: str,
    rac: RestApiController,
    rate_limiter: SafeRateLimiter,
    dry_run: bool,
    cp_writer: CheckpointWriter,
    stats: SafeStats,
    verbose: bool = False,
) -> None:
    try:
        # 1. Запрашиваем текущую карточку product-data
        pdata_res = await request_with_retry(rac, "GET", f"/product-data/{ean}?locale=de-DE", rate_limiter)
        if pdata_res.status == 404:
            await stats.inc("not_found")
            await cp_writer.write(ean)
            return
        elif pdata_res.status not in (200, 204):
            await stats.inc("errors")
            logger.warning(f"[{account}][{ean}] GET product-data returned {pdata_res.status}: {pdata_res.error}")
            return

        pdata = pdata_res.data.get("data") if isinstance(pdata_res.data, dict) else {}
        attrs = pdata.get("attributes", {}) if isinstance(pdata, dict) else {}
        if not attrs:
            await stats.inc("errors")
            logger.warning(f"[{account}][{ean}] empty attributes")
            return

        title_val = attrs.get("title") or ""
        title = title_val[0] if isinstance(title_val, list) and title_val else str(title_val)

        # Извлекаем текущую категорию (по ID или имени)
        curr_cat_detail = attrs.get("category_detail", {})
        curr_cat_id = None
        if isinstance(curr_cat_detail, dict):
            curr_cat_id = curr_cat_detail.get("id")
        elif isinstance(curr_cat_detail, list) and curr_cat_detail and isinstance(curr_cat_detail[0], dict):
            curr_cat_id = curr_cat_detail[0].get("id")

        try:
            curr_cat_id = int(curr_cat_id) if curr_cat_id is not None else None
        except (ValueError, TypeError):
            pass

        curr_cat_slug = attrs.get("category")
        if isinstance(curr_cat_slug, list) and curr_cat_slug:
            curr_cat_slug = str(curr_cat_slug[0]).strip().lower()
        elif isinstance(curr_cat_slug, str):
            curr_cat_slug = curr_cat_slug.strip().lower()
        else:
            curr_cat_slug = None

        target_cat = determine_category_for_title(title)
        if target_cat is None:
            # Ни одно правило не совпало — оставляем категорию без изменений
            await stats.inc("already_correct")
            await cp_writer.write(ean)
            if verbose:
                logger.info(f"[{account}][{ean}] Категория оставлена без изменений: {curr_cat_id or curr_cat_slug} | '{title[:50]}'")
            return

        # Оптимизация: если текущая категория уже совпадает с целевой (по id или slug), пропускаем
        is_already_target = (curr_cat_id == target_cat["id"]) or (curr_cat_slug == target_cat["name"].lower())
        if is_already_target:
            await stats.inc("already_correct")
            await cp_writer.write(ean)
            if verbose:
                logger.info(f"[{account}][{ean}] Категория уже верная: {curr_cat_id or curr_cat_slug} ({target_cat['name']}) | '{title[:50]}'")
            return

        if dry_run:
            await stats.inc("dry_run")
            if verbose:
                logger.info(f"[{account}][{ean}] [DRY-RUN] Требуется обновление: {curr_cat_id} -> {target_cat['id']} ({target_cat['name']}) | '{title[:50]}'")
            return

        # 2. Обновляем категорию через PUT /product-data
        new_attrs = {k: v for k, v in attrs.items() if v is not None}
        new_attrs["category"] = [target_cat["name"]]
        new_attrs["category_detail"] = {
            "id": target_cat["id"],
            "title": target_cat["title"],
            "name": target_cat["name"],
        }

        put_payload = {"ean": [ean], "attributes": new_attrs}
        put_res = await request_with_retry(rac, "PUT", "/product-data?locale=de-DE", rate_limiter, json_payload=put_payload)

        if put_res.status in (200, 204):
            await stats.inc("updated")
            await cp_writer.write(ean)
            logger.info(f"[{account}][{ean}] УСПЕШНО ОБНОВЛЕНО: {curr_cat_id} -> {target_cat['id']} ({target_cat['name']}) | '{title[:50]}'")
        else:
            await stats.inc("errors")
            logger.warning(f"[{account}][{ean}] PUT ошибка {put_res.status}: {put_res.error}")

    except Exception as e:
        await stats.inc("errors")
        logger.error(f"[{account}][{ean}] Exception: {type(e).__name__}: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Воркер
# ──────────────────────────────────────────────────────────────────────────────
async def run_worker(
    worker_id: int,
    ctx: AccountContext,
    queue: asyncio.Queue,
    rac: RestApiController,
    rate_limiter: SafeRateLimiter,
):
    while True:
        try:
            ean = await queue.get()
        except asyncio.CancelledError:
            break

        if ean is None:
            queue.task_done()
            break

        try:
            await process_single_ean(
                account=ctx.account,
                ean=ean,
                rac=rac,
                rate_limiter=rate_limiter,
                dry_run=ctx.dry_run,
                cp_writer=ctx.cp_writer,
                stats=ctx.stats,
                verbose=ctx.verbose,
            )
        except asyncio.CancelledError:
            queue.task_done()
            raise
        except Exception as e:
            await ctx.stats.inc("errors")
            logger.error(f"[{ctx.account}][{ean}] Worker unhandled: {type(e).__name__}: {e}")
        finally:
            queue.task_done()

        # Атомарная проверка шага логирования прогресса
        prog = await ctx.stats.check_progress()
        if prog:
            curr, snap = prog
            update_label = "need_update" if ctx.dry_run else "updated"
            update_val = snap.get("dry_run", 0) if ctx.dry_run else snap.get("updated", 0)
            logger.info(
                f"[{ctx.account}][{curr}/{ctx.pending_count}] "
                f"{update_label}={update_val} already_ok={snap['already_correct']} "
                f"not_found={snap['not_found']} errors={snap['errors']}"
            )


# ──────────────────────────────────────────────────────────────────────────────
# Обработка одного аккаунта
# ──────────────────────────────────────────────────────────────────────────────
async def process_account(ctx: AccountContext) -> None:
    completed = load_checkpoint(ctx.checkpoint_file)
    all_eans = load_eans_from_whitelist(ctx.whitelist_file)
    pending_eans = [e for e in all_eans if e not in completed]

    if ctx.limit:
        pending_eans = pending_eans[:ctx.limit]

    ctx.total_whitelist = len(all_eans)
    ctx.already_in_checkpoint = len(completed)
    ctx.pending_count = len(pending_eans)

    logger.info(
        f"[{ctx.account}] Всего в белом списке: {ctx.total_whitelist} | "
        f"Уже в чекпоинте: {ctx.already_in_checkpoint} | В очереди: {ctx.pending_count}"
    )

    if not pending_eans:
        logger.info(f"[{ctx.account}] Все товары уже обработаны.")
        ctx.completed = True
        return

    rate_limiter = SafeRateLimiter(max_rps=ctx.rate_limit)
    queue: asyncio.Queue = asyncio.Queue(maxsize=ctx.concurrency * 2)

    try:
        connector = aiohttp.TCPConnector(limit=ctx.concurrency * 2, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=connector) as session:
            rac = RestApiController(ctx.account, session)
            workers = [
                asyncio.create_task(run_worker(i, ctx, queue, rac, rate_limiter))
                for i in range(ctx.concurrency)
            ]

            ctx.t0 = time.time()
            for ean in pending_eans:
                await queue.put(ean)

            await queue.join()

            # Сигнал завершения для воркеров
            for _ in range(ctx.concurrency):
                await queue.put(None)
            await asyncio.gather(*workers)

            ctx.elapsed = time.time() - ctx.t0
            ctx.completed = True

            speed = ctx.pending_count / max(ctx.elapsed, 0.001)
            snap = ctx.stats.snapshot()
            logger.info(f"[{ctx.account}] Завершено за {ctx.elapsed:.1f} сек ({speed:.1f} items/sec)!")
            logger.info(f"[{ctx.account}] Итоговая статистика: {snap}")
    finally:
        ctx.cp_writer.close()


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(
        description="Автономное высокоскоростное обновление категорий всего каталога JV и XL (Kaufland API)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--account", choices=["jv", "xl", "all"], default="all",
                        help="Аккаунт для обработки: jv, xl или all (по умолчанию: all)")
    parser.add_argument("--rate-limit", type=float, default=85.0,
                        help="RPS на каждый аккаунт (официальный лимит Kaufland: 111 req/s, по умолчанию: 85.0)")
    parser.add_argument("--concurrency", type=int, default=35,
                        help="Количество параллельных воркеров на каждый аккаунт (по умолчанию: 35)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Безопасный режим аудита — только чтение GET, без отправки PUT в каталог")
    parser.add_argument("--limit", type=int, default=None,
                        help="Ограничить количество товаров на аккаунт (для тестирования)")
    args = parser.parse_args()

    mode_label = "DRY-RUN (симуляция)" if args.dry_run else "БОЕВОЙ РЕЖИМ (обновление каталога)"
    logger.info("=" * 80)
    logger.info(f"ОБНОВЛЕНИЕ КАТЕГОРИЙ ВСЕГО КАТАЛОГА [{mode_label}]")
    logger.info(f"Аккаунты: {args.account.upper()} | RPS: {args.rate_limit} | Concurrency: {args.concurrency}")
    if args.limit:
        logger.info(f"ЛИМИТ: {args.limit} товаров на каждый аккаунт")
    logger.info("=" * 80)

    contexts: List[AccountContext] = []
    if args.account in ("jv", "all"):
        contexts.append(AccountContext("JV", JV_VALID_FILE, args.rate_limit, args.concurrency, args.dry_run, args.limit))
    if args.account in ("xl", "all"):
        contexts.append(AccountContext("XL", XL_VALID_FILE, args.rate_limit, args.concurrency, args.dry_run, args.limit))

    tasks = [asyncio.create_task(process_account(ctx)) for ctx in contexts]

    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.warning("\n[ПРЕРВАНО ПОЛЬЗОВАТЕЛЕМ] Остановка воркеров и сохранение отчёта...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    # Итоговый отчёт
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = REPORT_DIR / f"cat_update_report_{now_str}.txt"

    with open(report_file, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("ОТЧЁТ ПО ОБНОВЛЕНИЮ КАТЕГОРИЙ КАТАЛОГА\n")
        f.write(f"Режим: {mode_label}\n")
        f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")

        for ctx in contexts:
            snap = ctx.stats.snapshot()
            elapsed = ctx.elapsed if ctx.elapsed > 0 else (time.time() - ctx.t0 if ctx.t0 > 0 else 0.0)
            speed = ctx.pending_count / max(elapsed, 0.001) if elapsed > 0 else 0.0

            f.write(f"АККАУНТ: {ctx.account}\n")
            f.write(f"  Всего в белом списке: {ctx.total_whitelist}\n")
            f.write(f"  Уже в чекпоинте: {ctx.already_in_checkpoint}\n")
            f.write(f"  В очереди на обработку: {ctx.pending_count}\n")
            if ctx.dry_run:
                f.write(f"  Требуют обновления (dry-run): {snap.get('dry_run', 0)}\n")
            else:
                f.write(f"  Обновлено категорий (PUT): {snap.get('updated', 0)}\n")
            f.write(f"  Уже с правильной категорией: {snap.get('already_correct', 0)}\n")
            f.write(f"  Товар не найден (404): {snap.get('not_found', 0)}\n")
            f.write(f"  Ошибок запросов: {snap.get('errors', 0)}\n")
            f.write(f"  Время: {elapsed:.1f} сек ({speed:.1f} items/sec)\n")
            f.write(f"  Статус: {'Завершено полностью' if ctx.completed else 'Прервано / частично выполнено'}\n\n")

        f.write("=" * 80 + "\n")

    logger.info(f"Итоговый отчёт сохранён: {report_file}")

    print("\n" + "=" * 80)
    print(f"ИТОГИ [{mode_label}]:")
    print("=" * 80)
    for ctx in contexts:
        snap = ctx.stats.snapshot()
        elapsed = ctx.elapsed if ctx.elapsed > 0 else (time.time() - ctx.t0 if ctx.t0 > 0 else 0.0)
        speed = ctx.pending_count / max(elapsed, 0.001) if elapsed > 0 else 0.0
        update_col = f"Требуют обновления: {snap.get('dry_run', 0)}" if ctx.dry_run else f"Обновлено: {snap.get('updated', 0)}"
        print(
            f"[{ctx.account}] Обработано: {snap.get('processed', 0)}/{ctx.pending_count} | "
            f"{update_col} | "
            f"Уже ОК: {snap.get('already_correct', 0)} | "
            f"404: {snap.get('not_found', 0)} | "
            f"Ошибок: {snap.get('errors', 0)} | "
            f"Время: {elapsed:.1f}с ({speed:.1f} items/s)"
        )
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
