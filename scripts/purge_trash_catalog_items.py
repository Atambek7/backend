#!/usr/bin/env python3
"""
Скрипт полной надежной зачистки мусорных товаров (Units + Product Data) на Kaufland.

Реализует строгую схему надежности:
1. Чтение EAN из файла или аргументов, валидация формата, проверка checksum GTIN (опционально strict),
   дедупликация и детальное логирование.
2. Архитектурно правильный порядок удаления и строгий откат:
   - Сначала поиск и удаление ВСЕХ Units (offers) по всем витринам (с пагинацией).
   - Проверка отсутствия Units: если хоть один оффер остался или проверка завершилась ошибкой,
     удаление Product Data категорически блокируется (status PARTIAL/FAILED), предотвращая появление офферов-призраков.
   - Затем удаление Product Data по всем 9 локалям (только если Units строго подтверждены как 0).
   - Проверка полного отсутствия Product Data (404 на всех локалях).
   - Только после подтверждения 100% чистоты выставляется SUCCESS и запись в чекпоинт.
3. Полная изоляция и гибкость чекпоинтов:
   - Имя чекпоинта по умолчанию привязывается к конкретному входному файлу (input stem) и аккаунту.
   - Флаги --checkpoint и --failed-file поддерживаются как для пакетных файлов, так и для одиночных/перечисленных --ean.
   - Файл failed EAN и JSON деталей ошибок имеют строго унифицированные имена.
   - Поддержка флага --force для принудительного повторного прогона поверх чекпоинта.
4. Абсолютная защита от падения asyncio.gather:
   - Вся логика внутри worker() защищена глобальным try/except с фиксацией в FAILED и failed_details.
   - asyncio.gather(*tasks, return_exceptions=True) гарантирует, что ни один сбой не прервет весь запуск.
5. Честная статистика:
   - Разделение реально удаленных юнитов (200/204) и уже отсутствующих (404).
   - То же самое для Product Data.
6. Сохранение реального HTTP-статуса при исчерпании попыток retry в request_with_retry.
7. Прозрачное предупреждение, если задан неполный перечень витрин через --storefronts.
8. Поддержка безопасного режима --dry-run.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field
import json
import logging
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Any, List, Set, Dict, Optional, Tuple

import aiohttp

BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BACKEND_DIR))

import config
from main_api.src.controller.rest_api_controller import RestApiController

LOCALES = ["de-DE", "cs-CZ", "sk-SK", "de-AT", "pl-PL", "fr-FR", "it-IT", "es-ES", "nl-NL"]
STOREFRONTS = ["de", "cz", "sk", "at", "pl", "fr", "it", "es", "nl"]

logger = logging.getLogger("purge_trash")


class SafeRateLimiter:
    """Потокобезопасный ограничитель частоты запросов к API Kaufland."""
    def __init__(self, max_rps: float = 14.0):
        self.interval = 1.0 / max(0.1, max_rps)
        self.lock = asyncio.Lock()
        self.last_time = 0.0

    async def acquire(self):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_time
            if elapsed < self.interval:
                await asyncio.sleep(self.interval - elapsed)
            self.last_time = time.monotonic()


@dataclass
class ApiResponse:
    status: int
    data: Any = None
    error: str = ""


async def request_with_retry(
    rac: RestApiController,
    method: str,
    endpoint: str,
    rate_limiter: SafeRateLimiter,
    max_retries: int = 5,
    base_delay: float = 1.0,
    json_payload: Any = None,
) -> ApiResponse:
    """
    Централизованный механизм запросов к Kaufland Seller API.
    Гарантирует прохождение через rate limiter и автоматический retry с exponential backoff
    для HTTP 429, 500-599, сетевых сбоев и таймаутов.
    На каждый retry генерирует свежие заголовки с актуальной HMAC-подписью.
    Если попытка последняя (attempt == max_retries), sleep не выполняется.
    Сохраняет настоящий HTTP-статус при исчерпании попыток.
    """
    endpoint_clean = endpoint if endpoint.startswith("/") else f"/{endpoint}"
    full_url = rac.base_api_url + endpoint_clean
    serialized_body = rac._serialize_body(json_payload) if json_payload is not None else None
    has_payload = bool(serialized_body) or method.upper() in {"POST", "PUT", "PATCH"}

    last_status = 0
    last_error = ""

    for attempt in range(1, max_retries + 1):
        await rate_limiter.acquire()
        headers = rac._get_header(
            method,
            full_url,
            serialized_body,
            has_payload=has_payload,
        )
        kwargs: dict[str, Any] = {
            "headers": headers,
            "timeout": aiohttp.ClientTimeout(total=30.0),
        }
        if serialized_body:
            kwargs["data"] = serialized_body

        try:
            async with rac.session.request(method, full_url, **kwargs) as response:
                status = response.status
                last_status = status
                text = await response.text()

                # Успешный результат
                if status in (200, 204):
                    if status == 204 or not text.strip():
                        return ApiResponse(status=status, data=None)
                    try:
                        return ApiResponse(status=status, data=json.loads(text))
                    except json.JSONDecodeError:
                        return ApiResponse(status=status, data=text)

                # 404 Not Found (нормальное состояние при удалении / отсутствии)
                if status == 404:
                    return ApiResponse(status=404, data=None)

                # Ошибки валидации со стороны клиента - не ретраим, фиксируем ошибку
                if status in (400, 422):
                    return ApiResponse(status=status, data=None, error=f"HTTP {status}: {text[:300]}")

                # 429 Rate Limit - повтор с экспоненциальной задержкой (только если есть след. попытка)
                if status == 429:
                    last_error = f"HTTP 429: Rate limited"
                    if attempt < max_retries:
                        delay = min(30.0, (base_delay * (2 ** (attempt - 1))) + random.uniform(0.5, 1.5))
                        logger.warning(
                            f"[{method} {endpoint_clean}] 429 Rate limited. Attempt {attempt}/{max_retries}, sleeping {delay:.1f}s"
                        )
                        await asyncio.sleep(delay)
                    continue

                # 5xx Server Error - повтор с экспоненциальной задержкой (только если есть след. попытка)
                if 500 <= status <= 599:
                    last_error = f"HTTP {status}: {text[:200]}"
                    if attempt < max_retries:
                        delay = min(30.0, (base_delay * (2 ** (attempt - 1))) + random.uniform(0.5, 1.5))
                        logger.warning(
                            f"[{method} {endpoint_clean}] Server Error {status}. Attempt {attempt}/{max_retries}, sleeping {delay:.1f}s"
                        )
                        await asyncio.sleep(delay)
                    continue

                # Любые другие неожиданные статусы
                return ApiResponse(status=status, data=None, error=f"HTTP {status}: {text[:300]}")

        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < max_retries:
                delay = min(30.0, (base_delay * (2 ** (attempt - 1))) + random.uniform(0.5, 1.5))
                logger.warning(
                    f"[{method} {endpoint_clean}] Network/Timeout ({type(exc).__name__}). Attempt {attempt}/{max_retries}, sleeping {delay:.1f}s"
                )
                await asyncio.sleep(delay)
        except Exception as exc:
            return ApiResponse(status=0, data=None, error=f"Unexpected exception: {exc}")

    return ApiResponse(
        status=last_status,
        data=None,
        error=f"Exhausted {max_retries} attempts for {method} {endpoint_clean}: {last_error or f'HTTP {last_status}'}",
    )


async def fetch_all_units_for_ean_storefront(
    rac: RestApiController,
    ean: str,
    storefront: str,
    rate_limiter: SafeRateLimiter,
) -> tuple[list[dict], list[str]]:
    """
    Получает ВСЕ units для EAN на указанной витрине с поддержкой пагинации:
    1. Сначала ищет по id_offer={ean}.
    2. Если ничего не найдено, ищет по ean={ean}.
    Возвращает список юнитов и список возникших ошибок.
    """
    errors: list[str] = []
    all_units: list[dict] = []
    seen_unit_ids: set[int] = set()

    # 1. Поиск по id_offer
    limit = 100
    offset = 0
    while True:
        resp = await request_with_retry(
            rac,
            "GET",
            f"/units?storefront={storefront}&id_offer={ean}&limit={limit}&offset={offset}",
            rate_limiter,
        )
        if resp.status == 200 and isinstance(resp.data, dict):
            chunk = resp.data.get("data", [])
            for u in chunk:
                uid = u.get("id_unit")
                if uid and uid not in seen_unit_ids:
                    seen_unit_ids.add(uid)
                    all_units.append(u)
            if len(chunk) < limit:
                break
            offset += len(chunk)
        elif resp.status == 404 or (resp.status == 400 and "storefront-not-configured" in str(resp.error)):
            break
        else:
            if resp.error:
                errors.append(f"[{storefront}] GET units by id_offer failed: {resp.error}")
            break

    # 2. Если по id_offer пусто и не было ошибок, проверяем по ean={ean}
    if not all_units and not errors:
        offset = 0
        while True:
            resp = await request_with_retry(
                rac,
                "GET",
                f"/units?storefront={storefront}&ean={ean}&limit={limit}&offset={offset}",
                rate_limiter,
            )
            if resp.status == 200 and isinstance(resp.data, dict):
                chunk = resp.data.get("data", [])
                for u in chunk:
                    uid = u.get("id_unit")
                    if uid and uid not in seen_unit_ids:
                        seen_unit_ids.add(uid)
                        all_units.append(u)
                if len(chunk) < limit:
                    break
                offset += len(chunk)
            elif resp.status == 404 or (resp.status == 400 and "storefront-not-configured" in str(resp.error)):
                break
            else:
                if resp.error:
                    errors.append(f"[{storefront}] GET units by ean failed: {resp.error}")
                break

    return all_units, errors


@dataclass
class PurgeItemResult:
    ean: str
    status: str  # "SUCCESS", "PARTIAL", "FAILED", "DRY_RUN"
    units_found: int = 0
    pdata_found: int = 0
    units_deleted: int = 0
    units_already_clean: int = 0
    pdata_deleted: int = 0
    pdata_already_clean: int = 0
    errors: list[str] = field(default_factory=list)
    verification_clean: bool = False


async def purge_single_ean(
    rac: RestApiController,
    ean: str,
    account: str,
    storefronts: list[str],
    locales: list[str],
    rate_limiter: SafeRateLimiter,
    dry_run: bool = False,
) -> PurgeItemResult:
    """
    Полный цикл зачистки и верификации одного EAN:
    
    Архитектурная схема:
    1. Найти ВСЕ Units по всем витринам.
    2. Удалить ВСЕ Units (DELETE /units/{id_unit}).
    3. Проверить отсутствие Units (Verification Units).
    4. Удалить Product Data по всем локалям (DELETE /product-data/{ean}).
    5. Проверить отсутствие Product Data (Verification Product Data -> 404).
    6. Только после подтверждения 100% чистоты выставляется SUCCESS.
    """
    res = PurgeItemResult(ean=ean, status="FAILED")

    if dry_run:
        # Режим DRY-RUN: сканируем без отправки DELETE
        total_units_found = 0
        units_sf_found = []
        for sf in storefronts:
            units, sf_errors = await fetch_all_units_for_ean_storefront(rac, ean, sf, rate_limiter)
            if sf_errors:
                res.errors.extend(sf_errors)
            if units:
                total_units_found += len(units)
                units_sf_found.append(f"{sf}({len(units)})")

        pdata_locales_found = []
        for loc in locales:
            chk = await request_with_retry(rac, "GET", f"/product-data/{ean}?locale={loc}", rate_limiter)
            if chk.status == 200:
                pdata_locales_found.append(loc)
            elif chk.status != 404 and chk.error:
                res.errors.append(f"[{loc}] DRY-RUN check product-data failed: {chk.error}")

        res.units_found = total_units_found
        res.pdata_found = len(pdata_locales_found)
        res.status = "DRY_RUN"
        res.verification_clean = (len(pdata_locales_found) == 0 and total_units_found == 0)
        logger.info(
            f"[DRY-RUN][{ean}] Scan: {res.units_found} units ({','.join(units_sf_found) or 'none'}), "
            f"{res.pdata_found} pdata locales ({','.join(pdata_locales_found) or 'none'})"
        )
        return res

    # === РЕЖИМ РЕАЛЬНОГО УДАЛЕНИЯ (EXECUTE) ===

    # 1. ШАГ 1: Поиск и удаление Units для всех витрин
    for sf in storefronts:
        units, fetch_errors = await fetch_all_units_for_ean_storefront(rac, ean, sf, rate_limiter)
        if fetch_errors:
            res.errors.extend(fetch_errors)

        if not units:
            res.units_already_clean += 1
            continue

        for u in units:
            unit_id = u.get("id_unit")
            if not unit_id:
                continue
            del_u_resp = await request_with_retry(
                rac, "DELETE", f"/units/{unit_id}?storefront={sf}", rate_limiter
            )
            if del_u_resp.status in (200, 204):
                res.units_deleted += 1
            elif del_u_resp.status == 404:
                res.units_already_clean += 1
            else:
                res.errors.append(f"[{sf}] DELETE unit {unit_id} failed: {del_u_resp.error}")

    # 2. ШАГ 2: Проверка отсутствия Units перед удалением карточки
    units_remain = False
    unit_check_errors = False
    for sf in storefronts:
        rem_units, v_err = await fetch_all_units_for_ean_storefront(rac, ean, sf, rate_limiter)
        if v_err:
            res.errors.extend(v_err)
            unit_check_errors = True
        if rem_units:
            units_remain = True
            res.errors.append(f"[{sf}] Unit check failed: {len(rem_units)} units still exist")

    # КРИТИЧЕСКАЯ ОСТАНОВКА: Если Units не подтверждены как 0 (остались офферы ИЛИ
    # проверка завершилась ошибками/таймаутами), удаление Product Data строго ЗАПРЕЩЕНО!
    if units_remain or unit_check_errors or len(res.errors) > 0:
        res.status = "PARTIAL" if res.units_deleted > 0 else "FAILED"
        res.verification_clean = False
        logger.warning(
            f"[{account.upper()}][{ean}] Остановка: удаление Product Data отменено "
            f"(units_remain={units_remain}, check_errors={unit_check_errors}, total_errors={len(res.errors)})"
        )
        return res

    # 3. ШАГ 3: Удаление Product Data для всех локалей (только если Units строго = 0)
    for loc in locales:
        del_resp = await request_with_retry(rac, "DELETE", f"/product-data/{ean}?locale={loc}", rate_limiter)
        if del_resp.status in (200, 204):
            res.pdata_deleted += 1
        elif del_resp.status == 404:
            res.pdata_already_clean += 1
        else:
            res.errors.append(f"[{loc}] DELETE product-data failed: {del_resp.error}")

    # 4. ШАГ 4: Финальная верификация Product Data (GET должен вернуть 404)
    pdata_remain = False
    pdata_check_errors = False
    for loc in locales:
        v_pdata = await request_with_retry(rac, "GET", f"/product-data/{ean}?locale={loc}", rate_limiter)
        if v_pdata.status == 200:
            pdata_remain = True
            res.errors.append(f"[{loc}] Product data verification failed: data still exists")
        elif v_pdata.status != 404:
            pdata_check_errors = True
            if v_pdata.error:
                res.errors.append(f"[{loc}] Verification error checking product-data: {v_pdata.error}")

    # 5. Итоговый статус обработки
    if not pdata_remain and not pdata_check_errors and len(res.errors) == 0:
        res.status = "SUCCESS"
        res.verification_clean = True
    elif (res.units_deleted > 0 or res.pdata_deleted > 0):
        res.status = "PARTIAL"
        res.verification_clean = False
    else:
        res.status = "FAILED"
        res.verification_clean = False

    return res


def validate_gtin_checksum(ean: str) -> bool:
    """
    Проверяет контрольную сумму GTIN-8, GTIN-12, GTIN-13, GTIN-14 по стандарту GS1 (mod 10).
    """
    if not ean.isdigit() or len(ean) not in (8, 12, 13, 14):
        return False
    digits = [int(c) for c in ean]
    checksum = digits[-1]
    payload = digits[:-1]
    # Чередование весов 3 и 1 справа налево перед контрольной цифрой
    weights = [3, 1] * (len(payload) // 2 + 1)
    weights = weights[:len(payload)]
    weights.reverse()
    s = sum(d * w for d, w in zip(payload, weights))
    calculated = (10 - (s % 10)) % 10
    return calculated == checksum


def normalize_and_validate_eans(
    raw_lines: list[str],
    strict_checksum: bool = False,
) -> tuple[list[str], int, int]:
    """
    Нормализует список EAN: strip(), проверка формата (8-14 цифр),
    проверка контрольной суммы (strict/warning), дедупликация с сохранением исходного порядка.
    """
    seen: set[str] = set()
    valid_eans: list[str] = []
    invalid_count = 0

    for line in raw_lines:
        s = line.strip()
        if not s or s.startswith("#") or s.startswith("-"):
            continue

        m = re.search(r"\b(\d{13})\b", s)
        if m:
            ean_clean = m.group(1)
        elif re.match(r"^\d{8,14}$", s):
            ean_clean = s
        else:
            invalid_count += 1
            logger.warning(f"Пропущена некорректная строка EAN: '{s}'")
            continue

        if strict_checksum and not validate_gtin_checksum(ean_clean):
            invalid_count += 1
            logger.warning(f"Пропущен EAN с невалидной контрольной суммой GS1 (strict mode): '{ean_clean}'")
            continue
        elif not validate_gtin_checksum(ean_clean):
            logger.debug(f"EAN '{ean_clean}' имеет нестандартную контрольную сумму, но включен для очистки.")

        if ean_clean not in seen:
            seen.add(ean_clean)
            valid_eans.append(ean_clean)

    return valid_eans, len(raw_lines), invalid_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Полная надежная зачистка мусорных товаров на Kaufland")
    parser.add_argument(
        "--ean",
        default="",
        help="Один конкретный EAN или список через запятую (например: 4069943370027,4069943370028)",
    )
    parser.add_argument(
        "--input",
        default="reports/eans_to_purge_target.txt",
        help="Файл со списком EAN (по умолчанию: reports/eans_to_purge_target.txt)",
    )
    parser.add_argument(
        "--account",
        default="jv",
        choices=["jv", "xl"],
        help="Аккаунт Kaufland (jv или xl, по умолчанию: jv)",
    )
    parser.add_argument(
        "--storefronts",
        default="",
        help=f"Список витрин через запятую (по умолчанию: {','.join(STOREFRONTS)})",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=35,
        help="Количество параллельных воркеров EAN (по умолчанию: 35)",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=60.0,
        help="Максимальное число запросов в секунду (официальный лимит Kaufland: 111 req/s, безопасный быстрый дефолт: 60.0)",
    )
    parser.add_argument(
        "--checkpoint",
        default="",
        help="Файл чекпоинта успешных EAN (по умолчанию: completed_purged_{input_stem}_{account}.txt)",
    )
    parser.add_argument(
        "--failed-file",
        default="",
        help="Файл для проблемных EAN (по умолчанию: failed_purged_{input_stem}_{account}.txt)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Игнорировать существующий чекпоинт и принудительно перепроверить все EAN",
    )
    parser.add_argument(
        "--strict-checksum",
        action="store_true",
        help="Строго отбрасывать EAN с неверной контрольной суммой GS1",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Режим проверки и сканирования без фактического удаления",
    )
    return parser.parse_args()


def resolve_file_path(path_str: str) -> Path:
    """
    Нормализует путь к файлу чекпоинта или ошибок.
    Если путь абсолютный, возвращает как есть.
    Если относительный, привязывает к директории backend/ (устраняя возможный префикс 'backend/').
    """
    p = Path(path_str)
    if p.is_absolute():
        return p
    clean_str = str(path_str)
    if clean_str.startswith("backend/"):
        clean_str = clean_str[len("backend/"):]
    return BACKEND_DIR / clean_str


async def main():
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    account = args.account.lower()
    dry_run = args.dry_run
    mode_str = "DRY-RUN (БЕЗ УДАЛЕНИЯ)" if dry_run else "EXECUTE (РЕАЛЬНОЕ УДАЛЕНИЕ)"

    # Валидация storefronts против STOREFRONTS
    valid_storefronts_set = set(STOREFRONTS)
    is_partial_scope = False
    if args.storefronts:
        target_storefronts = [s.strip().lower() for s in args.storefronts.split(",") if s.strip()]
        invalid_sfs = [s for s in target_storefronts if s not in valid_storefronts_set]
        if invalid_sfs:
            logger.error(
                f"Указаны недопустимые витрины: {', '.join(invalid_sfs)}. "
                f"Допустимые витрины: {', '.join(STOREFRONTS)}"
            )
            sys.exit(1)
        if len(target_storefronts) < len(STOREFRONTS):
            is_partial_scope = True
            logger.warning(
                f"ВНИМАНИЕ: Задан ограниченный список витрин ({', '.join(target_storefronts)}). "
                f"Очистка не гарантирует полное удаление на остальных {len(STOREFRONTS) - len(target_storefronts)} витринах."
            )
    else:
        target_storefronts = config.get_account_storefronts(account)

    # 1. Чтение и нормализация входных EAN
    if args.ean:
        raw_inputs = [e.strip() for e in args.ean.split(",") if e.strip()]
        valid_eans, total_raw, invalid_count = normalize_and_validate_eans(
            raw_inputs, strict_checksum=args.strict_checksum
        )
        checkpoint_file: Optional[Path] = resolve_file_path(args.checkpoint) if args.checkpoint else None
        failed_file: Optional[Path] = resolve_file_path(args.failed_file) if args.failed_file else None
        failed_json_file: Optional[Path] = failed_file.with_suffix(".json") if failed_file else None

        completed_eans: Set[str] = set()
        if checkpoint_file and checkpoint_file.exists() and not args.force:
            for line in checkpoint_file.read_text(encoding="utf-8").splitlines():
                val = line.strip()
                if val:
                    completed_eans.add(val)

        failed_eans: Set[str] = set()
        if failed_file and failed_file.exists():
            for line in failed_file.read_text(encoding="utf-8").splitlines():
                val = line.strip()
                if val:
                    failed_eans.add(val)
    else:
        p = Path(args.input)
        if p.is_absolute() and p.exists():
            input_path = p
        elif (BACKEND_DIR / p).exists():
            input_path = BACKEND_DIR / p
        elif p.exists():
            input_path = p.resolve()
        else:
            logger.error(f"Файл со списком EAN не найден: {args.input} (проверено в {BACKEND_DIR / p})")
            sys.exit(1)

        raw_inputs = input_path.read_text(encoding="utf-8").splitlines()
        valid_eans, total_raw, invalid_count = normalize_and_validate_eans(
            raw_inputs, strict_checksum=args.strict_checksum
        )

        # Выделенный чекпоинт, изолированный под конкретный входной файл и аккаунт!
        input_stem = input_path.stem
        if args.checkpoint:
            checkpoint_file = resolve_file_path(args.checkpoint)
        else:
            checkpoint_file = BACKEND_DIR / f"completed_purged_{input_stem}_{account}.txt"

        if args.failed_file:
            failed_file = resolve_file_path(args.failed_file)
        else:
            failed_file = BACKEND_DIR / f"failed_purged_{input_stem}_{account}.txt"

        # Унифицированное имя JSON для деталей ошибок
        failed_json_file = failed_file.with_suffix(".json")

        completed_eans = set()
        if checkpoint_file.exists() and not args.force:
            for line in checkpoint_file.read_text(encoding="utf-8").splitlines():
                val = line.strip()
                if val:
                    completed_eans.add(val)

        failed_eans = set()
        if failed_file.exists():
            for line in failed_file.read_text(encoding="utf-8").splitlines():
                val = line.strip()
                if val:
                    failed_eans.add(val)

    completed_in_input = [e for e in valid_eans if e in completed_eans]
    pending_eans = [e for e in valid_eans if e not in completed_eans]

    logger.info("=" * 65)
    logger.info(" Kaufland Purge Utility (High-Reliability Engine v2)")
    logger.info(f" Режим:         {mode_str}")
    logger.info(f" Аккаунт:       {account.upper()}")
    logger.info(f" Витрины:       {', '.join(target_storefronts)} {'(ЧАСТИЧНЫЙ ОХВАТ)' if is_partial_scope else '(ПОЛНЫЙ ОХВАТ)'}")
    logger.info(f" Локали:        {', '.join(LOCALES)}")
    logger.info(f" Concurrency:   {args.concurrency}")
    logger.info(f" Rate Limit:    {args.rate_limit} req/s")
    if checkpoint_file:
        logger.info(f" Чекпоинт:      {checkpoint_file.name} {'(ИГНОРИРУЕТСЯ --force)' if args.force else ''}")
    else:
        logger.info(" Чекпоинт:      Отключен (разовый запуск без сохранения)")
    if failed_file:
        logger.info(f" Failed TXT:    {failed_file.name}")
        logger.info(f" Failed JSON:   {failed_json_file.name if failed_json_file else 'None'}")
    else:
        logger.info(" Failed TXT:    Отключен")
    logger.info("-" * 65)
    logger.info(f" Прочитано строк из источника:   {total_raw:,}")
    logger.info(f" Отброшено невалидных строк:     {invalid_count:,}")
    logger.info(f" Уникальных валидных EAN:        {len(valid_eans):,}")
    logger.info(f" Уже в чекпоинте (пропущено):    {len(completed_in_input):,}")
    logger.info(f" Осталось к обработке:           {len(pending_eans):,}")
    logger.info("=" * 65)

    if not pending_eans:
        logger.info("Все товары уже обработаны! Завершение работы.")
        return

    rate_limiter = SafeRateLimiter(max_rps=args.rate_limit)
    sem = asyncio.Semaphore(args.concurrency)
    save_lock = asyncio.Lock()

    stats = {
        "SUCCESS": 0,
        "PARTIAL": 0,
        "FAILED": 0,
        "DRY_RUN": 0,
    }
    total_pdata_found = 0
    total_units_found = 0
    total_pdata_deleted = 0
    total_pdata_already_clean = 0
    total_units_deleted = 0
    total_units_already_clean = 0
    failed_details: list[dict] = []
    processed_count = 0
    t0 = time.time()

    async with aiohttp.ClientSession() as session:
        rac = RestApiController(account, session)

        async def worker(ean: str):
            nonlocal processed_count, total_pdata_found, total_units_found
            nonlocal total_pdata_deleted, total_pdata_already_clean, total_units_deleted, total_units_already_clean
            try:
                async with sem:
                    item_res = await purge_single_ean(
                        rac=rac,
                        ean=ean,
                        account=account,
                        storefronts=target_storefronts,
                        locales=LOCALES,
                        rate_limiter=rate_limiter,
                        dry_run=dry_run,
                    )

                    async with save_lock:
                        processed_count += 1
                        stats[item_res.status] += 1
                        total_pdata_found += item_res.pdata_found
                        total_units_found += item_res.units_found
                        total_pdata_deleted += item_res.pdata_deleted
                        total_pdata_already_clean += item_res.pdata_already_clean
                        total_units_deleted += item_res.units_deleted
                        total_units_already_clean += item_res.units_already_clean

                        # Логирование проблемных EAN
                        if item_res.status in ("PARTIAL", "FAILED"):
                            for err in item_res.errors:
                                logger.warning(f"[{item_res.status}][{ean}] {err}")

                        # Фиксация в чекпоинты (только если не dry-run)
                        if not dry_run:
                            if item_res.status == "SUCCESS":
                                if checkpoint_file:
                                    completed_eans.add(ean)
                                    with open(checkpoint_file, "a", encoding="utf-8") as f:
                                        f.write(f"{ean}\n")
                                        f.flush()
                                        os.fsync(f.fileno())
                                # Если ранее товар был в списке failed, убираем его
                                if ean in failed_eans:
                                    failed_eans.remove(ean)

                            elif item_res.status in ("PARTIAL", "FAILED"):
                                if failed_file and ean not in failed_eans:
                                    failed_eans.add(ean)
                                    with open(failed_file, "a", encoding="utf-8") as f:
                                        f.write(f"{ean}\n")
                                        f.flush()
                                        os.fsync(f.fileno())
                                failed_details.append({
                                    "ean": ean,
                                    "status": item_res.status,
                                    "errors": item_res.errors,
                                })

                        # Подробный вывод прогресса
                        if processed_count <= 10 or processed_count % 10 == 0 or processed_count == len(pending_eans):
                            elapsed = time.time() - t0
                            speed = processed_count / elapsed if elapsed > 0 else 0
                            remaining = len(pending_eans) - processed_count
                            eta_sec = int(remaining / speed) if speed > 0 else 0
                            eta_str = f"{eta_sec // 60}m {eta_sec % 60}s"
                            pct = (processed_count / len(pending_eans)) * 100
                            logger.info(
                                f"[{account.upper()}] {processed_count}/{len(pending_eans)} EAN ({pct:.1f}%) | "
                                f"SUCCESS: {stats['SUCCESS']} | PARTIAL: {stats['PARTIAL']} | FAILED: {stats['FAILED']} | "
                                f"Speed: {speed:.1f} EAN/s | ETA: {eta_str}"
                            )
            except Exception as unhandled_exc:
                logger.error(f"[{account.upper()}][{ean}] CRITICAL unhandled worker exception: {unhandled_exc}", exc_info=True)
                async with save_lock:
                    processed_count += 1
                    stats["FAILED"] += 1
                    if not dry_run:
                        if failed_file and ean not in failed_eans:
                            failed_eans.add(ean)
                            with open(failed_file, "a", encoding="utf-8") as f:
                                f.write(f"{ean}\n")
                                f.flush()
                                os.fsync(f.fileno())
                        failed_details.append({
                            "ean": ean,
                            "status": "FAILED",
                            "errors": [f"CRITICAL unhandled worker exception: {unhandled_exc}"],
                        })

        tasks = [worker(e) for e in pending_eans]
        await asyncio.gather(*tasks, return_exceptions=True)

    # Синхронизация и очистка файла failed (перезапись уникального списка без дублей и решенных EAN)
    if failed_file and not dry_run:
        with open(failed_file, "w", encoding="utf-8") as f:
            for e in sorted(failed_eans):
                f.write(f"{e}\n")

    # Сохраняем расширенные данные по ошибкам в JSON (или удаляем, если все ошибки закрыты)
    if not dry_run and failed_json_file:
        if failed_details:
            with open(failed_json_file, "w", encoding="utf-8") as f:
                json.dump(failed_details, f, ensure_ascii=False, indent=2)
        elif failed_json_file.exists():
            failed_json_file.unlink(missing_ok=True)

    logger.info("=" * 65)
    logger.info(f"[{account.upper()}] ИТОГОВЫЙ ОТЧЕТ ЗАЧИСТКИ ({mode_str})")
    logger.info("=" * 65)
    logger.info(f" Всего EAN к обработке:          {len(pending_eans):,}")
    logger.info(f" Успешно очищено (SUCCESS):      {stats['SUCCESS']:,}")
    logger.info(f" Не полностью очищено (PARTIAL): {stats['PARTIAL']:,}")
    logger.info(f" Ошибок (FAILED):                {stats['FAILED']:,}")
    if dry_run:
        logger.info(f" Просканировано (DRY_RUN):       {stats['DRY_RUN']:,}")
    logger.info("-" * 65)
    if dry_run:
        logger.info(f" Найдено Units (к удалению):        {total_units_found:,}")
        logger.info(f" Найдено Product Data (к удалению): {total_pdata_found:,}")
    else:
        logger.info(f" Всего удалено Units:               {total_units_deleted:,} (уже чисто: {total_units_already_clean:,})")
        logger.info(f" Всего удалено Product Data:        {total_pdata_deleted:,} (уже чисто: {total_pdata_already_clean:,})")
    if checkpoint_file:
        logger.info(f" Файл успешных (чекпоинт):          {checkpoint_file}")
    if failed_file and not dry_run:
        if failed_eans:
            logger.info(f" Файл проблемных EAN (без дублей):  {failed_file} ({len(failed_eans)} шт)")
            if failed_details and failed_json_file:
                logger.info(f" JSON с деталями ошибок:            {failed_json_file}")
        else:
            logger.info(f" Статус проблемных EAN:            Все ошибки устранены! ({failed_file.name} пуст)")
    logger.info("=" * 65)


if __name__ == "__main__":
    asyncio.run(main())
