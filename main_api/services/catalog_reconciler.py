import asyncio
import gc
import re
import time
from pathlib import Path
from typing import Optional, Tuple, Set, List, Dict, Any

from main_api.services.aftercool_service import AftercoolService, AftercoolProductsQuery
from main_api.src.utils.ean_utils import normalize_ean, is_valid_ean

BACKEND_DIR = Path(__file__).resolve().parent.parent.parent

# Регулярные выражения и паттерны для выявления британских складов и каталогов
UK_FILE_PATTERNS = (
    re.compile(r"^uk[-_]", re.IGNORECASE),
    re.compile(r"[-_]uk[-_]", re.IGNORECASE),
    re.compile(r"[-_]uk\.", re.IGNORECASE),
    re.compile(r"\(uk\)", re.IGNORECASE),
    re.compile(r"uk-scorzany", re.IGNORECASE),
)

# Регулярные выражения для выявления удаленных, поврежденных и служебных файлов каталога
TRASH_FILE_PATTERNS = (
    re.compile(r"loschen", re.IGNORECASE),
    re.compile(r"löschen", re.IGNORECASE),
    re.compile(r"delete", re.IGNORECASE),
    re.compile(r"dont_touch", re.IGNORECASE),
    re.compile(r"don_t_touch", re.IGNORECASE),
    re.compile(r"not_for_marketplaces", re.IGNORECASE),
    re.compile(r"no_aktiv", re.IGNORECASE),
    re.compile(r"nicht_aktiv", re.IGNORECASE),
    re.compile(r"wrong_attribute", re.IGNORECASE),
    re.compile(r"sortierung", re.IGNORECASE),
    re.compile(r"fehlerbehebung", re.IGNORECASE),
    re.compile(r"only_produktbeschreibung", re.IGNORECASE),
    re.compile(r"freelancer", re.IGNORECASE),
    re.compile(r"kristina_maksimova", re.IGNORECASE),
    re.compile(r"veronika_shuklina", re.IGNORECASE),
    re.compile(r"burosit", re.IGNORECASE),
    re.compile(r"ukler_group", re.IGNORECASE),
    re.compile(r"t_h__tr", re.IGNORECASE),
)

# Паттерны названий товаров, маркированных на удаление или не являющихся реальными товарами
TRASH_TITLE_PATTERNS = (
    re.compile(
        r"(?:^|[^\w])(delette|delete|product\s*removed|product_removed|gelöscht|geloscht|loschen|löschen|entfernt|artikel\s*entfernt|nicht\s*verf[uü]gbar)(?:$|[^\w])",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*(test)\b",
        re.IGNORECASE,
    ),
)


def is_blacklisted_source(source_file: Optional[str]) -> bool:
    """
    Проверяет, относится ли файл-источник к удаленным, служебным или запрещенным для маркетплейсов.
    Такие файлы НИКОГДА не должны выгружаться на Kaufland, даже если передан флаг --allow-all-factories.
    """
    if not source_file:
        return False
    sf = str(source_file).strip().lower()
    return any(p.search(sf) for p in TRASH_FILE_PATTERNS)


def is_trash_title(
    title: Optional[str],
    raw_name: Optional[str] = None,
    desc: Optional[str] = None,
) -> bool:
    """
    Проверяет, является ли заголовок мусорным (Delette, Product removed, Test и т.д.).
    Kaufland бракует такие товары как 'You have not entered a title yet'.
    """
    trash_exact = {
        "delette",
        "delete",
        "product removed",
        "product_removed",
        "gelöscht",
        "geloscht",
        "löschen",
        "loschen",
        "test",
        "entfernt",
    }
    for candidate in (title, raw_name, desc):
        if not candidate:
            continue
        c_str = str(candidate).strip()
        if not c_str:
            continue
        c_lower = c_str.lower()
        if c_lower in trash_exact:
            return True
        if any(p.search(c_str) for p in TRASH_TITLE_PATTERNS):
            return True
    if title:
        letters_only = re.sub(r"[^a-zA-Zа-яА-Я0-9]", "", title)
        if len(letters_only) < 4:
            return True
    return False


def is_uk_source(source_file: Optional[str]) -> bool:
    """
    Проверяет, относится ли файл-источник к складам Великобритании.
    Kaufland работает только в странах ЕС, поэтому товары UK-складов
    должны быть строго исключены из синхронизации.
    """
    if not source_file:
        return False
    sf = str(source_file).strip().lower()
    for pattern in UK_FILE_PATTERNS:
        if pattern.search(sf):
            return True
    return False


def select_canonical_item(
    items: List[Dict[str, Any]],
    allowed_factories: Optional[Set[str]] = None,
    allow_all_factories: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Выбирает каноническую европейскую запись товара из списка записей одного EAN в Aftercool.
    Правила отбора:
    1. Жестко отбрасываются записи с британских складов (is_uk_source).
    2. Жестко отбрасываются записи из файлов удалений/мусора (is_blacklisted_source).
    3. Жестко отбрасываются записи с мусорными названиями (is_trash_title).
    4. Приоритет отдается записям из белого списка фабрик (allowed_factories).
    5. Приоритет отдается записям с немецким описанием (Artikelbeschreibung) и ценой в EUR.
    """
    if not items:
        return None

    # Шаг 1: Исключаем любые UK-записи, мусорные файлы и товары-удаления
    valid_items = [
        it
        for it in items
        if not is_uk_source(str(it.get("source_file") or ""))
        and not is_blacklisted_source(str(it.get("source_file") or ""))
        and not is_trash_title(
            str(it.get("name") or ""),
            desc=str((it.get("row") or {}).get("Artikelbeschreibung") or "")
        )
    ]
    candidates = valid_items if valid_items else []
    if not candidates:
        return None

    if len(candidates) == 1:
        return candidates[0]

    # Шаг 2: Фильтрация по белому списку фабрик (если задан и не разрешены все)
    if allowed_factories and not allow_all_factories:
        factory_matched = []
        for it in candidates:
            sf = str(it.get("source_file") or "").strip().lower()
            sf_clean = re.sub(r"\.(csv|xlsx)$", "", sf)
            sf_norm = re.sub(r"[^a-zA-Z0-9]", "", sf_clean)
            if any(fac in sf_norm for fac in allowed_factories):
                factory_matched.append(it)
        if factory_matched:
            candidates = factory_matched

    if len(candidates) == 1:
        return candidates[0]

    # Шаг 3: Выбираем запись с наиболее полным немецким описанием
    def candidate_score(c: Dict[str, Any]) -> int:
        score = 0
        row = c.get("row") or {}
        desc = str(row.get("Artikelbeschreibung") or c.get("name") or "")
        if desc:
            score += len(desc[:500])
        if row.get("PictureURL") or row.get("pictureurls"):
            score += 100
        # Если есть цена
        price = row.get("Startpreis") or c.get("price")
        if price:
            score += 50
        return score

    candidates.sort(key=candidate_score, reverse=True)
    return candidates[0]


class CatalogReconciler:
    """
    Сервис предварительной индексации каталогов и классификации принадлежности EAN к аккаунтам JV и XL.
    Обеспечивает строгое выполнение правила:
      Kaufland JV == Aftercool JV
      Kaufland XL == Aftercool XL
    """

    def __init__(
        self,
        service: Optional[AftercoolService] = None,
        overlap_file: Optional[Path] = None,
        jv_cache_file: Optional[Path] = None,
        xl_cache_file: Optional[Path] = None,
    ):
        self.service = service or AftercoolService()
        self.overlap_file = overlap_file or (BACKEND_DIR / "overlap_jv_xl_aftercool.txt")
        self.jv_cache_file = jv_cache_file or (BACKEND_DIR / "cache_reconciler_eans_jv.txt")
        self.xl_cache_file = xl_cache_file or (BACKEND_DIR / "cache_reconciler_eans_xl.txt")
        self.jv_eans: Set[str] = set()
        self.xl_eans: Set[str] = set()
        self.overlap_eans: Set[str] = set()
        self._lock = asyncio.Lock()
        self.is_initialized: bool = False

    @staticmethod
    def _read_eans_from_file(path: Path) -> Set[str]:
        """Быстрое чтение множества EAN из локального кэш-файла (занимает ~0.02 сек и ~4-8 МБ)."""
        if not path.exists():
            return set()
        with open(path, "r", encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip() and is_valid_ean(line.strip())}

    @staticmethod
    def _write_eans_to_file(path: Path, eans: Set[str]):
        """Атомарная запись множества EAN в файл кэша."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for ean in sorted(eans):
                f.write(f"{ean}\n")

    async def _stream_eans_from_aftercool(
        self,
        session: str,
        account: str,
        sanity_threshold: int = 10000,
        logger: Optional[Any] = None,
    ) -> Set[str]:
        """Потоковая загрузка EAN без накопления сотен тысяч словарей в памяти."""
        eans: Set[str] = set()
        page = 0
        page_size = 1000
        max_pages = 250
        while page < max_pages:
            query = AftercoolProductsQuery(
                account=account,
                dataset="lister",
                include_row=0,
                limit=page_size,
                offset=page * page_size
            )
            chunk = None
            for attempt in range(5):
                try:
                    chunk = await self.service.get_products(session, query=query)
                    break
                except Exception as req_err:
                    if attempt == 4:
                        if logger:
                            logger.error(f"CatalogReconciler: Failed to fetch {account} page {page} after 5 retries: {req_err}")
                        raise
                    await asyncio.sleep(1.0 * (attempt + 1))
            if not chunk:
                break
            for it in chunk:
                sf = str(it.get("source_file") or "")
                if is_uk_source(sf):
                    continue
                ean = normalize_ean(self.service._extract_ean(it) or "")
                if ean and is_valid_ean(ean):
                    eans.add(ean)
            chunk_len = len(chunk)
            del chunk  # Мгновенно освобождаем пачку словарей из памяти
            if chunk_len < page_size:
                break
            page += 1
        gc.collect()
        return eans

    async def preload_indices(
        self,
        session: str,
        sanity_threshold: int = 10000,
        use_disk_cache: bool = True,
        max_cache_age_hours: float = 24.0,
        logger: Optional[Any] = None,
    ) -> Tuple[int, int]:
        """
        Загружает множества валидных EAN для аккаунтов JV и XL.
        Использует ультра-быстрый дисковый кэш (если файлы свежие) или потоковое чтение Aftercool.
        Память: всего ~15-30 МБ вместо ~500 МБ.
        """
        async with self._lock:
            loaded_from_cache = False
            if use_disk_cache and self.jv_cache_file.exists() and self.xl_cache_file.exists():
                try:
                    jv_age = (time.time() - self.jv_cache_file.stat().st_mtime) / 3600.0
                    xl_age = (time.time() - self.xl_cache_file.stat().st_mtime) / 3600.0
                    if jv_age < max_cache_age_hours and xl_age < max_cache_age_hours:
                        if logger:
                            logger.info(
                                f"CatalogReconciler: Loading EAN indices from disk cache (jv_age={jv_age:.1f}h, xl_age={xl_age:.1f}h)..."
                            )
                        jv_set = await asyncio.to_thread(self._read_eans_from_file, self.jv_cache_file)
                        xl_set = await asyncio.to_thread(self._read_eans_from_file, self.xl_cache_file)
                        if len(jv_set) >= sanity_threshold and len(xl_set) >= sanity_threshold:
                            loaded_from_cache = True
                            if logger:
                                logger.info(
                                    f"CatalogReconciler: Fast disk cache loaded successfully in memory: JV={len(jv_set):,}, XL={len(xl_set):,}"
                                )
                except Exception as cache_err:
                    if logger:
                        logger.warning(f"CatalogReconciler: Failed to read cache: {cache_err}. Falling back to streaming Aftercool.")

            if not loaded_from_cache:
                if logger:
                    logger.info("Starting CatalogReconciler streaming preload from Aftercool (include_row=0)...")
                jv_set = await self._stream_eans_from_aftercool(session, "jv", sanity_threshold, logger)
                xl_set = await self._stream_eans_from_aftercool(session, "xl", sanity_threshold, logger)

                # Сохраняем в дисковый кэш для мгновенных последующих запусков
                try:
                    await asyncio.to_thread(self._write_eans_to_file, self.jv_cache_file, jv_set)
                    await asyncio.to_thread(self._write_eans_to_file, self.xl_cache_file, xl_set)
                except Exception as save_err:
                    if logger:
                        logger.warning(f"CatalogReconciler: Failed to write disk cache: {save_err}")

            # Sanity Check (защита от ложного удаления каталога при сетевом сбое)
            if len(jv_set) < sanity_threshold:
                raise RuntimeError(
                    f"CRITICAL: Aftercool JV returned only {len(jv_set)} valid EANs, expected >= {sanity_threshold}. Aborting to prevent accidental purge!"
                )
            if len(xl_set) < sanity_threshold:
                raise RuntimeError(
                    f"CRITICAL: Aftercool XL returned only {len(xl_set)} valid EANs, expected >= {sanity_threshold}. Aborting to prevent accidental purge!"
                )

            self.jv_eans = jv_set
            self.xl_eans = xl_set
            self.overlap_eans = self.jv_eans.intersection(self.xl_eans)
            self.is_initialized = True

            # Сохраняем выявленные пересечения в реестр
            if self.overlap_eans:
                await self._record_overlaps(self.overlap_eans)

            gc.collect()
            if logger:
                logger.info(
                    f"CatalogReconciler successfully indexed: JV={len(self.jv_eans):,} EANs, "
                    f"XL={len(self.xl_eans):,} EANs, OVERLAP={len(self.overlap_eans):,} EANs"
                )

            return len(self.jv_eans), len(self.xl_eans)

    def resolve_target_account(self, ean: str) -> Tuple[Optional[str], str]:
        """
        Мгновенное разрешение целевого аккаунта для EAN согласно матрице решений:
        1. EAN есть и в JV, и в XL: целевой аккаунт JV (по правилу приоритета пользователя), статус OVERLAP.
        2. EAN только в JV: целевой аккаунт JV, статус ONLY_JV.
        3. EAN только в XL: целевой аккаунт XL, статус ONLY_XL.
        4. EAN нет нигде: целевой аккаунт None, статус ORPHAN.
        """
        if not self.is_initialized:
            raise RuntimeError("CatalogReconciler is not initialized! Preload indices before resolving target account.")
        norm_ean = normalize_ean(ean)
        if not norm_ean or not is_valid_ean(norm_ean):
            return None, "INVALID_EAN"

        in_jv = norm_ean in self.jv_eans
        in_xl = norm_ean in self.xl_eans

        if in_jv and in_xl:
            return "jv", "OVERLAP"
        elif in_jv and not in_xl:
            return "jv", "ONLY_JV"
        elif not in_jv and in_xl:
            return "xl", "ONLY_XL"
        else:
            return None, "ORPHAN"

    async def _record_overlaps(self, overlap_eans: Set[str]):
        """Атомарно записывает список EAN-пересечений в файл."""
        lines = [f"{ean}\n" for ean in sorted(overlap_eans)]
        await asyncio.to_thread(self._sync_write_file, self.overlap_file, lines)

    @staticmethod
    def _sync_write_file(path: Path, lines: List[str]):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(lines)
