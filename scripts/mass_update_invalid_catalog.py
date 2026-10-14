#!/usr/bin/env python3
"""
Пайплайн массового обновления и приведения к идеалу невалидных товаров Kaufland.
Решает полный цикл проблем товаров из отчетов invalid_items:
1. Восстановление фото из Aftercool (с авто-сжатием > 8.5 MB и хостингом на kl.automatonsoft.de).
2. Классификация в строгие листовые мебельные категории Kaufland.
3. Определение фабрики и региона:
   - SOFORT (504251, 504255): handling_time = 7 дней, amount = 1 шт.
   - Китай (_CN_, __CN__, (CN)): handling_time = 60 дней (12 недель), amount = 20 шт.
   - Европа / Турция: handling_time = 30 дней, amount = 20 шт.
4. Строгое правило SKU: id_offer = ean (без JVM/XL и без суффиксов витрин).
5. Синхронизация по всем 8 активным витринам (DE, CZ, SK, PL, AT, FR, IT, NL), без Испании (ES).
6. Структурированные немецкие описания, производитель JVMOEBEL, контакты GPSR.
7. Поддержка чекпоинтов, воркеров и безопасного Rate Limiter (SafeRateLimiter 60.0 RPS).
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import logging
import os
import re
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from PIL import Image
import aiohttp

BACKEND_DIR = Path("/home/Altera/workdir/kaufland/backend")
sys.path.insert(0, str(BACKEND_DIR))

import config
from main_api.src.controller.rest_api_controller import RestApiController
from main_api.services.aftercool_service import AftercoolService, AftercoolProductsQuery
from main_api.src.utils.price_utils import parse_price
from main_api.src.utils.text_utils import sanitize_description, is_blacklisted_category
from main_api.src.utils.category_matcher import match_leaf_category
from main_api.src.extracter import _normalize_aftercool_html
from scripts.purge_trash_catalog_items import SafeRateLimiter, request_with_retry
from scripts.reupload_aftercool_catalog import (
    CN_PATTERNS,
    get_default_shipping_group,
    SHIPPING_GROUP_CACHE,
    DEFAULT_STOREFRONTS,
)
from scripts.compress_image_service import (
    process_and_publish_image,
    MAX_SAFE_FILE_SIZE,
)
from scripts.jv_logo_filter import filter_brand_images, filter_people_images
from main_api.services.material_service import resolve_material_attributes
from main_api.src.servises.json_mapper import _extract_dimensions
from main_api.src.controller.kaufland_controller import format_si_dimension


def clean_title_for_kaufland(title: str) -> str:
    """Очищает заголовок от технических суффиксов, промо-слов и восклицательных знаков для модерации Kaufland."""
    if not title:
        return ""
    t = str(title).strip()
    # Убираем технические суффиксы в скобках в конце: (BD), (CN), (DE), (TR), [123] и т.д.
    t = re.sub(r"\s*\([A-Za-z0-9_-]+\)\s*$", "", t).strip()
    t = re.sub(r"\s*\[[A-Za-z0-9_-]+\]\s*$", "", t).strip()
    # Убираем запрещенные рекламные слова и восклицательные знаки
    t = re.sub(r"\b(?:Neu|NEU|New|NEW|Top|TOP|Sale|SALE|Neuheit|Sonderangebot|Super)\b[!*]?", "", t, flags=re.IGNORECASE)
    t = re.sub(r"[!*#]+", "", t)
    t = re.sub(r"\s{2,}", " ", t).strip()
    if t and t[0].islower():
        t = t[0].upper() + t[1:]
    return t


# 8 активных витрин (без Испании)
ACTIVE_STOREFRONTS = ["de", "cz", "sk", "pl", "at", "fr", "it", "nl"]

# Локали для отправки product-data
LOCALES = {
    "de": "de-DE",
    "at": "de-AT",
    "cz": "cs-CZ",
    "sk": "sk-SK",
    "pl": "pl-PL",
    "fr": "fr-FR",
    "it": "it-IT",
    "nl": "nl-NL",
}

# Фабрики SOFORT
SOFORT_FACTORIES = {"504251", "504255"}

# Контактная информация по безопасности GPSR
GPSR_CONTACT = {
    "name": "AEA GmbH & Co. KG",
    "address": "Am Flugplatz 28, 88483 Burgrieden",
    "email_address": "info@jvmoebel.de",
    "phone_number": "07392 - 93 78 44 0",
    "url": "https://www.jvmoebel.de/Infos/Kontakt.htm",
}

# Листовые категории мебели (резервный маппинг)
LEAF_FURNITURE_MAPPING = [
    (["ecksofa", "wohnlandschaft", "l-form", "u-form"], 46001, "ecksofas", "Ecksofas"),
    (["weinschrank", "weinkuehlschrank", "weinkühlschrank", "weinregal", "flaschenregal", "barschrank"], 4161, "weinregale", "Weinregale"),
    (["schuhschrank", "schuhkipper", "schuhregal", "schuhbank"], 3961, "schuhschraenke", "Schuhschränke"),
    (["tv-board", "lowboard", "tv-schrank", "tv-möbel", "tv-moebel", "fernsehtisch"], 3771, "tv-moebel", "TV-Möbel"),
    (["chaiselounge", "recamiere", "ottomane"], 5831, "cocktailsessel", "Cocktailsessel"),
    (["sessel", "einsitzer"], 45071, "klassische-sessel", "Klassische Sessel"),
    (["couchtisch", "kaffeetisch", "beistelltisch"], 3531, "couchtische", "Couchtische"),
    (["esstisch", "esstische"], 3541, "esstische", "Esstische"),
    (["stuhl", "stühle", "stuehle", "esszimmerstuhl", "polsterstuhl"], 4211, "esszimmerstuehle", "Esszimmerstühle"),
    (["sofa", "couch", "garnitur", "dreisitzer", "zweisitzer"], 45081, "klassische-sofas", "Klassische Sofas"),
    (["bett", "polsterbett", "doppelbett", "boxspringbett"], 2751, "polsterbetten", "Polsterbetten"),
    (["vitrine", "vitrinenschrank", "glasschrank"], 3951, "vitrinen", "Vitrinen"),
    (["kleiderschrank", "drehtuerenschrank", "schwebetuerenschrank", "waescheschrank"], 3891, "kleiderschraenke", "Kleiderschränke"),
    (["kommode", "sideboard", "highboard"], 2241, "kommoden", "Kommoden"),
    (["schreibtisch", "computertisch", "buerotisch"], 5661, "computertische", "Computertische"),
    (["kronleuchter", "luester", "lüster"], 3811, "kronleuchter", "Kronleuchter"),
    (["deckenleuchte", "deckenlampe", "leuchte", "lampe"], 2801, "deckenleuchten", "Deckenleuchten"),
]

BLACKLIST_CATEGORIES = {
    4921,   # Dekostoffe
    61021,  # Brennpasten
    21001,  # Schminkstifte
    66041,  # Sonstige Geschirre
    59301,  # Schrankgriffe
    1,      # Möbel (родительская)
    3791,   # Sessel (родительская)
}


def setup_logger(account: str) -> logging.Logger:
    BACKEND_DIR.joinpath("logs").mkdir(parents=True, exist_ok=True)
    ts = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y%m%d_%H%M%S")
    log_file = BACKEND_DIR / "logs" / f"mass_update_{account}_{ts}.log"

    logger = logging.getLogger(f"mass_update_{account}")
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


def is_china(source_file: str, factory_id: str) -> bool:
    source_upper = f"{source_file or ''} {factory_id or ''}".upper()
    return any(p in source_upper for p in CN_PATTERNS)


def is_sofort(factory_id: str, source_file: str) -> bool:
    fid = str(factory_id).strip()
    s_upper = (source_file or "").upper()
    return fid in SOFORT_FACTORIES or "SOFORT" in s_upper


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

    color = specifics.get("Farbe") or ""
    if color:
        lines.append(f"<strong>Farbe:</strong> {color}<br/><br/>")

    material = specifics.get("Material") or specifics.get("Polsterstoff") or ""
    if material:
        lines.append(f"<strong>Material:</strong> {material}<br/><br/>")

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


def extract_aftercool_pictures(row: dict, it: dict) -> List[str]:
    raw_strings = []
    for field in ["PictureURL", "GalleryURL", "picture"]:
        val = row.get(field) or it.get(field)
        if val and isinstance(val, str):
            raw_strings.append(val)

    purls = row.get("pictureurls") or it.get("pictureurls")
    if purls and isinstance(purls, str):
        raw_strings.append(purls)

    pics = []
    for s in raw_strings:
        s_norm = s.replace("ftp://jvfurniture.eu:21/", "https://jvfurniture.eu/")
        s_norm = s_norm.replace("ftp://jvfurniture.eu/", "https://jvfurniture.eu/")
        parts = [p.strip() for p in re.split(r"[;\r\n|]+", s_norm) if p.strip()]
        for p in parts:
            sub_urls = re.findall(r"(https?://.+?)(?=(?:https?://|$))", p)
            if not sub_urls and (p.startswith("http://") or p.startswith("https://")):
                sub_urls = [p]
            for u in sub_urls:
                u_clean = u.strip()
                if u_clean and u_clean not in pics:
                    pics.append(u_clean)
    return pics



async def process_and_optimize_images(session: aiohttp.ClientSession, ean: str, raw_urls: List[str], logger: logging.Logger) -> List[str]:
    """
    Проверяет размер каждой картинки через process_and_publish_image.
    Если > 8.5 MB, сжимает через Pillow и публикует на https://kl.automatonsoft.de/upload-images/.
    Отсеивает битые ссылки и миниатюры < 500x500 px.
    """
    optimized_urls = []
    for idx, url in enumerate(raw_urls, 1):
        if not url or not url.startswith("http"):
            continue
        try:
            is_main = (idx == 1)
            pub_url = await asyncio.to_thread(process_and_publish_image, url, f"{ean}_{idx}", True, is_main)
            if not pub_url:
                continue
            # Проверяем разрешение локально сохраненного/опубликованного файла
            parsed = urllib.parse.urlparse(pub_url)
            local_filename = os.path.basename(parsed.path)
            local_path = BACKEND_DIR / "media" / "upload-images" / local_filename
            if local_path.exists():
                try:
                    with Image.open(local_path) as im:
                        w, h = im.size
                        if is_main and (w, h) == (2048, 2048):
                            logger.info(f"  [EAN {ean}][Pic {idx} (ГЛАВНОЕ)] Установлен точный размер 2048x2048.")
                        if w < 500 or h < 500:
                            logger.warning(f"  [EAN {ean}][Pic {idx}] Разрешение {w}x{h} < 500px! Исключено из отправки.")
                            continue
                except Exception:
                    pass
            optimized_urls.append(pub_url)
        except Exception as e:
            logger.error(f"  [EAN {ean}][Pic {idx}] Ошибка скачивания/обработки {url}: {e}. Ссылка исключена!")
    return optimized_urls



def load_checkpoint(cp_file: Path) -> Set[str]:
    if not cp_file.exists():
        return set()
    with open(cp_file, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def append_checkpoint(cp_file: Path, ean: str, dry_run: bool = False):
    if dry_run or not cp_file:
        return
    cp_file.parent.mkdir(parents=True, exist_ok=True)
    with open(cp_file, "a", encoding="utf-8") as f:
        f.write(f"{ean}\n")
        f.flush()
        os.fsync(f.fileno())


def append_problematic(prob_file: Path, ean: str, title: str, reason: str, dry_run: bool = False):
    if dry_run or not prob_file:
        return
    prob_file.parent.mkdir(parents=True, exist_ok=True)
    with open(prob_file, "a", encoding="utf-8") as f:
        f.write(f"{ean} — {title.strip()} | {reason.strip()}\n")
        f.flush()


class AftercoolSessionManager:
    """Менеджер сессии Aftercool с потокобезопасным авто-релогином при истечении сессии (401)."""

    def __init__(self, af_service: AftercoolService):
        self.af_service = af_service
        self.cookie: Optional[str] = None
        self._lock = asyncio.Lock()

    async def get_cookie(self, force_refresh: bool = False) -> str:
        async with self._lock:
            if not self.cookie or force_refresh:
                self.cookie = await self.af_service.login()
            return self.cookie

    async def get_products(self, query: AftercoolProductsQuery):
        cookie = await self.get_cookie()
        try:
            return await self.af_service.get_products(cookie, query=query)
        except Exception:
            # Сессия Aftercool истекает через ~3 часа: обновляем куку и повторяем попытку
            cookie = await self.get_cookie(force_refresh=True)
            return await self.af_service.get_products(cookie, query=query)

    async def get_stammartikel_beschreibung(self, stamm_id: str) -> str:
        if not stamm_id:
            return ""
        cookie = await self.get_cookie()
        try:
            return await self.af_service.get_stammartikel_beschreibung(cookie, stamm_id)
        except Exception:
            cookie = await self.get_cookie(force_refresh=True)
            return await self.af_service.get_stammartikel_beschreibung(cookie, stamm_id)


async def process_single_ean(
    ean: str,
    account: str,
    rac: RestApiController,
    af_session: AftercoolSessionManager,
    rate_limiter: SafeRateLimiter,
    session: aiohttp.ClientSession,
    stamm_cache: Dict[str, str],
    cp_file: Path,
    prob_file: Path,
    logger: logging.Logger,
    dry_run: bool = False,
) -> bool:
    logger.info("=" * 60)
    logger.info(f"[EAN {ean}] Начало обработки...")

    # 1. Поиск товара в Aftercool с авто-релогином
    af_res = await af_session.get_products(AftercoolProductsQuery(account=account, q=ean, limit=1, include_row=1))
    if not af_res:
        af_res = await af_session.get_products(AftercoolProductsQuery(account="", q=ean, limit=1, include_row=1))

    if not af_res:
        logger.error(f"[EAN {ean}] НЕ НАЙДЕН в Aftercool! Заносим в список проблемных.")
        append_problematic(prob_file, ean, "", "Not found in Aftercool", dry_run=dry_run)
        return False

    it = af_res[0]
    row = it.get("row") or {}
    raw_title = (it.get("name") or row.get("Artikelbeschreibung") or "").strip()
    title = clean_title_for_kaufland(raw_title)
    source_file = it.get("source_file") or row.get("source_file") or ""
    factory_id = str(it.get("factory_id") or row.get("factory_id") or "")
    spec_xml = row.get("CustomItemSpecifics") or ""
    specs = parse_item_specifics(spec_xml)
    stamm_id = str(row.get("I_stammartikel") or "").strip()

    # Фильтр на мусорные заголовки
    if title.lower() in ("delet", "delete", "мусор", "del", "löschen", "loeschen") or len(title) < 5:
        logger.warning(f"[EAN {ean}] Заголовок '{title}' помечен как удаленный (мусор). Добавляем в список на Purge.")
        append_problematic(prob_file, ean, title, "Title marked as trash/Delete in Aftercool (candidate for purge)", dry_run=dry_run)
        return False

    # 2. Определение параметров доставки по фабрике
    is_sof = is_sofort(factory_id, source_file)
    is_cn = is_china(source_file, factory_id)

    if is_sof:
        target_amount = 1
        target_ht = 7
        fac_type = "SOFORT"
    elif is_cn:
        target_amount = 20
        target_ht = 60
        fac_type = "CHINA"
    else:
        target_amount = 20
        target_ht = 30
        fac_type = "EUROPE/TURKEY"

    logger.info(f"  Фабрика: {source_file} (ID: {factory_id}) -> Тип: {fac_type} | HT: {target_ht} дн. | Остаток: {target_amount} шт.")

    # 3. Извлечение и подготовка фото
    raw_pics = extract_aftercool_pictures(row, it)
    logger.info(f"  Найдено фото в Aftercool: {len(raw_pics)} шт.")

    # Проверяем текущие фото на Kaufland
    pdata_res = await request_with_retry(rac, "GET", f"/product-data/{ean}?locale=de-DE", rate_limiter)
    pdata = pdata_res.data.get("data", {}) if isinstance(pdata_res.data, dict) else {}
    attrs = pdata.get("attributes", {}) if isinstance(pdata, dict) else {}
    kaufland_pics = attrs.get("picture", [])

    final_pics = raw_pics if raw_pics else kaufland_pics
    if not final_pics:
        logger.warning(f"  [EAN {ean}] НЕТ ФОТО ни в Aftercool, ни на Kaufland! Заносим в список проблемных.")
        append_problematic(prob_file, ean, title, "No images found in Aftercool or Kaufland", dry_run=dry_run)
        return False
    else:
        # Фильтр 1: Вырезаем рекламные слайды с логотипом бренда (JV Möbel)
        pics_before_logo = len(final_pics)
        final_pics = await filter_brand_images(account, final_pics)
        if len(final_pics) < pics_before_logo:
            logger.info(f"  [{account.upper()}][EAN {ean}] Удалено слайдов с логотипом бренда: {pics_before_logo - len(final_pics)} шт.")

        # Фильтр 2: Вырезаем фото с людьми (моделями) через YOLOv8
        pics_before_people = len(final_pics)
        final_pics = await filter_people_images(final_pics)
        if len(final_pics) < pics_before_people:
            logger.info(f"  [{account.upper()}][EAN {ean}] Удалено фото с людьми (YOLOv8): {pics_before_people - len(final_pics)} шт.")

        final_pics = await process_and_optimize_images(session, ean, final_pics, logger)
        if not final_pics:
            logger.warning(f"  [EAN {ean}] Все фото отфильтрованы или ссылки не работают (404/битые)! Заносим в список проблемных.")
            append_problematic(prob_file, ean, title, "All images broken (404) or filtered out", dry_run=dry_run)
            return False

    # 4. Категория и Описание
    cat_detail = attrs.get("category_detail", [{}])
    curr_cat_id = cat_detail[0].get("id") if cat_detail else None
    curr_cat_name = cat_detail[0].get("name") if cat_detail else None

    target_cat_id, target_cat_name, target_cat_title = resolve_category(title, curr_cat_id, curr_cat_name)
    logger.info(f"  Категория: {target_cat_name} (ID: {target_cat_id})")

    # Структурированное описание
    stamm_html = ""
    if stamm_id and stamm_id not in ("None", "0"):
        if stamm_id in stamm_cache:
            stamm_html = stamm_cache[stamm_id]
        else:
            try:
                stamm_html = await af_session.get_stammartikel_beschreibung(stamm_id)
                stamm_cache[stamm_id] = stamm_html
            except Exception:
                pass

    rich_desc = format_rich_description(title, specs, stamm_html)

    # 5. Определение базовой цены товара
    raw_price = row.get("Startpreis") or row.get("Preis") or row.get("SofortkaufenPreis") or it.get("price")
    price_eur = parse_price(raw_price, default=0.0)

    if price_eur <= 15.0:
        # Пытаемся взять цену из существующего юнита DE (сначала по ean, затем по id_offer)
        u_find = await request_with_retry(rac, "GET", f"/units?storefront=de&ean={ean}", rate_limiter)
        u_list = u_find.data.get("data", []) if isinstance(u_find.data, dict) else []
        if not u_list:
            u_find = await request_with_retry(rac, "GET", f"/units?storefront=de&id_offer={ean}", rate_limiter)
            u_list = u_find.data.get("data", []) if isinstance(u_find.data, dict) else []

        if u_list and u_list[0].get("price"):
            price_eur = float(u_list[0]["price"]) / 100.0
            logger.info(f"  Цена взята из существующего юнита Kaufland: {price_eur:.2f} €")
        else:
            logger.warning(f"  [EAN {ean}] Пропуск: цена товара 0.0 € (в Aftercool и Kaufland нет цены продажи > 15 €).")
            append_problematic(prob_file, ean, title, f"Price is 0 EUR ({price_eur})", dry_run=dry_run)
            return False

    # 6. Отправка PUT /product-data по активным локалям
    new_attrs = dict(attrs)
    new_attrs.pop("category_detail", None)
    new_attrs["title"] = [title]
    new_attrs["description"] = [rich_desc]
    new_attrs["category"] = [target_cat_name]
    new_attrs["manufacturer"] = [config.XL_MANUFACTURER if account == "xl" else config.JV_MANUFACTURER]
    new_attrs["product_safety_contact"] = config.XL_CONTACT_DATA if account == "xl" else config.JV_CONTACT_DATA
    if final_pics:
        new_attrs["picture"] = final_pics[:20]

    # Извлечение и форматирование габаритов (Höhe, Breite, Länge)
    raw_size = specs.get("Maße") or specs.get("Masse") or row.get("Maße") or row.get("size") or ""
    dim_l, dim_w, dim_h = _extract_dimensions(raw_size)
    spec_h = specs.get("Höhe") or specs.get("Hoehe") or row.get("Höhe") or ""
    spec_w = specs.get("Breite") or row.get("Breite") or ""
    spec_l = specs.get("Länge") or specs.get("Laenge") or row.get("Länge") or ""
    if spec_h:
        dim_h = spec_h
    if spec_w:
        dim_w = spec_w
    if spec_l:
        dim_l = spec_l

    if dim_h:
        h_si = format_si_dimension(dim_h)
        if h_si:
            new_attrs["height"] = h_si
    if dim_w:
        w_si = format_si_dimension(dim_w)
        if w_si:
            new_attrs["width"] = w_si
    if dim_l:
        l_si = format_si_dimension(dim_l)
        if l_si:
            new_attrs["length"] = l_si

    # Цвет (colour) - поддержка множественных цветов без некорректных склеек через слэш
    color_val = specs.get("Farbe") or row.get("Farbe") or row.get("colour") or row.get("color")
    if color_val:
        parts = [p.strip() for p in re.split(r"[/,+|]|\s+-\s+", str(color_val)) if p.strip()]
        new_attrs["colour"] = parts if parts else [str(color_val).strip()]

    # Нормализация материалов и состава ткани через material_service
    mat_res = resolve_material_attributes(
        row=row,
        title=title,
        description=rich_desc,
        cat_name=target_cat_name,
    )
    resolved_mat = mat_res.material
    is_framed_upholstered = any(k in target_cat_name.lower() for k in ["sofa", "ecksofa", "sessel", "stuhl", "stuehle", "bett", "couch", "garnitur"])
    if is_framed_upholstered and (resolved_mat in ["Textil", "Stoff", None, ""]):
        all_txt = f"{title} {rich_desc} {row.get('CustomItemSpecifics', '')}".lower()
        if any(m in all_txt for m in ["metall", "stahl", "eisen", "edelstahl"]):
            resolved_mat = "Metall"
        else:
            resolved_mat = "Holz"

    if resolved_mat:
        new_attrs["material"] = [resolved_mat]

    if mat_res.material_composition:
        new_attrs["material_composition"] = [mat_res.material_composition]
    elif is_framed_upholstered:
        new_attrs["material_composition"] = ["100% Polyester"]
    else:
        new_attrs["material_composition"] = []
        new_attrs["parts_of_animal_origin"] = []

    if mat_res.parts_of_animal_origin:
        new_attrs["parts_of_animal_origin"] = [mat_res.parts_of_animal_origin]
    elif new_attrs.get("material_composition"):
        new_attrs["parts_of_animal_origin"] = ["No"]

    active_storefronts = config.get_account_storefronts(account)

    put_success = False
    if dry_run:
        logger.info(f"  [DRY-RUN] Карточка готова к отправке: pics={len(final_pics)}, cat={target_cat_name}, mat={new_attrs.get('material')}")
        put_success = True
    else:
        payload = {"ean": [ean], "attributes": new_attrs}
        for sf in active_storefronts:
            loc = LOCALES.get(sf, "de-DE")
            res_put = await request_with_retry(rac, "PUT", f"/product-data?locale={loc}", rate_limiter, json_payload=payload)
            if res_put.status in (200, 204):
                put_success = True
            else:
                logger.error(f"  [{loc}] Ошибка PUT /product-data: status={res_put.status}, err={res_put.error}")

    if not put_success:
        logger.error(f"  [EAN {ean}] PUT /product-data не удался ни на одной витрине! Товар не заносится в чекпоинт.")
        return False

    # 7. Создание/обновление офферов (units) по всем активным витринам
    units_success_count = 0
    for sf in active_storefronts:
        sg_id = await get_default_shipping_group(rac, rate_limiter, sf, SHIPPING_GROUP_CACHE, account=account, logger=logger)
        price_payload = rac.make_price_payload(storefront=sf, price=price_eur) if price_eur > 15.0 else None

        if dry_run:
            logger.info(f"  [DRY-RUN][{sf.upper()}] Оффер: SKU={ean}, amount={target_amount}, ht={target_ht}, sg={sg_id}")
            units_success_count += 1
            continue

        unit_res = await request_with_retry(rac, "GET", f"/units?storefront={sf}&id_offer={ean}", rate_limiter)
        units = unit_res.data.get("data", []) if isinstance(unit_res.data, dict) else []

        if units:
            unit_id = units[0].get("id_unit")
            patch_body = {
                "amount": target_amount,
                "handling_time": target_ht,
            }
            if price_payload:
                patch_body["listing_price"] = price_payload["listing_price"]
                patch_body["minimum_price"] = price_payload["minimum_price"]
            if sg_id:
                patch_body["id_shipping_group"] = sg_id

            patch_res = await request_with_retry(rac, "PATCH", f"/units/{unit_id}?storefront={sf}", rate_limiter, json_payload=patch_body)
            if patch_res.status in (200, 204):
                logger.info(f"  [{sf.upper()}] Юнит {unit_id} ОБНОВЛЕН: ht={target_ht}, amt={target_amount}")
                units_success_count += 1
            else:
                logger.error(f"  [{sf.upper()}] Ошибка PATCH юнита {unit_id}: status={patch_res.status}")
        else:
            if not price_payload:
                logger.warning(f"  [{sf.upper()}] Нет цены для создания нового оффера!")
                continue

            post_body = {
                "amount": target_amount,
                "handling_time": target_ht,
                "listing_price": price_payload["listing_price"],
                "minimum_price": price_payload["minimum_price"],
                "ean": ean,
                "id_offer": ean,
                "condition": "NEW",
            }
            if sg_id:
                post_body["id_shipping_group"] = sg_id

            post_res = await request_with_retry(rac, "POST", f"/units?storefront={sf}&embedded=eco_participation", rate_limiter, json_payload=post_body)
            if post_res.status in (200, 201, 204):
                logger.info(f"  [{sf.upper()}] Новый юнит СОЗДАН: ht={target_ht}, amt={target_amount}, price={price_payload['listing_price']}")
                units_success_count += 1
            elif post_res.status == 409 or (post_res.status == 400 and ("already-exists" in str(post_res.error) or "already exists" in str(post_res.error))):
                # Fallback при 409 / 400 already-exists (дубликат/уже создан)
                u_check = await request_with_retry(rac, "GET", f"/units?storefront={sf}&id_offer={ean}", rate_limiter)
                u_list = u_check.data.get("data", []) if isinstance(u_check.data, dict) else []
                if u_list:
                    u_id = u_list[0].get("id_unit")
                    patch_body = {"amount": target_amount, "handling_time": target_ht}
                    if price_payload:
                        patch_body["listing_price"] = price_payload["listing_price"]
                        patch_body["minimum_price"] = price_payload["minimum_price"]
                    if sg_id:
                        patch_body["id_shipping_group"] = sg_id
                    await request_with_retry(rac, "PATCH", f"/units/{u_id}?storefront={sf}", rate_limiter, json_payload=patch_body)
                    logger.info(f"  [{sf.upper()}] Юнит {u_id} обновлен через 409/400 fallback!")
                    units_success_count += 1
                else:
                    logger.info(f"  [{sf.upper()}] Юнит уже существует в базе (id_offer={ean}), ожидает завершения каталожной индексации")
                    units_success_count += 1
            else:
                logger.error(f"  [{sf.upper()}] Ошибка POST юнита: status={post_res.status}, body={post_res.data}, err={post_res.error}")

    if units_success_count > 0:
        append_checkpoint(cp_file, ean, dry_run=dry_run)
        global_cp = BACKEND_DIR / "reports" / f"mass_update_checkpoint_{account}.txt"
        if cp_file.resolve() != global_cp.resolve():
            append_checkpoint(global_cp, ean, dry_run=dry_run)
        logger.info(f"[EAN {ean}] Успешно обработан и занесен в чекпоинт ({units_success_count} витрин).")
        return True
    else:
        logger.warning(f"[EAN {ean}] Ни один юнит не был успешно обновлен/создан! Товар НЕ заносится в чекпоинт.")
        return False


async def process_single_ean_mfr_only(
    ean: str,
    account: str,
    rac: RestApiController,
    rate_limiter: SafeRateLimiter,
    cp_file: Path,
    logger: logging.Logger,
    dry_run: bool = False,
) -> bool:
    target_mfr = config.XL_MANUFACTURER if account == "xl" else config.JV_MANUFACTURER

    # 1. Получаем текущую карточку с Kaufland
    pdata_res = await request_with_retry(rac, "GET", f"/product-data/{ean}?locale=de-DE", rate_limiter)
    if pdata_res.status not in (200, 204) or not pdata_res.data:
        logger.error(f"[EAN {ean}] Ошибка GET /product-data: status={pdata_res.status}, err={pdata_res.error}")
        return False

    pdata = pdata_res.data.get("data", {}) if isinstance(pdata_res.data, dict) else {}
    attrs = pdata.get("attributes", {}) if isinstance(pdata, dict) else {}
    if not attrs:
        logger.warning(f"[EAN {ean}] Пустые атрибуты на Kaufland. Пропуск.")
        append_checkpoint(cp_file, ean, dry_run=dry_run)
        return False

    # 2. Обновляем ТОЛЬКО manufacturer
    new_attrs = dict(attrs)
    new_attrs.pop("category_detail", None)
    new_attrs["manufacturer"] = [target_mfr]

    active_storefronts = config.get_account_storefronts(account)
    if dry_run:
        logger.info(f"  [DRY-RUN][EAN {ean}] Будет установлен manufacturer: {target_mfr}")
    else:
        payload = {"ean": [ean], "attributes": new_attrs}
        for sf in active_storefronts:
            loc = LOCALES.get(sf, "de-DE")
            res_put = await request_with_retry(rac, "PUT", f"/product-data?locale={loc}", rate_limiter, json_payload=payload)
            if res_put.status not in (200, 204):
                logger.error(f"  [{loc}][EAN {ean}] Ошибка PUT /product-data: status={res_put.status}, err={res_put.error}")

    append_checkpoint(cp_file, ean, dry_run=dry_run)
    logger.info(f"[EAN {ean}] Manufacturer успешно обновлен на '{target_mfr}' и занесен в чекпоинт.")
    return True


async def main():
    parser = argparse.ArgumentParser(description="Mass update and fix invalid Kaufland products")
    parser.add_argument("--account", choices=["jv", "xl"], default="jv", help="Kaufland account")
    parser.add_argument("--input-file", type=str, default="backend/reports/factories_with_eans_invalid_items_05_10_2026.txt", help="Path to EANs file")
    parser.add_argument("--eans", nargs="+", help="Specific EANs to process")
    parser.add_argument("--limit", type=int, help="Limit number of items to process")
    parser.add_argument("--concurrency", type=int, default=5, help="Number of concurrent workers")
    parser.add_argument("--rate-limit", type=float, default=60.0, help="Max requests per second")
    parser.add_argument("--dry-run", action="store_true", help="Dry run without modifying data")
    parser.add_argument("--force", action="store_true", help="Force processing even if in checkpoint")
    parser.add_argument("--only-manufacturer", action="store_true", help="Only update manufacturer without changing any other product data")
    parser.add_argument("--checkpoint-file", type=str, help="Custom checkpoint file path")
    parser.add_argument("--prob-file", type=str, help="Custom problematic EANs file path")
    args = parser.parse_args()

    logger = setup_logger(args.account)
    logger.info("=" * 80)
    logger.info(f"ЗАПУСК ПАЙПЛАЙНА МАССОВОГО ОБНОВЛЕНИЯ ДЛЯ АККАУНТА: {args.account.upper()}")
    logger.info(f"Режим DRY-RUN: {args.dry_run} | RPS: {args.rate_limit} | Воркеров: {args.concurrency} | Только Manufacturer: {args.only_manufacturer}")
    logger.info("=" * 80)

    # Загрузка EANs
    eans_to_process = []
    if args.eans:
        eans_to_process = [e.strip() for e in args.eans if re.match(r"^\d{13}$", e.strip())]
    else:
        input_path = Path(args.input_file)
        if not input_path.exists():
            input_path = BACKEND_DIR / args.input_file
        if not input_path.exists():
            logger.error(f"Файл {args.input_file} не найден!")
            return
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                if "delet" in line.lower():
                    continue
                m = re.search(r"\b(\d{13})\b", line)
                if m:
                    ean_clean = m.group(1)
                    if ean_clean not in eans_to_process:
                        eans_to_process.append(ean_clean)

    if args.checkpoint_file:
        raw_cp = args.checkpoint_file
        if raw_cp.startswith("backend/"):
            raw_cp = raw_cp[len("backend/"):]
        cp_file = Path(raw_cp)
        if not cp_file.is_absolute():
            cp_file = BACKEND_DIR / raw_cp
    elif args.only_manufacturer:
        cp_file = BACKEND_DIR / "reports" / f"mass_update_checkpoint_mfr_{args.account}.txt"
    else:
        cp_file = BACKEND_DIR / "reports" / f"mass_update_checkpoint_{args.account}.txt"

    if args.prob_file:
        raw_prob = args.prob_file
        if raw_prob.startswith("backend/"):
            raw_prob = raw_prob[len("backend/"):]
        prob_file = Path(raw_prob)
        if not prob_file.is_absolute():
            prob_file = BACKEND_DIR / raw_prob
    else:
        prob_file = BACKEND_DIR / "problematic_eans_no_images.txt"

    completed = load_checkpoint(cp_file)
    if args.force:
        pending = eans_to_process
    else:
        pending = [e for e in eans_to_process if e not in completed]

    if args.limit:
        pending = pending[:args.limit]

    logger.info(f"Всего в списке: {len(eans_to_process):,} | Уже сделано: {len(completed):,} | К обработке: {len(pending):,}")
    if not pending:
        logger.info("Все товары уже обработаны!")
        return

    # Инициализация сервисов
    rate_limiter = SafeRateLimiter(max_rps=args.rate_limit)
    af_session = None
    stamm_cache: Dict[str, str] = {}
    if not args.only_manufacturer:
        af_service = AftercoolService()
        af_session = AftercoolSessionManager(af_service)
        await af_session.get_cookie()

    timeout = aiohttp.ClientTimeout(total=45.0)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        rac = RestApiController(args.account, session)

        # Обработка товаров через Semaphore
        sem = asyncio.Semaphore(args.concurrency)
        counter_lock = asyncio.Lock()
        success_cnt = 0
        skip_cnt = 0
        err_cnt = 0
        processed_cnt = 0
        total_pending = len(pending)

        async def worker(ean_val: str):
            nonlocal success_cnt, skip_cnt, err_cnt, processed_cnt
            async with sem:
                try:
                    if args.only_manufacturer:
                        res = await process_single_ean_mfr_only(
                            ean=ean_val,
                            account=args.account,
                            rac=rac,
                            rate_limiter=rate_limiter,
                            cp_file=cp_file,
                            logger=logger,
                            dry_run=args.dry_run,
                        )
                    else:
                        res = await process_single_ean(
                            ean=ean_val,
                            account=args.account,
                            rac=rac,
                            af_session=af_session,
                            rate_limiter=rate_limiter,
                            session=session,
                            stamm_cache=stamm_cache,
                            cp_file=cp_file,
                            prob_file=prob_file,
                            logger=logger,
                            dry_run=args.dry_run,
                        )
                    async with counter_lock:
                        processed_cnt += 1
                        if res:
                            success_cnt += 1
                        else:
                            skip_cnt += 1
                        if processed_cnt % 10 == 0 or processed_cnt == total_pending:
                            logger.info(f">>> ПРОГРЕСС: [{processed_cnt}/{total_pending}] (Успешно: {success_cnt}, Пропущено: {skip_cnt}, Ошибок: {err_cnt}) <<<")
                except Exception as exc:
                    async with counter_lock:
                        processed_cnt += 1
                        err_cnt += 1
                    logger.error(f"[EAN {ean_val}] Непредвиденная ошибка воркера: {exc}", exc_info=True)

        tasks = [asyncio.create_task(worker(e)) for e in pending]
        await asyncio.gather(*tasks)

    logger.info("=" * 80)
    logger.info("ОБРАБОТКА ЗАВЕРШЕНА!")
    logger.info(f"ИТОГИ: Обработано: {processed_cnt}/{total_pending} | Успешно: {success_cnt} | Пропущено: {skip_cnt} | Ошибок: {err_cnt}")
    logger.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
