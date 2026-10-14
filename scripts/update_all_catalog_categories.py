#!/usr/bin/env python3
"""
Скрипт автономного высокоскоростного обновления категорий для всего каталога JV и XL.

Принципы безопасности:
1. Строгие белые списки:
   - JV: reports/JV_VALID.txt (81 443 уникальных EAN)
   - XL: reports/XL_VALID.txt (86 705 уникальных EAN)
2. Исключительно обновление категорий:
   - НЕ трогает юниты (цены, остатки, сроки доставки, handling_time).
   - НЕ трогает описания, картинки, MPN, параметры материалов (получает через GET и отправляет обратно неизменными).
3. Исчерпывающая таблица 100% валидных листовых мебельных и сопутствующих категорий Kaufland API.
4. Оптимизация нагрузки:
   - Если категория уже совпадает с целевой (по id или slug) — товар СКИПАЕТСЯ без PUT-запроса.
   - Если для товара нет уверенного правила (None) — категория НЕ ломается вслепую, товар скипается.
5. Высокая скорость в рамках официальных лимитов:
   - 85 req/s на аккаунт (официальный лимит Kaufland: 111 req/s на аккаунт).
   - Поддержка возобновления через чекпоинты (checkpoints) с fsync.
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

JV_VALID_FILE = REPORT_DIR / "JV_VALID.txt"
XL_VALID_FILE = REPORT_DIR / "XL_VALID.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("update_all_cats")

# ──────────────────────────────────────────────────────────────────────────────
# Каноническая таблица 100% валидных листовых категорий Kaufland API
# ──────────────────────────────────────────────────────────────────────────────
VALID_TARGET_CATEGORIES: Dict[str, Dict[str, Any]] = {
    # Мебель для сидения и отдыха
    "ecksofas": {"id": 5911, "title": "Ecksofas", "name": "ecksofas"},
    "couchgarnituren": {"id": 5961, "title": "Couchgarnituren", "name": "couchgarnituren"},
    "klassische-sofas": {"id": 45081, "title": "Klassische Sofas", "name": "klassische-sofas"},
    "klassische-sessel": {"id": 45071, "title": "Klassische Sessel", "name": "klassische-sessel"},
    "ohrensessel": {"id": 5861, "title": "Ohrensessel", "name": "ohrensessel"},
    "cocktailsessel": {"id": 5831, "title": "Cocktailsessel", "name": "cocktailsessel"},
    "fernseh-und-relaxsessel": {"id": 58231, "title": "Fernseh- und Relaxsessel", "name": "fernseh-und-relaxsessel"},
    "sitzhocker": {"id": 6351, "title": "Sitzhocker", "name": "sitzhocker"},
    "fusshocker": {"id": 6211, "title": "Fußhocker", "name": "fusshocker"},
    "sitzbaenke": {"id": 2701, "title": "Sitzbänke", "name": "sitzbaenke"},

    # Столы
    "couchtische": {"id": 3531, "title": "Couchtische", "name": "couchtische"},
    "beistelltische": {"id": 3541, "title": "Beistelltische", "name": "beistelltische"},
    "esstische": {"id": 3521, "title": "Esstische", "name": "esstische"},
    "konsolentische": {"id": 3561, "title": "Konsolentische", "name": "konsolentische"},
    "schminktische": {"id": 59691, "title": "Schminktische", "name": "schminktische"},
    "computertische": {"id": 5661, "title": "Computertische", "name": "computertische"},
    "konferenztische": {"id": 5671, "title": "Konferenztische", "name": "konferenztische"},
    "nachttische-und-schraenke": {"id": 2321, "title": "Nachttische & -schränke", "name": "nachttische-und-schraenke"},
    "billardtische": {"id": 10121, "title": "Billardtische", "name": "billardtische"},

    # Стулья
    "esszimmerstuehle": {"id": 4211, "title": "Esszimmerstühle", "name": "esszimmerstuehle"},
    "armlehnstuehle": {"id": 4311, "title": "Armlehnstühle", "name": "armlehnstuehle"},

    # Кровати и спальни
    "polsterbetten": {"id": 2751, "title": "Polsterbetten", "name": "polsterbetten"},
    "etagenbetten": {"id": 2871, "title": "Etagenbetten", "name": "etagenbetten"},
    "hochbetten": {"id": 2741, "title": "Hochbetten", "name": "hochbetten"},
    "jugendbetten": {"id": 2771, "title": "Jugendbetten", "name": "jugendbetten"},
    "himmelbetten": {"id": 2761, "title": "Himmelbetten", "name": "himmelbetten"},
    "wasserbetten": {"id": 2791, "title": "Wasserbetten", "name": "wasserbetten"},
    "schlafzimmer": {"id": 3271, "title": "Schlafzimmer", "name": "schlafzimmer"},

    # Шкафы, комоды, стенки, полки
    "schubladenschraenke": {"id": 6141, "title": "Schubladenschränke", "name": "schubladenschraenke"},
    "kommoden": {"id": 2241, "title": "Kommoden", "name": "kommoden"},
    "wohnwaende": {"id": 3361, "title": "Wohnwände", "name": "wohnwaende"},
    "wohnzimmerschraenke": {"id": 45091, "title": "Wohnzimmerschränke", "name": "wohnzimmerschraenke"},
    "haengeregale": {"id": 4171, "title": "Hängeregale", "name": "haengeregale"},
    "aktenschraenke": {"id": 6121, "title": "Aktenschränke", "name": "aktenschraenke"},
    "vitrinen": {"id": 3951, "title": "Vitrinen", "name": "vitrinen"},

    # Гарнитуры комнат
    "wohnzimmer": {"id": 3381, "title": "Wohnzimmer", "name": "wohnzimmer"},
    "esszimmer": {"id": 3481, "title": "Esszimmer", "name": "esszimmer"},
    "jugendzimmer": {"id": 3491, "title": "Jugendzimmer", "name": "jugendzimmer"},
    "kinderzimmer": {"id": 3301, "title": "Kinderzimmer", "name": "kinderzimmer"},
    "gartenmoebel-sets": {"id": 69163, "title": "Gartenmöbel-Sets", "name": "gartenmoebel-sets"},

    # Освещение
    "deckenleuchten": {"id": 4361, "title": "Deckenleuchten", "name": "deckenleuchten"},
    "wandleuchten": {"id": 4351, "title": "Wandleuchten", "name": "wandleuchten"},
    "kronleuchter": {"id": 4421, "title": "Kronleuchter", "name": "kronleuchter"},

    # Сантехника и сауны
    "badewannen": {"id": 43411, "title": "Badewannen", "name": "badewannen"},
    "whirlpools": {"id": 43421, "title": "Whirlpools", "name": "whirlpools"},
    "duschkabinen": {"id": 28311, "title": "Duschkabinen", "name": "duschkabinen"},
    "duschabtrennungen": {"id": 28361, "title": "Duschabtrennungen", "name": "duschabtrennungen"},
    "waschbecken": {"id": 28421, "title": "Waschbecken", "name": "waschbecken"},
    "saunen": {"id": 43431, "title": "Saunen", "name": "saunen"},

    # Декор, зеркала, искусство, текстиль
    "wandspiegel": {"id": 4771, "title": "Wandspiegel", "name": "wandspiegel"},
    "standspiegel": {"id": 60921, "title": "Standspiegel", "name": "standspiegel"},
    "skulpturen": {"id": 5121, "title": "Skulpturen", "name": "skulpturen"},
    "reliefs": {"id": 4741, "title": "Reliefs", "name": "reliefs"},
    "leinwandbilder": {"id": 4711, "title": "Leinwandbilder", "name": "leinwandbilder"},
    "blumenstaender": {"id": 26871, "title": "Blumenständer", "name": "blumenstaender"},
    "vasen": {"id": 6931, "title": "Vasen", "name": "vasen"},
    "designerteppiche": {"id": 4631, "title": "Designer-Teppiche", "name": "designerteppiche"},
}


# ──────────────────────────────────────────────────────────────────────────────
# Детерминированная классификация по заголовку
# ──────────────────────────────────────────────────────────────────────────────
def determine_category_for_title(title: str) -> Optional[Dict[str, Any]]:
    """
    Высокоточная детерминированная классификация товара Kaufland по немецкому названию.
    Строгая иерархия анкеров для предотвращения ложных срабатываний.
    """
    t = title.lower()

    # 1. Бильярдные столы и сауны
    if any(k in t for k in ["billardtisch", "billard"]):
        return VALID_TARGET_CATEGORIES["billardtische"]
    if any(k in t for k in ["sauna", "saunen", "infrarotkabine"]):
        return VALID_TARGET_CATEGORIES["saunen"]

    # 2. Рельефы и настенные барельефы (СТРОГО перед зеркалами и комодами!)
    if any(k in t for k in ["wandrelief", "flachrelief", "relief", "tafel wandrelief"]):
        return VALID_TARGET_CATEGORIES["reliefs"]

    # 3. Картины и настенные полотна
    if any(k in t for k in ["leinwandbild", "gemälde", "gemaelde", "ölgemälde", "oelgemaelde", "ölbild", "oelbild"]):
        if not any(k in t for k in ["sofa", "couch", "bett", "tisch", "schrank", "sideboard", "kommode"]):
            return VALID_TARGET_CATEGORIES["leinwandbilder"]
    if ("wandbild" in t or "bild" in t) and any(k in t for k in ["ölgemälde", "acryl", "leinwand", "handarbeit stein", "gemälde"]):
        if not any(k in t for k in ["sofa", "couch", "bett", "tisch", "schrank", "sideboard", "kommode"]):
            return VALID_TARGET_CATEGORIES["leinwandbilder"]

    # 4. Статуи, скульптуры, декоративные фигуры
    if any(k in t for k in [
        "skulptur", "statue", "skulpturen", "statuen",
        "büste", "bueste", "torso",
    ]):
        return VALID_TARGET_CATEGORIES["skulpturen"]
    if any(k in t for k in ["figur", "figuren"]) and not any(k in t for k in ["sofa", "couch", "bett", "tisch", "schrank", "kommode"]):
        return VALID_TARGET_CATEGORIES["skulpturen"]

    # 5. Сантехника: ванны, джакузи, раковины, душевые кабины
    if any(k in t for k in ["whirlpool", "whirlwanne", "jacuzzi"]):
        return VALID_TARGET_CATEGORIES["whirlpools"]
    if any(k in t for k in ["badewanne", "eckbadewanne", "acrylwanne", "badewannen", "freistehende wanne", "bad wanne"]):
        return VALID_TARGET_CATEGORIES["badewannen"]
    if any(k in t for k in ["duschkabine", "eckdusche", "duschabtrennung", "duschwand"]):
        return VALID_TARGET_CATEGORIES["duschkabinen"]
    if any(k in t for k in ["duschwanne", "duschtasse", "brausewanne"]):
        return VALID_TARGET_CATEGORIES["duschabtrennungen"]
    if any(k in t for k in ["waschbecken", "waschtisch", "waschschale", "aufsatzwaschbecken", "spülbecken", "spuelbecken"]):
        return VALID_TARGET_CATEGORIES["waschbecken"]

    # 6. Туалетные столики / Гримерные столики (Schminktische)
    # СТРОГО перед зеркалами и комодами! Комплект "Schminktisch mit Spiegel" - это SCHMINKTISCH!
    if any(k in t for k in ["schminktisch", "frisiertisch", "kosmetiktisch", "schminkkommode", "sckmink"]):
        return VALID_TARGET_CATEGORIES["schminktische"]

    # 7. Консольные столики (Konsolentische)
    # СТРОГО перед зеркалами! "Konsole mit Spiegel" - это KONSOLENTISCH!
    if any(k in t for k in ["konsolentisch", "wandkonsole", "dekokonsole", "eingangskonsole"]) or ("konsole" in t and "tisch" in t):
        return VALID_TARGET_CATEGORIES["konsolentische"]

    # 8. Зеркала
    if any(k in t for k in ["standspiegel", "ganzkörperspiegel", "ganzkoerperspiegel"]):
        return VALID_TARGET_CATEGORIES["standspiegel"]
    if "wandspiegel" in t:
        return VALID_TARGET_CATEGORIES["wandspiegel"]
    if "spiegel" in t and not any(k in t for k in ["spiegelschrank", "schrank", "kommode", "sideboard", "schminktisch", "garderobe"]):
        return VALID_TARGET_CATEGORIES["wandspiegel"]

    # 9. Цветочные подставки и вазы
    if any(k in t for k in ["blumenständer", "blumenstaender", "vasenständer", "vasenstaender", "pflanzenständer", "blumensäule", "blumen säule"]):
        return VALID_TARGET_CATEGORIES["blumenstaender"]
    if any(k in t for k in ["blumenvase", "bodenvase", "dekovase"]) or (re.search(r"\bvasen?\b", t) and not any(k in t for k in ["tisch", "schrank", "kommode", "sofa", "bett"])):
        return VALID_TARGET_CATEGORIES["vasen"]

    # 10. Садовая мебель и садовые качели
    if any(k in t for k in ["gartenmöbel", "gartenmoebel", "garten möbel", "terrassenmöbel", "balkonmöbel", "lounge garten", "gartenschaukel", "outdoor-schaukel", "outdoorschaukel"]):
        return VALID_TARGET_CATEGORIES["gartenmoebel-sets"]

    # 11. Ковры
    if any(k in t for k in ["teppich", "teppiche", "orientteppich", "webteppich", "hochflor"]):
        if not any(k in t for k in ["sofa", "couch", "bett", "tisch", "schrank"]):
            return VALID_TARGET_CATEGORIES["designerteppiche"]

    # 12. Кровати и спальные гарнитуры (СТРОГО перед столиками и тумбами!)
    # Комплект "Bett mit 2x Nachttisch" - это КРОВАТЬ / СПАЛЬНЯ!
    if any(k in t for k in ["himmelbett", "himmelbetten"]):
        return VALID_TARGET_CATEGORIES["himmelbetten"]
    if any(k in t for k in ["wasserbett", "wasserbetten"]):
        return VALID_TARGET_CATEGORIES["wasserbetten"]
    if any(k in t for k in ["etagenbett", "doppelstockbett", "stockbett"]):
        return VALID_TARGET_CATEGORIES["etagenbetten"]
    if "hochbett" in t:
        return VALID_TARGET_CATEGORIES["hochbetten"]
    if any(k in t for k in ["jugendbett", "kinderbett"]):
        return VALID_TARGET_CATEGORIES["jugendbetten"]

    # Поиск любого слова bett / betten (в т.ч. rundbett, lederbett, polsterbett, boxspring)
    is_bed = any(k in t for k in [
        "polsterbett", "boxspring", "doppelbett", "schlafsystem", "bettgestell",
        "schlafbett", "rundbett", "ehebett", "lederbett", "holzbett", "massivholzbett",
        "futonbett", "designbett", "luxusbett", "bettset", "bett-set",
    ]) or bool(re.search(r"[a-zäöü]*bett(en)?\b", t))

    is_bedding_only = any(k in t for k in [
        "kissen", "bettbezug", "bettwäsche", "bettwaesche",
        "bettdecke", "bettlaken", "spannbettlaken", "matratze",
    ]) and not any(k in t for k in ["bettgestell", "polsterbett", "boxspring", "holzbett", "bett mit", "bett 1", "bett 2"])

    if is_bed and not is_bedding_only:
        # Если в комплекте спальни есть шкаф (Kleiderschrank) -> schlafzimmer
        if any(k in t for k in ["schlafzimmer", "kleiderschrank"]) and any(k in t for k in ["set", "garnitur", "teilig", "gruppe"]):
            return VALID_TARGET_CATEGORIES["schlafzimmer"]
        return VALID_TARGET_CATEGORIES["polsterbetten"]

    # 13. Освещение и светильники
    if any(k in t for k in ["kronleuchter", "lüster", "luester"]):
        return VALID_TARGET_CATEGORIES["kronleuchter"]
    if any(k in t for k in ["wandleuchte", "wandlampe"]):
        return VALID_TARGET_CATEGORIES["wandleuchten"]
    if any(k in t for k in [
        "deckenleuchte", "pendelleuchte", "hängeleuchte", "deckenlampe",
        "stehleuchte", "stehlampe", "laterne",
    ]):
        return VALID_TARGET_CATEGORIES["deckenleuchten"]
    if re.search(r"\b(lampe|leuchte)n?\b", t) and not any(k in t for k in ["sofa", "couch", "tisch", "bett", "schrank", "kommode"]):
        return VALID_TARGET_CATEGORIES["deckenleuchten"]

    # 14. Скамьи и банкетки (Sitzbänke)
    if any(k in t for k in ["sitzbank", "polsterbank", "bettbank", "flurbank", "holzbank", "eckbank"]) or (
        re.search(r"\bbank\b", t) and not any(k in t for k in ["sofa", "couch", "bett"])
    ):
        return VALID_TARGET_CATEGORIES["sitzbaenke"]

    # 15. Пуфы и банкетки для ног (СТРОГО перед диванами!)
    is_hocker = any(k in t for k in ["fußhocker", "fusshocker", "sitzhocker", "polsterhocker", "hocker", "pouf"])
    if is_hocker and not any(k in t for k in ["sofa", "couch", "garnitur", "ecksofa", "wohnlandschaft"]):
        return VALID_TARGET_CATEGORIES["sitzhocker"]

    # 16. Угловые диваны и жилые ландшафты
    if any(k in t for k in ["ecksofa", "eckcouch", "wohnlandschaft", "l-form", "u-form"]):
        return VALID_TARGET_CATEGORIES["ecksofas"]

    # 17. Стулья: обеденные стулья и стулья с подлокотниками (СТРОГО перед диванами и креслами!)
    # Пример: "Moderner Esszimmerstuhl Luxus Stühle", "Set 4x Stühle Sessel Esszimmerstuhl"
    is_chair = any(k in t for k in [
        "esszimmerstuhl", "esszimmerstühle", "esszimmerstuehle",
        "küchenstuhl", "kuechenstuhl", "lehnstuhl", "freischwinger", "barhocker",
    ]) or (
        any(k in t for k in ["stuhl", "stühle", "stuehle"])
        and not any(k in t for k in ["esstisch", "küchentisch", "kuechentisch", "schreibtisch", "tisch"])
    )
    if is_chair and not any(k in t for k in ["sofa", "couch", "ecksofa", "wohnlandschaft"]):
        if any(k in t for k in ["armlehnstuhl", "armlehnstühle", "armlehnstuehle"]):
            return VALID_TARGET_CATEGORIES["armlehnstuehle"]
        return VALID_TARGET_CATEGORIES["esszimmerstuehle"]

    # 18. Диванные гарнитуры
    # СТРОГО перед журнальными столиками!
    if any(k in t for k in [
        "couchgarnitur", "sofagarnitur", "polstergarnitur", "polstergruppe",
        "3+2+1", "3+1+1", "3+2", "3+1", "321", "311", "3-sitzer und", "3 sitzer und",
        "sofas mit couchtisch", "couch mit couchtisch", "sofa set", "couch set",
    ]):
        return VALID_TARGET_CATEGORIES["couchgarnituren"]

    # 19. Диваны и софы
    if any(k in t for k in [
        "sofa", "schlafsofa", "chaiselongue", "chesterfield", "divan", "chaise",
        "recamiere", "kanapee",
    ]) or ("couch" in t and not any(k in t for k in ["couchtisch", "sofatisch"])):
        if any(k in t for k in ["garnitur", "gruppe", "set"]) and any(k in t for k in ["sessel", "3-sitzer", "2-sitzer"]):
            return VALID_TARGET_CATEGORIES["couchgarnituren"]
        return VALID_TARGET_CATEGORIES["klassische-sofas"]

    # 20. Кресла
    if "ohrensessel" in t:
        return VALID_TARGET_CATEGORIES["ohrensessel"]
    if "cocktailsessel" in t:
        return VALID_TARGET_CATEGORIES["cocktailsessel"]
    if any(k in t for k in ["relaxsessel", "fernsehsessel", "massagesessel"]):
        return VALID_TARGET_CATEGORIES["fernseh-und-relaxsessel"]
    if any(k in t for k in ["sessel", "loungesessel", "clubsessel", "einzelsessel", "fauteuil", "thron"]):
        return VALID_TARGET_CATEGORIES["klassische-sessel"]

    # 21. Конференц-столы
    if "konferenztisch" in t:
        return VALID_TARGET_CATEGORIES["konferenztische"]

    # 22. Обеденные гарнитуры (Esszimmer / Speisezimmer)
    if any(k in t for k in ["esszimmer-set", "esszimmer garnitur", "speisezimmer", "esszimmer möbel", "esszimmermöbel"]) or (
        "esstisch" in t and any(k in t for k in ["stühle", "stuehle", "stühlen", "stuehlen"])
    ):
        return VALID_TARGET_CATEGORIES["esszimmer"]

    # 23. Гарнитуры комнат
    if any(k in t for k in ["jugendzimmer", "jugendmöbel"]):
        return VALID_TARGET_CATEGORIES["jugendzimmer"]
    if any(k in t for k in ["kinderzimmer", "kindermöbel"]):
        return VALID_TARGET_CATEGORIES["kinderzimmer"]
    if any(k in t for k in [
        "wohnzimmer-set", "wohnzimmer möbel-set", "wohnzimmer möbel set",
        "wohnzimmer möbel", "wohnzimmergarnitur",
    ]):
        return VALID_TARGET_CATEGORIES["wohnzimmer"]
    if any(k in t for k in ["schlafzimmer-set", "schlafzimmer möbel", "schlafzimmer garnitur", "schlafzimmermöbel"]):
        return VALID_TARGET_CATEGORIES["schlafzimmer"]

    # 24. ТВ-стенки
    if any(k in t for k in [
        "wohnwand", "anbauwand", "medienwand", "tv-wand",
        "tv-lowboard und wandregal", "tv-ecke",
    ]):
        return VALID_TARGET_CATEGORIES["wohnwaende"]

    # 25. Столы: обеденные, журнальные, придиванные, компьютерные
    if any(k in t for k in ["esstisch", "küchentisch", "kuechentisch", "speisetisch", "esszimmertisch"]):
        return VALID_TARGET_CATEGORIES["esstische"]
    if any(k in t for k in ["couchtisch", "kaffeetisch", "wohnzimmertisch", "stubentisch", "salontisch", "sofatisch"]):
        return VALID_TARGET_CATEGORIES["couchtische"]
    if any(k in t for k in [
        "schreibtisch", "computertisch", "cheftisch", "bürotisch", "buerotisch",
        "arbeitsplatz", "schulbank", "pc tisch",
    ]):
        return VALID_TARGET_CATEGORIES["computertische"]

    # Столы общего типа
    if "tisch" in t or "tische" in t:
        if any(k in t for k in ["esszimmer", "küche", "kueche", "speise", "massivholz", "ausziehbar", "küchen"]):
            return VALID_TARGET_CATEGORIES["esstische"]
        if any(k in t for k in ["wohnzimmer", "kaffee", "salon"]):
            return VALID_TARGET_CATEGORIES["couchtische"]
        if any(k in t for k in ["büro", "arbeitszimmer"]):
            return VALID_TARGET_CATEGORIES["computertische"]

    # 26. Прикроватные тумбы отдельно (СТРОГО если в названии НЕТ кровати)
    if any(k in t for k in ["nachttisch", "nachtschrank", "nachtschränkchen", "nachtkommode", "bettkommode", "betttisch", "nachtkonsole"]):
        return VALID_TARGET_CATEGORIES["nachttische-und-schraenke"]

    # 27. Придиванные и приставные столики
    if any(k in t for k in ["beistelltisch", "anbautisch", "telefontisch", "ablagetisch", "blumentisch"]):
        return VALID_TARGET_CATEGORIES["beistelltische"]

    # 28. Витрины и буфеты
    if any(k in t for k in ["vitrine", "buffetvitrine", "glasvitrine", "standvitrine", "sammlervitrine"]):
        return VALID_TARGET_CATEGORIES["vitrinen"]

    # 29. Комоды, тумбы, сайдборды, ТВ-тумбы
    if any(k in t for k in [
        "tv-garnitur", "tv-lowboard", "tv-schrank", "tv-board", "tv-möbel",
        "tv lowboard", "tv board", "lowboard", "sideboard", "highboard",
        "kommode", "anrichte", "fernsehschrank", "schubladenschrank",
    ]):
        return VALID_TARGET_CATEGORIES["schubladenschraenke"]

    # 30. Офисные шкафы и настенные полки
    if any(k in t for k in ["büroschrank", "büro schrank", "bueroschrank", "aktenschrank"]):
        return VALID_TARGET_CATEGORIES["aktenschraenke"]
    if any(k in t for k in ["wandregal", "hängeregal", "haengeregal", "hängeboard", "oberes regal", "schweberegal"]):
        return VALID_TARGET_CATEGORIES["haengeregale"]

    # 31. Шкафы и гардеробы
    if any(k in t for k in ["wohnzimmerschrank", "wandschrank", "mehrzweckschrank"]):
        return VALID_TARGET_CATEGORIES["wohnzimmerschraenke"]
    if any(k in t for k in [
        "schrank", "kleiderschrank", "garderobe", "standregal",
        "regal", "drehtürenschrank", "schwebetürenschrank",
    ]):
        return VALID_TARGET_CATEGORIES["schlafzimmer"]

    # 32. Если в названии просто указана комната
    if "wohnzimmer" in t:
        return VALID_TARGET_CATEGORIES["wohnzimmer"]
    if "esszimmer" in t:
        return VALID_TARGET_CATEGORIES["esszimmer"]
    if "schlafzimmer" in t:
        return VALID_TARGET_CATEGORIES["schlafzimmer"]

    # Ни одно правило не сработало -> None (не трогать категорию вслепую)
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
        checkpoint_suffix: str = "full",
        reset_checkpoint: bool = False,
    ):
        self.account = account.upper()
        self.whitelist_file = whitelist_file
        self.rate_limit = rate_limit
        self.concurrency = concurrency
        self.dry_run = dry_run
        self.limit = limit

        cp_filename = f"cat_{checkpoint_suffix}_checkpoint_{account.lower()}.txt" if checkpoint_suffix else f"cat_update_checkpoint_{account.lower()}.txt"
        self.checkpoint_file = REPORT_DIR / cp_filename
        if reset_checkpoint and self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
            logger.info(f"[{self.account}] Чекпоинт {self.checkpoint_file.name} сброшен.")

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
            # Ни одно уверенное правило не совпало — оставляем категорию без изменений, НЕ ЛОМАЕМ
            await stats.inc("already_correct")
            await cp_writer.write(ean)
            if verbose:
                logger.info(f"[{account}][{ean}] Категория оставлена без изменений (нет уверенного правила): {curr_cat_id or curr_cat_slug} | '{title[:50]}'")
            return

        # Оптимизация: если текущая категория уже совпадает с целевой (по id или slug), пропускаем
        is_already_target = (curr_cat_id == target_cat["id"]) or (curr_cat_slug == target_cat["name"].lower())
        # Эквивалентные категории: комоды (2241 kommoden vs 6141 schubladenschraenke)
        if target_cat["id"] in (6141, 2241) and curr_cat_id in (6141, 2241):
            is_already_target = True
        # Эквивалентные категории: пуфы/банкетки (6351 sitzhocker vs 6211 fusshocker)
        if target_cat["id"] in (6351, 6211) and curr_cat_id in (6351, 6211):
            is_already_target = True

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
        # Сохраняем АБСОЛЮТНО ВСЕ исходные атрибуты без изменений (описание, картинки, MPN, производитель, и т.д.)!
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
# Процесс аккаунта
# ──────────────────────────────────────────────────────────────────────────────
async def process_account(ctx: AccountContext):
    logger.info(f"[{ctx.account}] Старт инициализации...")

    whitelist = load_eans_from_whitelist(ctx.whitelist_file)
    ctx.total_whitelist = len(whitelist)

    checkpoint = load_checkpoint(ctx.checkpoint_file)
    ctx.already_in_checkpoint = len(checkpoint)

    pending_eans = [e for e in whitelist if e not in checkpoint]
    if ctx.limit is not None and ctx.limit > 0:
        pending_eans = pending_eans[:ctx.limit]

    ctx.pending_count = len(pending_eans)

    logger.info(
        f"[{ctx.account}] Всего в белом списке: {ctx.total_whitelist} | "
        f"Уже в чекпоинте: {ctx.already_in_checkpoint} | "
        f"В очереди: {ctx.pending_count}"
    )

    if not pending_eans:
        logger.info(f"[{ctx.account}] Нет товаров для обработки. Завершено.")
        ctx.completed = True
        return

    queue: asyncio.Queue = asyncio.Queue(maxsize=ctx.concurrency * 4)
    rate_limiter = SafeRateLimiter(ctx.rate_limit)

    timeout = aiohttp.ClientTimeout(total=45.0, connect=10.0)
    conn = aiohttp.TCPConnector(limit=ctx.concurrency * 2, ttl_dns_cache=300)

    ctx.t0 = time.time()

    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
            rac = RestApiController(ctx.account.lower(), session)

            workers = [
                asyncio.create_task(run_worker(i, ctx, queue, rac, rate_limiter))
                for i in range(ctx.concurrency)
            ]

            async def enqueue():
                for ean in pending_eans:
                    await queue.put(ean)
                for _ in range(ctx.concurrency):
                    await queue.put(None)

            producer = asyncio.create_task(enqueue())

            await producer
            await queue.join()

            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

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
    parser.add_argument("--checkpoint-suffix", type=str, default="full",
                        help="Суффикс чекпоинта (по умолчанию: full, т.е. cat_full_checkpoint_{account}.txt)")
    parser.add_argument("--reset-checkpoint", action="store_true",
                        help="Сбросить чекпоинт перед запуском")
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
        contexts.append(AccountContext(
            "JV", JV_VALID_FILE, args.rate_limit, args.concurrency, args.dry_run, args.limit,
            args.checkpoint_suffix, args.reset_checkpoint
        ))
    if args.account in ("xl", "all"):
        contexts.append(AccountContext(
            "XL", XL_VALID_FILE, args.rate_limit, args.concurrency, args.dry_run, args.limit,
            args.checkpoint_suffix, args.reset_checkpoint
        ))

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
    report_file = REPORT_DIR / f"cat_full_report_{now_str}.txt"

    with open(report_file, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("ОТЧЁТ ПО ОБНОВЛЕНИЮ КАТЕГОРИЙ ВСЕГО КАТАЛОГА\n")
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
            f.write(f"  Уже с правильной категорией / оставлено: {snap.get('already_correct', 0)}\n")
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
            f"Уже ОК / Скип: {snap.get('already_correct', 0)} | "
            f"404: {snap.get('not_found', 0)} | "
            f"Ошибок: {snap.get('errors', 0)} | "
            f"Время: {elapsed:.1f}с ({speed:.1f} items/s)"
        )
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
