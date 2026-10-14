from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any, Optional, Dict, List


SPECIFIC_ALIASES = {
    "manufacturer": ("hersteller", "herstellername", "manufacturer"),
    "brand": ("marke", "brand"),
    "material": ("material", "materialart", "polsterstoff", "gestellmaterial"),
    "material_composition": ("materialzusammensetzung", "zusammensetzung", "material composition", "composition"),
    "colour": ("farbe", "colour", "color"),
    "width": ("breite", "width"),
    "length": ("länge", "tiefe", "length", "depth"),
    "height": ("höhe", "height"),
}


def normalize_specific_key(value: str) -> str:
    """Удаляет лишние пробелы и переводит в нижний регистр для безопасного поиска."""
    return re.sub(r"\s+", " ", value.strip()).casefold()


def get_canonical_key(raw_key: str) -> str:
    """Возвращает каноническое имя ключа на основе SPECIFIC_ALIASES."""
    norm = normalize_specific_key(raw_key)
    for canonical, aliases in SPECIFIC_ALIASES.items():
        if norm == canonical or norm in aliases:
            return canonical
    return norm  # Возвращаем нормализованный ключ, если нет синонима


def parse_item_specifics(value: Any) -> Dict[str, List[str]]:
    """
    Парсит CustomItemSpecifics (XML или строка) в словарь списков с каноническими ключами.
    Гарантирует, что значения всегда списки (у одного ключа может быть несколько значений).
    """
    if not value:
        return {}
    if isinstance(value, dict):
        res: Dict[str, List[str]] = {}
        for k, v in value.items():
            key = get_canonical_key(k)
            if key not in res:
                res[key] = []
            if isinstance(v, list):
                res[key].extend(str(x) for x in v)
            else:
                res[key].append(str(v))
        return res

    text = str(value).strip()
    if not text or "<NameValueList" not in text:
        return {}

    specifics: Dict[str, List[str]] = {}
    
    def add_specific(raw_name: str, val: str):
        if not raw_name or not val:
            return
        key = get_canonical_key(raw_name)
        if key not in specifics:
            specifics[key] = []
        if val not in specifics[key]:
            specifics[key].append(val)

    try:
        # Оборачиваем в root, чтобы парсер не падал, если в XML несколько корневых узлов
        root_text = f"<root>{text}</root>"
        root = ET.fromstring(root_text)
        for node in root.findall(".//NameValueList"):
            name = (node.findtext("Name") or "").strip()
            for v in node.findall("Value"):
                val = (v.text or "").strip()
                add_specific(name, val)
    except Exception:
        # Fallback: мощный regex парсер, если XML сломан (CDATA, эскейпинг)
        for block in re.findall(
            r"<NameValueList\b[^>]*>(.*?)</NameValueList>",
            text,
            flags=re.DOTALL | re.IGNORECASE,
        ):
            name_m = re.search(
                r"<Name\b[^>]*>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</Name>",
                block,
                flags=re.DOTALL | re.IGNORECASE,
            )
            val_ms = re.findall(
                r"<Value\b[^>]*>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</Value>",
                block,
                flags=re.DOTALL | re.IGNORECASE,
            )
            if name_m and val_ms:
                name = name_m.group(1).strip()
                for val in val_ms:
                    add_specific(name, val.strip())
                    
    return specifics


def get_first_specific(specifics: Dict[str, List[str]], key: str) -> Optional[str]:
    """Вспомогательная функция для безопасного получения первого значения из списка."""
    vals = specifics.get(key)
    return vals[0] if vals else None


def extract_explicit_composition(text: str) -> Optional[str]:
    """
    Ищет явный процентный состав вроде '80% Polyester, 20% Baumwolle' или '100% Leinen' в описании/названии.
    """
    if not text:
        return None

    pattern = r"\b\d{1,3}\s*%\s*[a-zA-ZäöüßÄÖÜ]+(?:[,\s/]+\d{1,3}\s*%\s*[a-zA-ZäöüßÄÖÜ]+)*\b"
    matches = re.findall(pattern, text)
    if matches:
        known_fibers = [
            "polyester",
            "baumwolle",
            "leinen",
            "viskose",
            "polyurethan",
            "leder",
            "wolle",
            "seide",
            "nylon",
            "polyamid",
            "polyacryl",
        ]
        for m in matches:
            m_lower = m.lower()
            if any(f in m_lower for f in known_fibers):
                percentages = [int(p) for p in re.findall(r"(\d{1,3})\s*%", m)]
                if percentages and sum(percentages) <= 100:
                    return m.strip()
    return None


class MaterialResolution(tuple):
    """
    3-элементный кортеж (material, material_composition, parts_of_animal_origin)
    с дополнительным атрибутом .materials (полный список материалов для составных товаров).
    Обеспечивает 100% обратную совместимость.
    """
    material: Optional[str]
    material_composition: Optional[str]
    parts_of_animal_origin: Optional[str]
    materials: list[str]

    def __new__(
        cls,
        material: Optional[str],
        material_composition: Optional[str],
        parts_of_animal_origin: Optional[str],
        materials: Optional[list[str]] = None,
    ):
        return super().__new__(cls, (material, material_composition, parts_of_animal_origin))

    def __init__(
        self,
        material: Optional[str],
        material_composition: Optional[str],
        parts_of_animal_origin: Optional[str],
        materials: Optional[list[str]] = None,
    ):
        self.material = material
        self.material_composition = material_composition
        self.parts_of_animal_origin = parts_of_animal_origin
        if materials:
            seen = set()
            self.materials = [m for m in materials if m and not (m in seen or seen.add(m))]
        elif material:
            self.materials = [material]
        else:
            self.materials = []


def resolve_material_attributes(
    row: dict,
    title: str = "",
    description: str = "",
    cat_name: str = "",
) -> MaterialResolution:
    """
    Определяет (material, material_composition, parts_of_animal_origin) и список .materials.
    Строгая Precedence:
    1. Explicit CustomItemSpecifics composition
    2. Explicit material + deterministic composition rule
    3. Existing approved material resolver
    """
    # 1. Извлекаем нормализованные specifics
    if "CustomItemSpecifics" in row:
        specifics = parse_item_specifics(row.get("CustomItemSpecifics"))
    else:
        specifics = parse_item_specifics(row)

    # Приоритет 1: Явно заданный состав в CustomItemSpecifics (например "80% Polyester, 20% Baumwolle")
    explicit_comp_specific = get_first_specific(specifics, "material_composition")
    if explicit_comp_specific:
        parts_of_animal = "Yes" if "leder" in explicit_comp_specific.lower() and "kunstleder" not in explicit_comp_specific.lower() else "No"
        mat = get_first_specific(specifics, "material") or "Textil"
        return MaterialResolution(
            material=mat,
            material_composition=explicit_comp_specific,
            parts_of_animal_origin=parts_of_animal,
            materials=[mat]
        )

    # Приоритет 2 и 3: Эвристика по описанию, заголовку и материалам
    materials_list_raw = specifics.get("material", [])
    gestell_list_raw = specifics.get("gestellmaterial", [])
    
    if not materials_list_raw:
        # Фолбэк на старые сырые поля (на случай если мы работаем с сырым row)
        m1 = row.get("Material", "")
        m2 = row.get("material", "")
        m3 = row.get("Polsterstoff", "")
        materials_list_raw = [m for m in (m1, m2, m3) if m]
        
    if not gestell_list_raw:
        g = row.get("Gestellmaterial", "")
        gestell_list_raw = [g] if g else []

    mat_raw = " ".join(str(m) for m in materials_list_raw if m)
    gestell = " ".join(str(g) for g in gestell_list_raw if g)
    fuell = row.get("Füllmaterial", "")

    all_text = f"{mat_raw} {gestell} {fuell} {title} {description}".lower()

    upholstery_keywords = [
        "sofa", "couch", "sessel", "kinosessel", "polster", "stuhl", "stühle",
        "hocker", "liege", "garnitur", "ottomane", "ecksofa", "wohnlandschaft",
        "recamiere", "chaise", "polsterbett", "matratze", "kopfteil", "boxspring"
    ]
    is_upholstered = any(k in all_text or k in (cat_name or "").lower() for k in upholstery_keywords)

    frame_mat: Optional[str] = None
    gestell_lower = f"{gestell} {mat_raw}".lower()

    if any(x in gestell_lower or x in title.lower() for x in ["massivholz", "holz", "mdf", "spanplatte", "holzfurnier"]):
        frame_mat = "Holz"
    elif any(x in gestell_lower or x in title.lower() for x in ["metall", "stahl", "eisen", "aluminium", "edelstahl"]):
        frame_mat = "Metall"
    elif "glas" in gestell_lower or "glas" in title.lower():
        frame_mat = "Glas"
    elif any(x in gestell_lower or x in title.lower() for x in ["marmor", "stein"]):
        frame_mat = "Marmor"
    elif "kunststoff" in gestell_lower:
        frame_mat = "Kunststoff"

    upholstery_mat: Optional[str] = None
    material_composition: Optional[str] = None
    parts_of_animal_origin = "No"

    # Явный процентный состав в тексте
    explicit_comp = extract_explicit_composition(description) or extract_explicit_composition(title)
    if explicit_comp:
        upholstery_mat = "Textil"
        material_composition = explicit_comp
        parts_of_animal_origin = (
            "Yes" if "leder" in explicit_comp.lower() and "kunstleder" not in explicit_comp.lower() else "No"
        )
    elif "echtleder" in all_text or (
        "leder" in all_text and not any(x in all_text for x in ["kunstleder", "lederimitat", "pu-leder", "lederoptik"])
    ):
        upholstery_mat = "Leder"
        material_composition = "100% Leder"
        parts_of_animal_origin = "Yes"
    elif any(x in all_text for x in ["kunstleder", "lederimitat", "pu-leder", "lederoptik", "polyurethan"]):
        upholstery_mat = "Kunstleder"
        material_composition = "100% Polyurethan"
        parts_of_animal_origin = "No"
    elif "baumwolle" in all_text or "cotton" in all_text:
        upholstery_mat = "Baumwolle"
        material_composition = "100% Baumwolle"
        parts_of_animal_origin = "No"
    elif "leinen" in all_text or "flachs" in all_text:
        upholstery_mat = "Leinen"
        material_composition = "100% Leinen"
        parts_of_animal_origin = "No"
    elif "schurwolle" in all_text or "wolle" in all_text:
        upholstery_mat = "Wolle"
        material_composition = "100% Wolle"
        parts_of_animal_origin = "Yes"
    elif "seide" in all_text or "silk" in all_text:
        upholstery_mat = "Seide"
        material_composition = "100% Seide"
        parts_of_animal_origin = "Yes"
    elif "viskose" in all_text or "rayon" in all_text:
        upholstery_mat = "Viskose"
        material_composition = "100% Viskose"
        parts_of_animal_origin = "No"
    elif (
        bool(mat_raw)
        or any(x in all_text for x in ["samt", "velour", "velours", "microfaser", "mikrofaser", "textil", "polyester", "webstoff"])
        or bool(re.search(r"\b(stoff|polsterstoff|bezugsstoff|möbelstoff)\b", all_text))
    ):
        upholstery_mat = "Textil"
        material_composition = "100% Polyester"
        parts_of_animal_origin = "No"
    elif is_upholstered:
        upholstery_mat = "Textil"
        material_composition = "100% Polyester"
        parts_of_animal_origin = "No"

    if upholstery_mat in ["Stoff", "Textil", "Samt", "Mikrofaser", "Microfaser", "Webstoff", "Velours", "Velour"]:
        upholstery_mat = "Textil"

    materials_list: list[str] = []
    primary_material: Optional[str] = None

    if frame_mat and upholstery_mat and frame_mat != upholstery_mat:
        materials_list = [frame_mat]
        primary_material = frame_mat
    elif upholstery_mat:
        materials_list = [upholstery_mat]
        primary_material = upholstery_mat
    elif frame_mat:
        materials_list = [frame_mat]
        primary_material = frame_mat
        material_composition = None
        parts_of_animal_origin = "No"
    else:
        norm_raw = mat_raw.split()[0] if mat_raw else "Holz"
        if norm_raw in ["Stoff", "Textil", "Samt", "Mikrofaser", "Microfaser", "Webstoff", "Velours", "Velour"]:
            norm_raw = "Textil"
        materials_list = [norm_raw]
        primary_material = norm_raw

    return MaterialResolution(
        material=primary_material,
        material_composition=material_composition,
        parts_of_animal_origin=parts_of_animal_origin,
        materials=materials_list,
    )
