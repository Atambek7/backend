from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional, Dict, List
from urllib.parse import urlparse

from main_api.src.utils.ean_utils import normalize_ean
from main_api.src.utils.price_utils import parse_price
from main_api.services.material_service import (
    resolve_material_attributes,
    parse_item_specifics,
)
from main_api.src.utils.text_utils import sanitize_title


@dataclass
class CanonicalProduct:
    ean: str
    title: str
    description: str
    manufacturer: str
    price: float | int
    amount: int
    pictures: List[str]
    brand: Optional[str]
    material: Optional[str]
    materials: List[str]
    material_composition: Optional[str]
    parts_of_animal_origin: Optional[str]
    colour: List[str]
    width: Optional[str]
    length: Optional[str]
    height: Optional[str]
    source_file: str
    raw_specifics: Dict[str, List[str]]
    
    # Дополнительные поля для обратной совместимости
    pic_main: str = ""
    fabric: str = ""
    size: str = ""
    number_of_units: str = ""
    sitzhoehe: str = ""
    sitztiefe: str = ""
    anzahl_der_sitzplaetze: str = ""
    zimmer: str = ""
    stil: str = ""
    fuellmaterial: str = ""
    i_stammartikel: str = ""
    
    def as_dict(self) -> dict:
        return {
            "ean": self.ean,
            "article": self.title,
            "price": self.price,
            "amount": self.amount,
            "pic_main": self.pic_main,
            "pics": self.pictures,
            "fabric": self.fabric,
            "size": self.size,
            "color": self.colour[0] if self.colour else "",
            "colour": self.colour[0] if self.colour else "",
            "height": self.height or "",
            "length": self.length or "",
            "width": self.width or "",
            "material": self.material or "",
            "materials": self.materials,
            "material_composition": self.material_composition or "",
            "parts_of_animal_origin": self.parts_of_animal_origin or "",
            "number_of_units": self.number_of_units,
            "sitzhoehe": self.sitzhoehe,
            "sitztiefe": self.sitztiefe,
            "anzahl_der_sitzplaetze": self.anzahl_der_sitzplaetze,
            "zimmer": self.zimmer,
            "stil": self.stil,
            "fuellmaterial": self.fuellmaterial,
            "I_stammartikel": self.i_stammartikel,
            "manufacturer": self.manufacturer,
            "brand": self.brand or "",
        }


def _parse_price(value: Any) -> float | int:
    parsed = parse_price(value)
    return int(parsed) if parsed.is_integer() else parsed


def _normalize_ean(value: Any) -> str:
    digits = normalize_ean(value)
    return digits if len(digits) >= 8 else ""


def _guess_fabric(elem: dict) -> str:
    raw_url = elem.get("GalleryURL") or elem.get("PictureURL") or ""
    if not raw_url:
        return ""

    path_parts = [part for part in urlparse(str(raw_url)).path.split("/") if part]
    try:
        service_index = path_parts.index("AfterbuyUploadService")
    except ValueError:
        service_index = -1

    if service_index >= 0 and len(path_parts) > service_index + 2:
        return path_parts[service_index + 2]
    if service_index >= 0 and len(path_parts) > service_index + 1:
        return path_parts[service_index + 1]
    return path_parts[-2] if len(path_parts) >= 2 else ""


def _extract_dimensions(size_str: Any) -> tuple[str, str, str]:
    text = str(size_str or "").strip()
    if not text:
        return "", "", ""

    w_match = re.search(r"\bB(?:reite)?\s*[:=]?\s*(\d+(?:[,.]\d+)?)\s*(cm|m)?\b", text, re.IGNORECASE)
    l_match = re.search(r"\b(?:T(?:iefe)?|L(?:änge)?)\s*[:=]?\s*(\d+(?:[,.]\d+)?)\s*(cm|m)?\b", text, re.IGNORECASE)
    h_match = re.search(r"\bH(?:öhe)?\s*[:=]?\s*(\d+(?:[,.]\d+)?)\s*(cm|m)?\b", text, re.IGNORECASE)

    def to_cm(val_str: str, unit: str | None) -> str:
        if not val_str:
            return ""
        val = float(val_str.replace(",", "."))
        if unit and unit.lower() == "m":
            val *= 100
        return f"{int(val) if val.is_integer() else val} cm"

    if w_match or l_match or h_match:
        width = to_cm(w_match.group(1), w_match.group(2)) if w_match else ""
        length = to_cm(l_match.group(1), l_match.group(2)) if l_match else ""
        height = to_cm(h_match.group(1), h_match.group(2)) if h_match else ""
        return length, width, height

    match3 = re.search(
        r"(\d+(?:[,.]\d+)?)\s*(?:cm|m)?\s*[xX*×]\s*(\d+(?:[,.]\d+)?)\s*(?:cm|m)?\s*[xX*×]\s*(\d+(?:[,.]\d+)?)\s*(cm|m)?",
        text,
    )
    if match3:
        v1 = float(match3.group(1).replace(",", "."))
        v2 = float(match3.group(2).replace(",", "."))
        v3 = float(match3.group(3).replace(",", "."))
        unit = match3.group(4)
        if unit and unit.lower() == "m":
            v1 *= 100
            v2 *= 100
            v3 *= 100
        width_val = max(v1, v2)
        length_val = min(v1, v2)
        height_val = v3
        return (
            f"{int(length_val) if length_val.is_integer() else length_val} cm",
            f"{int(width_val) if width_val.is_integer() else width_val} cm",
            f"{int(height_val) if height_val.is_integer() else height_val} cm",
        )

    match2 = re.search(
        r"(\d+(?:[,.]\d+)?)\s*(?:cm|m)?\s*[xX*×]\s*(\d+(?:[,.]\d+)?)\s*(cm|m)?",
        text,
    )
    if match2:
        v1 = float(match2.group(1).replace(",", "."))
        v2 = float(match2.group(2).replace(",", "."))
        unit = match2.group(3)
        if unit and unit.lower() == "m":
            v1 *= 100
            v2 *= 100
        width_val = max(v1, v2)
        length_val = min(v1, v2)
        return (
            f"{int(length_val) if length_val.is_integer() else length_val} cm",
            f"{int(width_val) if width_val.is_integer() else width_val} cm",
            "",
        )

    return "", "", ""


def _get_first(specifics: Dict[str, List[str]], key: str) -> str:
    vals = specifics.get(key)
    return str(vals[0]).strip() if vals else ""


def map_canonical_product(elem: dict) -> CanonicalProduct:
    # 1. Парсинг CustomItemSpecifics через единый сервис
    specifics = parse_item_specifics(elem.get("CustomItemSpecifics") or {})
    
    title = str(elem.get("Artikelbeschreibung") or "")
    description = str(elem.get("Description") or elem.get("Artikelbeschreibung") or "")
    cat_name = str(elem.get("Produktart") or elem.get("CategoryID") or "")
    
    # 2. Определение EAN
    raw_ean = elem.get("EAN") or str(elem.get("Herstellernummer", "")).replace("JVM", "")
    ean = _normalize_ean(raw_ean)
    
    # 3. Определение производителя
    # source Hersteller / Manufacturer -> account default is done in upper layer.
    manufacturer = _get_first(specifics, "manufacturer")
    brand = _get_first(specifics, "brand")
    
    # 4. Резолвинг материалов
    mat_res = resolve_material_attributes(
        row={"CustomItemSpecifics": specifics}, # передаем распарсенные специфики
        title=title,
        description=description,
        cat_name=cat_name,
    )
    mat = mat_res.material
    mat_comp = mat_res.material_composition
    parts_animal = mat_res.parts_of_animal_origin
    all_mats = mat_res.materials
    
    title = sanitize_title(title, material=mat)
    
    # 5. Размеры
    width = _get_first(specifics, "width") or str(elem.get("Breite", ""))
    length = _get_first(specifics, "length") or str(elem.get("Länge", ""))
    height = _get_first(specifics, "height") or str(elem.get("Höhe", ""))
    
    size_raw = elem.get("Maße", "")
    if not size_raw:
        # Fallback to look in specifics
        for k, v in specifics.items():
            if "maß" in k.lower() and v:
                size_raw = str(v[0])
                break

    if not (width and length and height):
        parsed_l, parsed_w, parsed_h = _extract_dimensions(size_raw)
        if not length and parsed_l: length = parsed_l
        if not width and parsed_w: width = parsed_w
        if not height and parsed_h: height = parsed_h

    # 6. Остальные атрибуты
    colour = specifics.get("colour", [])
    if not colour and elem.get("Farbe"):
        colour = [str(elem.get("Farbe")).strip()]

    stock_val = None
    stock_keys = ("amount", "Menge", "menge", "Bestand", "bestand", "stock", "quantity")
    for k in stock_keys:
        if k in elem and elem[k] is not None and str(elem[k]).strip() != "":
            stock_val = elem[k]
            break

    stock = 20
    if stock_val is not None:
        try:
            parsed_stock = int(round(float(str(stock_val).strip().replace(",", "."))))
            stock = max(0, parsed_stock)
        except (ValueError, TypeError):
            stock = 20

    pics = []
    if isinstance(elem.get("pictureurls", ""), str):
        pics = [p for p in elem.get("pictureurls", "").split(";") if p]
    elif isinstance(elem.get("pictureurls"), list):
        pics = [p for p in elem.get("pictureurls") if p]

    # Для обратной совместимости 
    canonical = CanonicalProduct(
        ean=ean,
        title=title,
        description=description,
        manufacturer=manufacturer,
        brand=brand,
        material=mat,
        materials=all_mats,
        material_composition=mat_comp,
        parts_of_animal_origin=parts_animal,
        colour=colour,
        width=width,
        length=length,
        height=height,
        pictures=pics,
        source_file=str(elem.get("source_file", "")),
        raw_specifics=specifics,
        price=_parse_price(elem.get("Startpreis", 0)),
        amount=stock,
        pic_main=str(elem.get("GalleryURL") or elem.get("PictureURL") or ""),
        fabric=str(elem.get("Fabric") or _guess_fabric(elem)),
        size=str(size_raw),
        number_of_units=str(elem.get("Anzahl der Teile", "")),
        sitzhoehe=str(elem.get("Sitzhöhe", "")),
        sitztiefe=str(elem.get("Sitztiefe", "")),
        anzahl_der_sitzplaetze=str(elem.get("Anzahl der Sitzplätze", "")),
        zimmer=str(elem.get("Zimmer", "")),
        stil=str(elem.get("Stil", "")),
        fuellmaterial=str(elem.get("Füllmaterial", "")),
        i_stammartikel=str(elem.get("I_stammartikel", "")),
    )
    
    return canonical


def map_json_item(elem: dict) -> dict:
    """Для обратной совместимости, возвращает словарь, но уже нормализованный через CanonicalProduct."""
    return map_canonical_product(elem).as_dict()

def normalize_json_item(elem: dict) -> dict:
    """Для обратной совместимости."""
    return map_canonical_product(elem).as_dict()
