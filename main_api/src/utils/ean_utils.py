"""
Единые утилиты нормализации и валидации EAN-штрихкодов маркетплейса Kaufland.
"""
from __future__ import annotations

import re
from typing import Any


def normalize_ean(value: Any) -> str:
    """
    Нормализует EAN:
    - Сохраняет ведущие нули (например, '0401234567890.0' -> '0401234567890').
    - Удаляет артефакты форматирования: суффикс '.0', пробелы, нецифровые символы.
    - Обрабатывает списки: берет первый элемент.
    - Удаляет префикс поставщика 'JVM' (если есть).
    - Безопасно обрабатывает экспоненциальную запись (например, '4.26055E+12' -> '4260550000000').
    - Возвращает пустую строку при None, 'nan', 'null', 'none'.
    """
    if value is None:
        return ""
    if isinstance(value, list):
        value = value[0] if value else ""
    text = str(value).strip()
    if not text or text.lower() in ("nan", "none", "null"):
        return ""
    if text.startswith("JVM"):
        text = text[3:]
    if text.endswith(".0"):
        text = text[:-2]
    if ("e" in text.lower() or "." in text) and not text.isdigit():
        try:
            text = f"{int(float(text))}"
        except (ValueError, TypeError):
            pass
    digits = re.sub(r"\D", "", text)
    return digits


def is_valid_ean(value: Any) -> bool:
    """
    Проверяет валидность штрихкода (длина 8, 13 или 14 цифр и контрольная сумма GS1).
    """
    clean = normalize_ean(value)
    if not clean.isdigit() or len(clean) not in (8, 13, 14):
        return False
    digits = [int(d) for d in clean]
    weights = [3, 1] * 7
    total = sum(d * w for d, w in zip(digits[:-1][::-1], weights))
    check = (10 - (total % 10)) % 10
    return check == digits[-1]
