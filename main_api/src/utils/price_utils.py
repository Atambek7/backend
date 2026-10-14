"""
Единые утилиты парсинга, валидации и конвертации цен маркетплейса Kaufland.
"""
from __future__ import annotations

import re
from typing import Any


def parse_price(value: Any, default: Any = 0.0) -> Any:
    """
    Парсит цену в float (в евро или базовой валюте витрины).
    Корректно обрабатывает:
    - Числа (int, float)
    - Европейский формат: '1.299,00' -> 1299.0, '49,99' -> 49.99
    - Американский формат: '1,299.00' -> 1299.0
    - Символы валюты: '49,99 €', '1000 zł'
    - Строки с пробелами и неразрывными пробелами
    - Невалидные значения -> default (по умолчанию 0.0)
    """
    if value is None or isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip().replace("\u00a0", "").replace(" ", "")
    if not text or text.lower() in ("nan", "none", "null"):
        return default

    # Удаляем знаки валют и лишние символы
    text = re.sub(r"[€$£złKč]", "", text)

    # Обработка разделителей
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            # Европейский формат: 1.299,00 -> 1299.00
            text = text.replace(".", "").replace(",", ".")
        else:
            # Американский формат: 1,299.00 -> 1299.00
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")

    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return default
    try:
        return float(match.group(0))
    except (ValueError, TypeError):
        return default


def format_price_cents(price: float | int | str) -> int:
    """
    Конвертирует цену в центы для Kaufland API.
    По требованию бизнеса: без искусственного округления до .99.
    1000.00 EUR -> 100000 центов (1000,00 €).
    49.99 EUR -> 4999 центов.
    """
    numeric = parse_price(price) if isinstance(price, str) else float(price)
    return int(round(numeric * 100))
