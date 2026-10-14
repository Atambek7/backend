from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "kaufland_leaf_categories.json"

_COMPILED_RULES: Optional[List[dict]] = None


def _load_rules() -> List[dict]:
    global _COMPILED_RULES
    if _COMPILED_RULES is not None:
        return _COMPILED_RULES

    if not CONFIG_PATH.is_file():
        logger.warning(f"Category config file not found: {CONFIG_PATH}")
        _COMPILED_RULES = []
        return _COMPILED_RULES

    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            raw_rules = json.load(f)

        # Сортируем правила строго по убыванию приоритета
        raw_rules.sort(key=lambda r: r.get("priority", 0), reverse=True)

        compiled = []
        for r in raw_rules:
            exact_regexes = []
            for pat in r.get("exact_match_patterns", []):
                try:
                    exact_regexes.append(re.compile(pat, flags=re.IGNORECASE))
                except re.error as e:
                    logger.warning(f"Invalid regex '{pat}' in category '{r.get('name')}': {e}")

            r_copy = dict(r)
            r_copy["_compiled_exact"] = exact_regexes
            compiled.append(r_copy)

        _COMPILED_RULES = compiled
        return _COMPILED_RULES
    except Exception as exc:
        logger.error(f"Failed to load category rules from {CONFIG_PATH}: {exc}")
        _COMPILED_RULES = []
        return _COMPILED_RULES


def match_leaf_category(title: str) -> Optional[Tuple[int, str, str]]:
    """
    Классифицирует название товара по строгому набору правил из kaufland_leaf_categories.json.
    Возвращает (id_category, name, title_de) или None, если совпадений не найдено.
    """
    if not title:
        return None

    t = title.lower().strip()
    rules = _load_rules()

    for rule in rules:
        # 1. Проверяем исключения (exclude_keywords)
        excludes = rule.get("exclude_keywords", [])
        has_excluded = False
        for ex in excludes:
            ex_low = ex.lower().strip()
            if not ex_low:
                continue
            # Если исключение это короткое слово <= 4 символов, проверяем по границе слова
            if len(ex_low) <= 4:
                if re.search(rf"\b{re.escape(ex_low)}\b", t):
                    has_excluded = True
                    break
            else:
                if ex_low in t:
                    has_excluded = True
                    break

        if has_excluded:
            continue

        # 2. Проверяем скомпилированные регулярные выражения (exact_match_patterns)
        for rx in rule.get("_compiled_exact", []):
            if rx.search(t):
                return (rule["id_category"], rule["name"], rule["title_de"])

        # 3. Проверяем включения (include_keywords)
        includes = rule.get("include_keywords", [])
        for inc in includes:
            inc_low = inc.lower().strip()
            if not inc_low:
                continue
            # Словосочетания или слова с дефисом
            if " " in inc_low or "-" in inc_low:
                if inc_low in t:
                    return (rule["id_category"], rule["name"], rule["title_de"])
            else:
                # Одиночные слова проверяем строго по границе слова, чтобы 'schrank' не цеплял 'weinschrank'
                if re.search(rf"\b{re.escape(inc_low)}\b", t):
                    return (rule["id_category"], rule["name"], rule["title_de"])

    return None
