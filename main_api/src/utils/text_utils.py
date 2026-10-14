"""
Утилиты для очистки и санитаризации текстов (заголовков и описаний) для Kaufland Seller API.
"""
from __future__ import annotations

import re
from typing import Optional

# Регулярное выражение для промо- и стоп-слов, запрещенных в названиях товаров на Kaufland
STOP_WORDS_RE = re.compile(
    r"\b(Neu|New|Top|Sale|Super|Günstig|Aktion|Sonderangebot|Reduziert|Schnäppchen|Angebot|Gratis|Rabatt|Bestseller|Hit)\b!?",
    re.IGNORECASE,
)

# Control characters (кроме newline \n, return \r, tab \t)
CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")

# Диапазоны символов Emoji
EMOJI_RE = re.compile(
    r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF"
    r"\U0001F1E0-\U0001F1FF\U00002702-\U000027B0\U0001F900-\U0001F9FF"
    r"\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF\U00002600-\U000026FF]"
)

DEFAULT_FURNITURE_PADDING = (
    "Hochwertige Möbel für Ihr Zuhause. Qualität und Design vereint. "
    "Dieses Produkt überzeugt durch sorgfältige Verarbeitung, "
    "ansprechendes Design und langlebige Materialien für höchsten Wohnkomfort."
)


def sanitize_title(title: Optional[str], material: Optional[str] = None) -> str:
    """
    Очистка заголовка товара для каталога Kaufland:
    1. Удаляет промо-слова (Neu!, Sale, Top, etc.).
    2. Удаляет все знаки '!'.
    3. Разрешает конфликты материалов (Leder vs Textil), если материал указан и не является комбинацией.
    4. Нормализует формы диванов: 'U Form' -> 'U-Form', 'L Form' -> 'L-Form'.
    5. Удаляет висячие одиночные буквы-артефакты (r, s, e, etc.) в начале, середине и конце названия.
    6. Устраняет множественные пробелы и ведущие/замыкающие знаки пунктуации (-, :, /, etc.).
    7. Делает первую букву заглавной (без занижения регистра остальных букв).
    8. Обрезает до 255 символов по границе слова.
    9. Функция полностью идемпотентна: f(f(x)) == f(x).
    """
    if not title or not isinstance(title, str):
        return ""

    text = title.strip()
    if not text:
        return ""

    # 1. Удаление стоп-слов
    text = STOP_WORDS_RE.sub(" ", text)

    # 2. Удаление всех знаков '!'
    text = text.replace("!", "")

    # 3. Разрешение конфликтов материалов в заголовке (только если нет комбинации обоих материалов)
    if material:
        mat_str = str(material).lower().strip()
        has_leder = "leder" in mat_str
        has_textil = "textil" in mat_str or "stoff" in mat_str
        if has_leder and not has_textil:
            # Если материал строго кожа, убираем из заголовка упоминание Textil / Stoff
            text = re.sub(r"\b(Textil|Stoff)\b", " ", text, flags=re.IGNORECASE)
        elif has_textil and not has_leder:
            # Если материал строго ткань/текстиль, убираем из заголовка Leder / Kunstleder / Echtleder
            text = re.sub(r"\b(Leder|Kunstleder|Echtleder)\b", " ", text, flags=re.IGNORECASE)

    # 4. Нормализация форм мебели: "U Form", "u-form", "U - Form", "L Form", "l form" -> "U-Form", "L-Form"
    text = re.sub(r"(?i)\b([ultv])\s*[-–—]?\s*form\b", lambda m: f"{m.group(1).upper()}-Form", text)

    # 5. Удаление изолированных одиночных букв-паразитов (например, "r Weißer", "Einfarbiger r Sessel", "s Sofa", "(r)")
    # Сохраняем 'x' / 'X', если используется как размер/множитель (например, "4 x Stuhl", "200 x 90 cm")
    def _repl_single(m: re.Match) -> str:
        tok = re.sub(r"[^a-zA-ZäöüßÄÖÜ]", "", m.group(0))
        if tok.lower() == "x":
            return m.group(0)
        return " "

    text = re.sub(r"(?<!\S)[\"'„“»«(\[]?[a-zA-ZäöüßÄÖÜ][.,;:/\–—\"'„“»«)\]]?(?!\S)", _repl_single, text)

    # 6. Схлопывание пробелов и зачистка висячей лидирующей пунктуации и остаточных одиночных букв
    text = re.sub(r"\s+", " ", text).strip()
    while True:
        prev = text
        text = re.sub(r"^[\s\-–—:/,;.\"'„“»«()\[\]]+", "", text).strip()
        text = re.sub(r"^(?:[\"'„“»«(\[]?[a-zA-ZäöüßÄÖÜ][.,;:/\–—\"'„“»«)\]]?(?:\s+[-–—:/,;.]*\s*|$))+", "", text).strip()
        if text == prev:
            break

    # 7. Зачистка висячих кавычек в конце строки
    text = re.sub(r"[\"'„“»«]+$", "", text).strip()

    # 8. Первая буква заглавная
    if text:
        text = text[0].upper() + text[1:]

    # 9. Ограничение длины до 255 символов (Kaufland API limit)
    text = text[:255].strip()

    return text


def consolidate_description_sections(html_str: str) -> str:
    """
    Консолидирует разрозненные секции описания (в частности размеры Maße, разбитые на части):
    1. Находит все ключевые заголовки секций (Maße, Produktbeschreibung, Produktmerkmale, Farbe, Material и т.д.).
    2. Если размеры товара разбиты на несколько блоков (например, Hochbett сверху, а Schreibtisch и Kleiderschrank снизу),
       собирает ВСЕ блоки размеров в единый сводный блок <strong>Maße:</strong> со всеми подразделами.
    3. Выстраивает правильную структуру описания:
       - Если присутствует Produktbeschreibung: сначала идет вводное описание, затем Produktmerkmale,
         затем Farbe/Material, и в конце - единый блок Maße.
       - Если Produktbeschreibung отсутствует (простые товары): блок Maße остается на первом месте.
    """
    if not html_str or len(html_str) < 50:
        return html_str

    header_pattern = re.compile(
        r'(<(?P<tag>strong|b|h[1-6])\b[^>]*>\s*'
        r'(?P<title>Ma[ßs]e:?|Abmessungen:?|Produktbeschreibung:?|Beschreibung:?|Produktmerkmale:?|Merkmale:?|Eigenschaften:?|Farbe:?|Farben:?|Material:?|Materialien:?|Lieferumfang:?|Ausführung:?)'
        r'\s*</(?P=tag)>)',
        re.IGNORECASE,
    )

    splits = list(header_pattern.finditer(html_str))
    if not splits:
        return html_str

    sections = []
    if splits[0].start() > 0:
        pre_content = html_str[:splits[0].start()].strip()
        if pre_content:
            sections.append(('pre', '', pre_content))

    for i, match in enumerate(splits):
        title = match.group('title').strip()
        start = match.end()
        end = splits[i + 1].start() if i + 1 < len(splits) else len(html_str)
        content = html_str[start:end].strip()

        title_norm = re.sub(r'[:\s]+$', '', title).lower()
        if re.search(r'ma[ßs]e|abmessungen', title_norm):
            cat = 'dimensions'
        elif re.search(r'produktbeschreibung|beschreibung', title_norm):
            cat = 'desc'
        elif re.search(r'produktmerkmale|merkmale|eigenschaften', title_norm):
            cat = 'features'
        elif re.search(r'farbe', title_norm):
            cat = 'color'
        elif re.search(r'material', title_norm):
            cat = 'material'
        elif re.search(r'lieferumfang', title_norm):
            cat = 'scope'
        elif re.search(r'ausführung', title_norm):
            cat = 'notes'
        else:
            cat = 'other'

        sections.append((cat, title, content))

    dim_sections = [s for s in sections if s[0] == 'dimensions']
    desc_sections = [s for s in sections if s[0] == 'desc']

    # Порядок секций строго по требованию:
    # 1. Maße (все размеры сведены воедино)
    # 2. Farbe
    # 3. Material (если есть отдельной секцией)
    # 4. Produktmerkmale (список характеристик с маркерами '-')
    # 5. Produktbeschreibung (основной текст описания)
    # 6. Lieferumfang / Ausführung / прочие примечания
    ordered_cats = ['dimensions', 'color', 'material', 'features', 'desc', 'scope', 'notes', 'other']

    distinct_cats = [s[0] for s in sections if s[0] != 'pre']
    expected_order = sorted(distinct_cats, key=lambda c: ordered_cats.index(c) if c in ordered_cats else 99)

    # Если нет нескольких блоков размеров и все секции уже идут в правильном порядке, переставлять не требуется
    if len(dim_sections) <= 1 and distinct_cats == expected_order:
        return html_str

    consolidated_dim_items = []
    for cat, title, content in dim_sections:
        c = re.sub(r'^(?:<br\s*/?>\s*)+', '', content)
        c = re.sub(r'(?:<br\s*/?>\s*)+$', '', c)
        c = re.sub(r'</div>$', '', c).strip()
        if c:
            # Унифицируем подзаголовки внутри Maße, добавляя двоеточие: <strong>Hochbett</strong> -> <strong>Hochbett:</strong>
            c = re.sub(r'<strong>([A-Za-z0-9äöüÄÖÜ\s-]+)(?<!:)</strong>', r'<strong>\1:</strong>', c)
            consolidated_dim_items.append(c)

    merged_dim_html = '<br/><br/>'.join(consolidated_dim_items)

    result_parts = []
    pre_text = ""
    for cat, title, content in sections:
        if cat == 'pre':
            clean_pre = re.sub(r'^<div\b[^>]*>', '', content).strip()
            clean_pre = re.sub(r'</div>$', '', clean_pre).strip()
            clean_pre = re.sub(r'(?:<br\s*/?>\s*)+$', '', clean_pre)
            if clean_pre:
                pre_text = clean_pre

    # Если нет структурированных секций (dimensions, color, features, desc), сохраняем pre_text в начале
    has_structured = any(s[0] in ('dimensions', 'color', 'features', 'desc') for s in sections)
    if pre_text and not has_structured:
        result_parts.append(pre_text)

    for target_cat in ordered_cats:
        if target_cat == 'dimensions':
            if merged_dim_html:
                result_parts.append(f'<strong>Maße:</strong><br/><br/>{merged_dim_html}')
        else:
            for cat, title, content in sections:
                if cat == target_cat:
                    has_leading_br = bool(re.match(r'^(?:<br\s*/?>|\s*[\r\n])', content))
                    c = re.sub(r'^(?:<br\s*/?>\s*)+', '', content)
                    c = re.sub(r'(?:<br\s*/?>\s*)+$', '', c)
                    c = re.sub(r'</div>$', '', c).strip()

                    # В Produktmerkmale форматируем маркеры списков через '-'
                    if target_cat == 'features':
                        c = re.sub(r'(?m)^[•*]\s*', '- ', c)
                        c = re.sub(r'(?:<br\s*/?>\s*)[•*]\s*', '<br/>- ', c)

                    # Если был pre_text и это секция описания, добавляем pre_text в начало Produktbeschreibung
                    if target_cat == 'desc' and pre_text:
                        c = f'{pre_text}<br/>{c}'
                        pre_text = ""

                    t_display = title if title.endswith(':') else f'{title}:'
                    separator = '<br/>' if has_leading_br else ' '
                    result_parts.append(f'<strong>{t_display}</strong>{separator}{c}')

    final_html = '<br/><br/>'.join(result_parts)
    final_html = re.sub(r'^<div>', '', final_html)
    final_html = re.sub(r'</div>$', '', final_html)
    final_html = f'<div>{final_html.strip()}</div>'
    return final_html


def sanitize_description(text: Optional[str], material: Optional[str] = None, min_length: int = 150) -> str:
    """
    Санитаризация описания товара для Kaufland Seller API:
    1. Удаляет управляющие символы (control characters).
    2. Удаляет emoji.
    3. Удаляет запрещенные атрибуты тегов (style, class, id, onclick).
    4. Удаляет промо-слова (Neu!, Sale, Top, etc.) и восклицательные знаки '!'.
    5. Разрешает конфликты материалов (Leder vs Textil), если передан конкретный материал.
    6. Приводит первую букву описания к заглавной.
    7. Оборачивает в <p>...</p>, если HTML-теги отсутствуют.
    8. Если длина чистого текста < min_length, добавляет нейтральный мебельный текст-дополнение.
    9. Функция полностью идемпотентна: f(f(x)) == f(x).
    """
    if text is None:
        raw_text = ""
    else:
        raw_text = str(text)

    # 1. Удаление control characters
    cleaned = CONTROL_CHARS_RE.sub("", raw_text)

    # 2. Удаление emoji
    cleaned = EMOJI_RE.sub("", cleaned).strip()

    # 3. Удаление запрещенных HTML-атрибутов (style, class, id, onclick и т.д.)
    cleaned = re.sub(
        r'\s+(?:style|class|id|onclick|onload|target)\s*=\s*(?:"[^"]*"|\'[^\']*\'|[^\s>]+)',
        '',
        cleaned,
        flags=re.IGNORECASE
    )

    # 4. Удаление промо-слов и знаков '!'
    cleaned = STOP_WORDS_RE.sub(" ", cleaned)
    cleaned = cleaned.replace("!", "")

    # 4.1 Разрешение конфликтов материалов (Leder vs Textil)
    if material:
        mat_str = str(material).lower().strip()
        has_leder = "leder" in mat_str
        has_textil = "textil" in mat_str or "stoff" in mat_str
        if has_leder and not has_textil:
            cleaned = re.sub(r"\b(Textil|Stoff)\b", " ", cleaned, flags=re.IGNORECASE)
        elif has_textil and not has_leder:
            cleaned = re.sub(r"\b(Leder|Kunstleder|Echtleder)\b", " ", cleaned, flags=re.IGNORECASE)

    # 4.2 Удаление внешних картинок (<img>) и преобразование ссылок (<a>) в простой текст
    cleaned = re.sub(r"<img\b[^>]*>", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"<a\b[^>]*>(.*?)</a>", r"\1", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"https?://[^\s<>'\"`]+", "", cleaned)
    cleaned = re.sub(r"\bwww\.[^\s<>'\"`]+\.[a-z]{2,}\b", "", cleaned, flags=re.IGNORECASE)

    # 4.3 Удаление упоминаний аукционов и eBay
    cleaned = re.sub(r"(?i)\bsie\s+bieten\s+in\s+dieser\s+auktion\s+auf\s+(?:das|den|die)?\s*", "Das ", cleaned)
    cleaned = re.sub(
        r"(?i)\b(?:in\s+dieser\s+auktion|in\s+unserem\s+ebay[- ]?shop|unser\s+ebay[- ]?shop|ebay[- ]?shop|unsere\s+auktionen|weitere\s+auktionen|auf\s+ebay|bei\s+ebay|\bebay\b|\bauktion(?:en)?\b|mitbieten|mindestgebot)\b",
        "",
        cleaned,
    )

    # 4.4 Удаление апсейлов с ценами в евро внутри описания
    cleaned = re.sub(
        r"(?i)\b(?:passend(?:er|e|es)?\s+[a-zA-ZäöüßÄÖÜ\s]+)?\b\d+(?:[.,]\d+)?\s*(?:euro|eur|€)\b",
        "",
        cleaned,
    )

    # 4.5 Удаление условий доставки, склада и самовывоза
    cleaned = re.sub(r"(?i)\(?\s*lagerware\s*\)?", "", cleaned)
    cleaned = re.sub(
        r"(?i)\b(?:lieferung\s*(?:erfolgt\s*)?(?:per|mit)\s*spedition|bis\s*bordsteinkante|selbstabholung(?:\s*möglich)?)\b",
        "",
        cleaned,
    )

    # 4.6 Удаление обрезков вариантов
    cleaned = re.sub(r"(?i)<strong>\s*[a-z0-9]\s*\)\s*</strong>", "", cleaned)
    cleaned = re.sub(r"(?i)\bvariante\s*\d+\s*:\s*[a-zA-ZäöüßÄÖÜ\s/]+", "", cleaned)

    # 4.7 Удаление контактов продавца (email и телефоны)
    cleaned = re.sub(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", "", cleaned)
    cleaned = re.sub(r"(?i)(?:tel(?:efon)?|fon|mobil)?\s*:?\s*(?:\+?49\s*\(?0\)?|\b0)[0-9\s\-\/]{7,15}\b", "", cleaned)

    # 4.8 Консолидация разрозненных секций размеров (Maße) и упорядочивание секций
    cleaned = consolidate_description_sections(cleaned)

    # Зачистка множественных переходов строк и пустых блоков
    cleaned = re.sub(r"(?:<br\s*/?>\s*){3,}", "<br/><br/>", cleaned)
    cleaned = re.sub(r"<p\b[^>]*>\s*</p>", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"<div\b[^>]*>\s*</div>", "", cleaned, flags=re.IGNORECASE)

    # 5. Проверка наличия HTML-тегов
    has_html = bool(re.search(r"<[a-zA-Z]", cleaned))
    if not has_html:
        cleaned = f"<p>{cleaned.strip()}</p>" if cleaned.strip() else "<p></p>"

    # 6. Приведение первой буквы текста внутри первого <p> или в начале строки к заглавной
    def _cap_start(m):
        return f"{m.group(1)}{m.group(2).upper()}{m.group(3)}"

    cleaned = re.sub(r"(<p\b[^>]*>\s*)([a-zäöü])(.*)", _cap_start, cleaned, count=1, flags=re.DOTALL)
    m_plain = re.match(r"^(\s*)([a-zäöü])(.*)$", cleaned, flags=re.DOTALL)
    if m_plain:
        cleaned = f"{m_plain.group(1)}{m_plain.group(2).upper()}{m_plain.group(3)}"

    # 7. Проверка минимальной длины чистого текста
    plain_content = re.sub(r"<[^>]+>", "", cleaned).strip()
    plain_content = re.sub(r"\s+", " ", plain_content)

    if len(plain_content) < min_length and DEFAULT_FURNITURE_PADDING not in cleaned:
        if not plain_content:
            padding_to_add = DEFAULT_FURNITURE_PADDING
        else:
            padding_to_add = f" {DEFAULT_FURNITURE_PADDING}"

        # Если есть закрывающий тег </p>, вставляем дополнение перед последним </p>
        if "</p>" in cleaned:
            parts = cleaned.rsplit("</p>", 1)
            cleaned = f"{parts[0]}{padding_to_add}</p>{parts[1]}"
        else:
            cleaned = f"{cleaned}{padding_to_add}"

    # Схлопывание лишних пробелов с сохранением переносов/тегов
    cleaned = re.sub(r"[ \t]+", " ", cleaned).strip()
    return cleaned


# Черный список категорий (бытовая химия CLP, продукты питания LMIV, косметика, бытовая техника)
CATEGORY_BLACKLIST_PREFIXES = [
    # Опасная химия и чистящие средства (CLP Регламент)
    "Entkalker", "WC-Reiniger", "Kaffeemaschinenreiniger",
    "Reinigungsmittel", "Reiniger", "Putzmittel", "Desinfektionsmittel",
    # Продукты питания и напитки (LMIV Регламент)
    "Pralinen", "Roséwein", "Backmischungen", "Lutscher",
    "Fertigmahlzeiten", "Babynahrung", "Lebensmittel", "Nahrungsmittel",
    "Süßwaren", "Getränke", "Wein", "Bier", "Spirituosen", "Nudeln", "Pasta", "Teigwaren",
    # Косметика и гигиена
    "Feuchtigkeitscremes", "Seifen", "Badeöle", "Maniküre",
    "Kosmetik", "Körperpflege", "Hautpflege",
    # Крупная и мелкая бытовая электротехника (требующая энергопаспорта/WEEE)
    "Kühlschränke", "Geschirrspüler",
    "Waschmaschinen", "Trockner", "Staubsauger", "Wandhauben", "Dunstabzugshauben",
    # Мелкая техника и уход за волосами/телом (курьезы AI Kaufland)
    "Glätteisen", "Haarglätter", "Lockenstäbe", "Haartrockner", "Föhne",
    "Rasierer", "Epilierer", "Bügeleisen", "Dampfbügeleisen",
    "Toaster", "Kaffeemaschinen", "Wasserkocher", "Mikrowellen", "Mixer",
    # Немебельные курьезы из отчетов
    "Weihnachtsmusik", "Gießkannen", "Parfüm", "Parfuem", "Skate", "Fix-Produkte", "Babydecken",
    # Общие мусорные категории
    "Sonstiges",
]


def _normalize_umlauts(text: str) -> str:
    return (
        str(text or "")
        .lower()
        .replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )


def is_blacklisted_category(cat_name: Optional[str], title: Optional[str] = None) -> bool:
    """
    Проверяет, входит ли категория в жесткий черный список немебельных товаров.
    Учитывает как немецкие умлауты (ä, ö, ü), так и транслитерацию (ae, oe, ue).
    Также блокирует ложные 'Круглые кровати' (Round beds), если в названии товара нет слова 'rund'.
    """
    if not cat_name or not isinstance(cat_name, str):
        return False
    cat_norm = _normalize_umlauts(cat_name).strip()

    # Мусорная категория "Sonstiges"
    if cat_norm in ("sonstiges", "other"):
        return True

    # Ложные круглые кровати
    if ("round bed" in cat_norm or "rundbett" in cat_norm) and title:
        title_norm = _normalize_umlauts(str(title))
        if "rund" not in title_norm and "round" not in title_norm:
            return True

    for bl in CATEGORY_BLACKLIST_PREFIXES:
        bl_norm = _normalize_umlauts(bl)
        if bl_norm == "seifen":
            # Исключаем дозаторы и мыльницы (Seifenspender & Seifenschalen - аксессуары для ванной)
            if re.search(r"\bseifen\b", cat_norm) and "seifenspender" not in cat_norm and "seifenschale" not in cat_norm:
                return True
        elif bl_norm in cat_norm:
            return True
    return False


def resolve_fallback_category(title: Optional[str]) -> tuple[int, str]:
    """
    Детерминированный эвристический роутер мебельных категорий Kaufland по названию товара.
    Возвращает пару (id_category, name) проверенной листовой категории мебели.
    """
    if not title or not isinstance(title, str):
        return (5961, "couchgarnituren")

    t = title.lower()

    # 1. Столы и кофейные столики (Couchtisch должен попадать в столики, а не в диваны)
    if any(k in t for k in ["tisch", "couchtisch", "beistelltisch", "esstisch", "schreibtisch", "bartisch"]):
        if any(k in t for k in ["couchtisch", "beistelltisch", "kaffeetisch", "wohnzimmertisch"]):
            return (3541, "beistelltische")
        return (3481, "esszimmer")

    # 2. Диваны и мягкие гарнитуры
    if any(k in t for k in ["sofa", "couch", "wohnlandschaft", "recamiere", "ottomane"]):
        if "eck" in t or "l-form" in t or "u-form" in t:
            return (5961, "couchgarnituren")
        return (45081, "klassische-sofas")

    # 3. Кровати и матрасы
    if any(k in t for k in ["bett", "matratze", "boxspring", "futon", "schlafsystem"]):
        return (2751, "polsterbetten")

    # 4. Кресла и шезлонги
    if any(k in t for k in ["sessel", "fauteuil", "relaxsessel", "loungesessel", "ohrensessel", "cocktailsessel", "liege"]):
        return (5831, "cocktailsessel")

    # 5. Стулья и табуреты
    if any(k in t for k in ["stuhl", "stühle", "stuehle", "hocker", "bank", "sitzbank", "barhocker"]):
        return (4211, "esszimmerstuehle")

    # 6. Комоды и тумбы
    if any(k in t for k in ["kommode", "sideboard", "highboard", "lowboard", "nachtkonsole", "nachttisch", "tv-board", "board"]):
        return (6141, "schubladenschraenke")

    # 7. Шкафы и гардеробы
    if any(k in t for k in ["schrank", "kleiderschrank", "garderobe", "vitrine", "regal", "wandregal"]):
        return (3271, "schlafzimmer")

    # Дефолт для общих мебельных товаров при отсутствии совпадений
    return (46001, "Möbel")


LOCALIZED_FURNITURE_DESCRIPTIONS = {
    "de-DE": "Hochwertige Möbel für Ihr Zuhause. Qualität und Design vereint. Dieses Produkt überzeugt durch sorgfältige Verarbeitung, ansprechendes Design und langlebige Materialien für höchsten Wohnkomfort.",
    "fr-FR": "Meuble de haute qualité pour votre intérieur. Alliant esthétique et robustesse, ce produit séduit par sa fabrication soignée, son design élégant et ses matériaux durables pour un confort optimal.",
    "cs-CZ": "Vysoce kvalitní nábytek pro váš domov. Spojení elegance a odolnosti. Tento produkt zaujme pečlivým zpracováním, moderním designem a odolnými materiály pro maximální pohodlí.",
    "sk-SK": "Vysoko kvalitný nábytok pre váš domov. Spojenie elegancie a odolnosti. Tento produkt zaujme starostlivým spracovaním, moderným dizajnom a odolnými materiálmi pre maximálne pohodlie.",
    "pl-PL": "Wysokiej jakości mebel do Twojego domu. Połączenie estetyki i funkcjonalności. Produkt wyróżnia się starannym wykonaniem, eleganckim designem oraz trwałymi materiałami zapewniającymi maksymalny komfort.",
    "it-IT": "Mobile di alta qualità per la tua casa. Perfetta combinazione di estetica e robustezza. Questo prodotto convince per la lavorazione accurata, il design elegante e i materiali resistenti per il massimo comfort.",
    "nl-NL": "Hoogwaardig meubelstuk voor uw interieur. Kwaliteit en design verenigd. Dit product overtuigt door zorgvuldige afwerking, aantrekkelijk design en duurzame materialen voor optimaal wooncomfort.",
}


def generate_localized_description(
    title: str,
    locale: str = "de-DE",
    base_description: Optional[str] = None,
    material: Optional[str] = None,
    min_length: int = 150,
) -> str:
    """
    Генерирует семантически корректное, валидное HTML-описание товара для целевой локали.
    1. Для de-DE: если передано базовое описание, санитаризирует его.
    2. Для зарубежных локалей (fr-FR, cs-CZ, sk-SK, pl-PL, it-IT, nl-NL):
       формирует структурированное описание на языке витрины с заголовком и текстом характеристик.
    3. Оборачивает в <p>...</p>, гарантирует длину >= min_length и соответствие правилам Kaufland.
    """
    norm_locale = str(locale or "de-DE").strip()
    if norm_locale.lower() in ("de", "at", "de-de", "de-at"):
        norm_locale = "de-DE"
    elif norm_locale.lower() in ("fr", "fr-fr"):
        norm_locale = "fr-FR"
    elif norm_locale.lower() in ("cz", "cs", "cs-cz"):
        norm_locale = "cs-CZ"
    elif norm_locale.lower() in ("sk", "sk-sk"):
        norm_locale = "sk-SK"
    elif norm_locale.lower() in ("pl", "pl-pl"):
        norm_locale = "pl-PL"
    elif norm_locale.lower() in ("it", "it-it"):
        norm_locale = "it-IT"
    elif norm_locale.lower() in ("nl", "nl-nl"):
        norm_locale = "nl-NL"

    if norm_locale == "de-DE" and base_description:
        return sanitize_description(base_description, material=material, min_length=min_length)

    clean_title = sanitize_title(title, material=material) if title else "Möbel"
    body_text = LOCALIZED_FURNITURE_DESCRIPTIONS.get(norm_locale, LOCALIZED_FURNITURE_DESCRIPTIONS["de-DE"])
    mat_part = f"<p>Material: {material}</p>" if material else ""
    html = f"<p><strong>{clean_title}</strong></p><p>{body_text}</p>{mat_part}"
    return sanitize_description(html, material=material, min_length=min_length)
