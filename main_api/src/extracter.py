"""
Модуль для извлечения и форматирования HTML-описаний товаров.

Основной функционал:
- Извлечение HTML контента из файлов (по EAN)
- Добавление служебных полей (описание товара, соцсети)
- Подготовка финального веб-тага для отправки в API Kaufland
"""

import os
from html import unescape
from urllib.parse import unquote
from bs4 import BeautifulSoup
from main_api.src.logger import log
from .ssh_client import SSHFileClient
from config import REMOTE_BASE_DIR


def get_description(ean: str) -> str:
    """
    Извлекает оригинальный HTML-контент товара из файла.

    Алгоритм:
    1. Находит блок #tab-content1 в HTML-файле (основное описание товара)
    2. Добавляет !important ко всем CSS стилям (чтобы не переопределялись)
    3. Сохраняет весь <head> для шрифтов и стилей
    4. Возвращает комбинацию head + контент

    Args:
        ean: EAN-код товара; используется для поиска файла media/html/{ean}.html

    Returns:
        Строка с HTML контентом или пустая строка, если файл не найден
    """
    # Вычисляем путь к корню проекта и ищем файл HTML
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    file_path = os.path.join(base_dir, "media", "html", f"{ean}.html")

    if not os.path.exists(file_path):
        log(f"Нет файла: {ean}")
        return ""

    with open(file_path, encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    # Находим основное описание товара (должно быть в #tab-content1)
    tab = soup.find("div", id="tab-content1")
    if not tab:
        log(f"Не найден #tab-content1 в {ean}")
        return ""

    # 1. Сохраняем всю голову (<head>) для стилей и шрифтов
    head_str = str(soup.head) if soup.head else ""

    # 2. Извлекаем содержимое tab-content1
    content = "".join(
        str(child) for child in tab.children if child.name or str(child).strip()
    )

    # 3. Обрабатываем inline-стили: добавляем !important, чтобы они не переопределялись
    content_soup = BeautifulSoup(content, "html.parser")
    for tag in content_soup.find_all(True):
        if tag.has_attr("style"):
            styles = tag["style"]
            new_styles = []
            for rule in styles.split(";"):
                rule = rule.strip()
                if rule and not rule.lower().endswith("!important"):
                    # Пропускаем animation/transition (к ним !important не добавляем)
                    if not any(x in rule.lower() for x in ["animation", "transition"]):
                        rule += " !important"
                if rule:
                    new_styles.append(rule)
            tag["style"] = "; ".join(new_styles)

    content = str(content_soup)

    # 4. Собираем финальный webtag: голова + контент
    final_html = f"""
        {head_str}
        {content}
    """.strip()

    return final_html


def add_extra_fields_webtag(description: str, webtag: str) -> str:
    """
    Добавляет служебные блоки (описание товара, соцсети) к основному HTML-контенту.

    Структура результата:
    1. Оригинальный webtag (содержимое товара)
    2. Блок "Описание товара" с текстом описания (из GPT или вручную)
    3. Блок "Что может быть интересно" с ссылками на шоурумы, контакты, соцсети

    Args:
        description: Текстовое описание товара (из GPT или другого источника)
        webtag: Основной HTML-контент (из get_description)

    Returns:
        Полный HTML с основным контентом, описанием и соцблоками
    """
    # Немецкие подписи для служебных блоков
    db_keywords = {
        "Produktbeschreibung": "Produktbeschreibung",  # Заголовок блока описания
        "Was für Sie interessant sein kann": "Was für Sie interessant sein kann",  # Заголовок соцблока
    }

    new_web_tag = """"""

    # Добавляем оригинальный контент товара
    new_web_tag += webtag + "\n"

    # Добавляем блок "Описание товара"
    product_description = f"""
    <div>
    <div style="background-color: #494949;">
    <h2 class="lastViewHeading bg-color-7 color-1 font-1"><span style="vertical-align: inherit;"><span style="vertical-align: inherit;">
    {db_keywords["Produktbeschreibung"]}</span></span></h2>
    </div>
    <div>&nbsp;</div>
    <div>
    <p><span style="font-size: 12pt; color: #000000;">{description}</span></p>
    </div>
    <div>&nbsp;</div>
    </div>
    """
    new_web_tag += product_description + "\n"

    return new_web_tag


async def adapt_html_description(
    ean: str, description: str, ssh_client: SSHFileClient, controller: str | None = None
) -> str:
    """
    Основная функция для подготовки финального HTML-описания товара.

    Используется в контроллере при создании товара в Kaufland:
    1. Извлекает оригинальный HTML товара из файла
    2. Добавляет служебные блоки (описание, соцсети)
    3. Возвращает готовый HTML для поля 'description' в API

    Args:
        ean: EAN-код товара
        description: Текстовое описание (из GPT генератора)

    Returns:
        Полный HTML контент для отправки в API Kaufland

    Пример использования (в kaufland_controller.py):
        new_webtag = await adapt_html_description(ean, gpt_description)
        attributes["description"] = [new_webtag]
    """
    # Извлекаем оригинальный HTML контент товара
    webtag = get_description_from_remote_server(
        ean=ean, ssh_client=ssh_client, controller=controller
    )
    # Добавляем служебные блоки (описание)
    new_webtag = add_extra_fields_webtag(description=description, webtag=webtag)

    return new_webtag


async def adapt_html_description_v2(html: str, description: str):
    """
    Новая версия для подготовки финального HTML-описвния товара.
    
    Старая версия читала с нашего сервера (которая сейчас не робит).
    Поэтому надо передавать уже спаршенный HTML из афтеркула от Равиля. 
    """
    log("Adapting HTML description (v2)")
    cleaned_html = _normalize_aftercool_html(html)
    webtag = add_extra_fields_webtag(description=description, webtag=cleaned_html)
    return webtag
    


def get_description_from_remote_server(
    ean: str, ssh_client: SSHFileClient, controller: str | None = None
):
    """
    Извлекаем HTML-контент товара с удалённого сервера по SSH.
    """

    remote_path = f"{REMOTE_BASE_DIR[controller]}{ean}.html"
    html_content = ssh_client.read_file(remote_path)

    if not html_content:
        log("Пустой файл")
        return ""

    soup = BeautifulSoup(html_content, "html.parser")

    tab = soup.find("div", id="tab-content1")

    if not tab:
        # Fallbacks for newer templates
        tab = _find_fallback_description(soup)
    if not tab:
        log(f"Не найдено описание (fallback) в {ean}.html")
        return ""
    head_str = str(soup.head) if soup.head else ""

    content = "".join(
        str(child)
        for child in tab.children
        if getattr(child, "name", None) or str(child).strip()
    )
    content_soup = BeautifulSoup(content, "html.parser")
    for tag in content_soup.find_all(True):
        if tag.has_attr("style"):
            styles = tag["style"]
            new_styles = []

            for rule in styles.split(";"):
                rule = rule.strip()
                if not rule:
                    continue

                if "!important" not in rule.lower() and not any(
                    x in rule.lower() for x in ["animation", "transition"]
                ):
                    rule += "!important"

                new_styles.append(rule)

            tag["style"] = "; ".join(new_styles)
    final_html = f"""
          {head_str}
          {str(content_soup)}
    """.strip()
    log(f"Generated description successfully [{ean}]")
    return final_html


def _normalize_aftercool_html(raw_html: str) -> str:
    """
    Нормализует HTML из Aftercool:
    - декодирует URL-encoding (%0d%0a и т.д.),
    - убирает служебные теги/скрипты,
    - оставляет контент из основного блока описания с приоритетом детальных характеристик.
    """
    if not raw_html:
        return ""

    decoded = unquote(str(raw_html))
    decoded = unescape(decoded)
    soup = BeautifulSoup(decoded, "html.parser")

    # Удаляем потенциально проблемные или бесполезные для Kaufland узлы.
    for tag in soup.find_all(["script", "style", "link", "meta", "noscript"]):
        tag.decompose()

    candidate = _find_fallback_description(soup)

    if candidate:
        inner_html = "".join(str(child) for child in candidate.contents).strip()
        return inner_html if inner_html else str(candidate)

    body = soup.body
    if body:
        return "".join(str(child) for child in body.contents).strip()

    return str(soup).strip()


def _find_fallback_description(soup: BeautifulSoup):
    # 1) Tabs & Details panels (самый точный источник характеристик и описания в шаблонах Afterbuy/Aftercool)
    for selector in [
        ".tabs__panel--details .text",
        ".tabs__panel--details",
        ".details-pre",
        "#tab-content1",
        "#tab1",
        ".tab-pane.active",
        ".panel-body",
    ]:
        candidate = soup.select_one(selector)
        if candidate and len(candidate.get_text(strip=True)) > 40:
            return candidate

    # 2) Блок с заголовком Produktbeschreibung, но исключаем рекламный баннер компании
    for header in soup.find_all(["h2", "h3", "h4"]):
        if "produktbeschreibung" in header.get_text(strip=True).lower():
            parent = header.find_parent()
            if parent:
                candidate = parent.find(class_="text") or parent
                txt = candidate.get_text(strip=True).lower()
                # Исключаем общий корпоративный баннер ("Unser Unternehmen bietet...")
                if "unser unternehmen bietet" in txt and len(txt) < 300:
                    continue
                if candidate and len(txt) > 40:
                    return candidate

    # 3) Обычные текстовые карточки, за исключением корпоративного баннера
    for cand in soup.select(".card .text"):
        txt = cand.get_text(strip=True).lower()
        if "unser unternehmen bietet" in txt and len(txt) < 300:
            continue
        if len(txt) > 40:
            return cand

    # 4) Fallback к содержимому body
    return soup.body
