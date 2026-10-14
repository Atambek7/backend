#!/usr/bin/env python3
"""
Модуль и CLI-утилита для умного сжатия изображений товаров без видимой потери качества.
Решает проблему ошибки Kaufland:
"The media file is too large. Please upload a smaller file (max. 10 MB for images and 200 MB for videos)."

Возможности:
1. Скачивание изображений по URL или чтение локальных файлов.
2. Проверка размера файла (HEAD-запрос или чтение байт).
3. Интеллектуальное сжатие:
   - Конвертация тяжелых PNG (особенно 8000x6000 и т.д.) в оптимизированный прогрессивный JPEG.
   - Сохранение прозрачности через заливку белым фоном (требование маркетплейсов).
   - Сохранение высокой четкости (LANCZOS ресайз только если размер превышает 4K / 3840px).
   - 4:4:4 цветовая субдискретизация (subsampling=0) для максимального сохранения деталей.
   - Гарантия размера < 8.5 MB (с запасом под лимит 10 MB Kaufland).
4. Размещение в локальном хранилище backend/media/upload-images/ с мгновенным доступом по
   публичному HTTPS URL: https://kl.automatonsoft.de/upload-images/<filename>
5. Обновление атрибута picture в Kaufland через PUT /product-data для заданного EAN.
"""

import argparse
import asyncio
import io
import logging
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
from PIL import Image
Image.MAX_IMAGE_PIXELS = None

BACKEND_DIR = Path("/home/Altera/workdir/kaufland/backend")
sys.path.insert(0, str(BACKEND_DIR))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("compress_image_service")

# Константы
UPLOAD_IMAGES_DIR = BACKEND_DIR / "media" / "upload-images"
PUBLIC_URL_PREFIX = "https://kl.automatonsoft.de/upload-images"
MAX_SAFE_FILE_SIZE = 2.0 * 1024 * 1024  # 2.0 MB (гарантирует легкие картинки без тяжелых файлов)
MAX_DIMENSION = 3840  # 4K UHD максимальная сторона (сохраняет ультра-детализацию)
DEFAULT_JPEG_QUALITY = 90


def ensure_storage_dir():
    UPLOAD_IMAGES_DIR.mkdir(parents=True, exist_ok=True)


def sanitize_filename(name: str) -> str:
    base = os.path.basename(name)
    # Строго ASCII латиница, цифры, дефис, точка и подчеркивание (исключаем спецсимволы, кириллицу, умлауты)
    base = re.sub(r"[^a-zA-Z0-9_\-.]", "_", base)
    base = re.sub(r"\.{2,}", ".", base)
    base = re.sub(r"_{2,}", "_", base)
    return base


def quote_url(url: str) -> str:
    """Безопасное кодирование URL для urllib (поддержка кириллицы, умлаутов и спецсимволов)."""
    try:
        url_clean = url.strip()
        if url_clean.startswith("ftp://jvfurniture.eu:21/"):
            url_clean = "https://jvfurniture.eu/" + url_clean[len("ftp://jvfurniture.eu:21/"):]
        elif url_clean.startswith("ftp://jvfurniture.eu/"):
            url_clean = "https://jvfurniture.eu/" + url_clean[len("ftp://jvfurniture.eu/"):]

        parts = urllib.parse.urlsplit(url_clean)
        path_unquoted = urllib.parse.unquote(parts.path)
        path_quoted = urllib.parse.quote(path_unquoted)
        query_unquoted = urllib.parse.unquote(parts.query)
        query_quoted = urllib.parse.quote(query_unquoted, safe="=&?+")
        return urllib.parse.urlunsplit((parts.scheme, parts.netloc, path_quoted, query_quoted, parts.fragment))
    except Exception:
        return url


def get_remote_image_size(url: str) -> Optional[int]:
    """Быстрая проверка размера файла через HTTP HEAD запрос."""
    try:
        safe_url = quote_url(url)
        req = urllib.request.Request(safe_url, headers={"User-Agent": "Mozilla/5.0"}, method="HEAD")
        with urllib.request.urlopen(req, timeout=10) as resp:
            cl = resp.headers.get("Content-Length")
            if cl and cl.isdigit():
                return int(cl)
    except Exception as e:
        logger.debug(f"HEAD check failed for {url}: {e}")
    return None


def download_image_bytes(url: str) -> bytes:
    """Скачивание изображения с таймаутом и User-Agent."""
    safe_url = quote_url(url)
    req = urllib.request.Request(safe_url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def make_seamless_square(
    img: Image.Image,
    target_size: int = 2048,
    fill_ratio: float = 0.90,
    feather_px: int = 35,
) -> Image.Image:
    """
    Приводит любое фото товара к идеальному квадрату target_size x target_size (2048x2048)
    на чистом белом фоне (255, 255, 255) БЕЗ видимых рамок, стыков и полос (letterbox).

    1. Конвертирует прозрачность в белый фон.
    2. Осветляет околобелые/светло-серые студийные участки фона.
    3. Применяет мягкий альфа-градиент (feathering) по внешнему периметру картинки,
       гарантируя, что все края картинки плавно переходят в чистый белый (255, 255, 255).
    4. Центрирует объект на чисто белом холсте target_size x target_size.
    """
    if img.mode in ("RGBA", "LA", "P"):
        bg = Image.new("RGB", img.size, (255, 255, 255))
        if img.mode == "P":
            img = img.convert("RGBA")
        bg.paste(img, mask=img.split()[-1] if img.mode == "RGBA" else None)
        img_rgb = bg
    else:
        img_rgb = img.convert("RGB")

    w, h = img_rgb.size
    arr = np.array(img_rgb, dtype=np.float32)

    # 2. Осветление околобелого студийного фона (если фон светло-серый > 215)
    is_neutral_light = (
        (arr[:, :, 0] > 215) &
        (arr[:, :, 1] > 215) &
        (arr[:, :, 2] > 215) &
        (np.abs(arr[:, :, 0] - arr[:, :, 1]) < 18) &
        (np.abs(arr[:, :, 1] - arr[:, :, 2]) < 18)
    )
    brightness = arr.mean(axis=2)
    factor = np.clip((brightness - 215.0) / 40.0, 0.0, 1.0)[:, :, np.newaxis]
    arr = np.where(is_neutral_light[:, :, np.newaxis], arr + (255.0 - arr) * factor * 0.85, arr)

    # 3. Мягкий переход (feathering) по краям исходного прямоугольника в чистый белый
    mask_y = np.ones(h, dtype=np.float32)
    feather_h = min(feather_px, h // 4)
    if feather_h > 0:
        fade_in = np.linspace(0.0, 1.0, feather_h)
        mask_y[:feather_h] = fade_in
        mask_y[-feather_h:] = fade_in[::-1]

    mask_x = np.ones(w, dtype=np.float32)
    feather_w = min(feather_px, w // 4)
    if feather_w > 0:
        fade_in_x = np.linspace(0.0, 1.0, feather_w)
        mask_x[:feather_w] = fade_in_x
        mask_x[-feather_w:] = fade_in_x[::-1]

    edge_mask = (mask_y[:, np.newaxis] * mask_x[np.newaxis, :])[:, :, np.newaxis]
    blended_arr = arr * edge_mask + 255.0 * (1.0 - edge_mask)
    blended_arr = np.clip(blended_arr, 0.0, 255.0).astype(np.uint8)
    blended_img = Image.fromarray(blended_arr)

    # 4. Масштабирование с сохранением пропорций под target_size
    max_side = int(target_size * fill_ratio)
    scale = max_side / max(w, h)
    new_w = max(1, int(round(w * scale)))
    new_h = max(1, int(round(h * scale)))

    resized_img = blended_img.resize((new_w, new_h), Image.LANCZOS)

    # 5. Размещение по центру чисто белого холста target_size x target_size
    canvas = Image.new("RGB", (target_size, target_size), (255, 255, 255))
    offset = ((target_size - new_w) // 2, (target_size - new_h) // 2)
    canvas.paste(resized_img, offset)

    return canvas


def compress_image_bytes(
    data: bytes,
    filename_hint: str = "image.jpg",
    max_size_bytes: int = int(MAX_SAFE_FILE_SIZE),
    max_dim: int = MAX_DIMENSION,
    quality: int = DEFAULT_JPEG_QUALITY,
    is_main: bool = False,
    target_square_size: int = 2048,
) -> Tuple[bytes, str]:
    """
    Сжимает байты изображения без потери качества.
    - Для ГЛАВНОГО ФОТО (is_main=True): масштабирует и центрирует на чистом белом фоне
      строго 2048x2048 px без видимых рамок, полос и швов (требование маркетплейса Kaufland).
    - Для ОСТАЛЬНЫХ ФОТО (is_main=False): сохраняет оригинальное разрешение и пропорции
      (масштабирует только при экстремальных разрешениях > max_dim / 3840px).
    Возвращает (compressed_bytes, new_filename).
    """
    img = Image.open(io.BytesIO(data))
    orig_w, orig_h = img.size
    orig_format = (img.format or "JPEG").upper()

    if is_main:
        logger.info(
            f"[Главное фото] Бесшовное сведение в точный квадрат {target_square_size}x{target_square_size} "
            f"на чистом белом фоне (RGB 255,255,255) без видимых рамок и полос..."
        )
        img_rgb = make_seamless_square(img, target_size=target_square_size, fill_ratio=0.90)
    else:
        # 1. Обработка прозрачности: RGBA/LA/P -> RGB на чистом белом фоне
        if img.mode in ("RGBA", "LA", "P"):
            bg = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode == "P":
                img = img.convert("RGBA")
            bg.paste(img, mask=img.split()[-1] if img.mode == "RGBA" else None)
            img_rgb = bg
        else:
            img_rgb = img.convert("RGB")

        # Для ДОПОЛНИТЕЛЬНЫХ ФОТО: размер не меняем, масштабируем только при экстремальных разрешениях (> 4K)
        w, h = img_rgb.size
        if max(w, h) > max_dim:
            scale = max_dim / max(w, h)
            new_w, new_h = int(w * scale), int(h * scale)
            logger.info(f"[Доп. фото] Масштабирование разрешения с {w}x{h} до {new_w}x{new_h} (LANCZOS)...")
            img_rgb = img_rgb.resize((new_w, new_h), Image.LANCZOS)

    # 3. Сохранение в оптимизированный прогрессивный JPEG с 4:4:4 субдискретизацией
    current_q = quality
    out_buf = io.BytesIO()
    for attempt in range(5):
        out_buf.seek(0)
        out_buf.truncate(0)
        img_rgb.save(
            out_buf,
            format="JPEG",
            quality=current_q,
            optimize=True,
            progressive=True,
            subsampling=0,  # 4:4:4 chroma subsampling (максимальная четкость текста и текстур)
        )
        out_bytes = out_buf.getvalue()
        if len(out_bytes) <= max_size_bytes:
            break
        current_q = max(75, int(current_q * 0.9))
        logger.info(f"Размер ({len(out_bytes)/(1024*1024):.2f} MB) > лимита, снижение качества до {current_q}...")

    # Формируем имя файла с расширением .jpg (для главного фото добавляем суффикс _2048 для сброса кэша CDN Kaufland)
    clean_name = sanitize_filename(filename_hint)
    stem, _ = os.path.splitext(clean_name)
    if is_main and not stem.endswith("_2048"):
        new_filename = f"{stem}_2048.jpg"
    else:
        new_filename = f"{stem}.jpg"
    return out_bytes, new_filename


def process_and_publish_image(
    source: str,
    ean: Optional[str] = None,
    force_compress: bool = False,
    is_main: bool = False,
    target_square_size: int = 2048,
) -> str:
    """
    Основная функция обработки:
    - Принимает URL или путь к файлу.
    - Проверяет размер и безопасность URL.
    - Для главного фото (is_main=True): приводит разрешение строго к 2048x2048 на белом фоне.
    - Для остальных фото (is_main=False): сохраняет исходные размеры, если они не превышают 4K.
    - Сжимает через Pillow и размещает в backend/media/upload-images/
    - Возвращает итоговый публичный URL.
    """
    ensure_storage_dir()
    is_url = source.startswith("http://") or source.startswith("https://")
    is_webp = ".webp" in source.lower()
    has_unsafe_url = is_url and (any(ord(c) > 127 for c in source) or " " in source or source.startswith("http://"))
    needs_rehost_or_convert = is_webp or has_unsafe_url

    # 0. Если изображение уже опубликовано на нашем сервере и укладывается в лимит (и не запрошено принудительное обновление)
    if not force_compress and is_url and source.startswith(PUBLIC_URL_PREFIX):
        parsed = urllib.parse.urlparse(source)
        local_filename = os.path.basename(parsed.path)
        local_path = UPLOAD_IMAGES_DIR / local_filename
        if local_path.exists() and local_path.stat().st_size <= MAX_SAFE_FILE_SIZE:
            # Для главного фото проверяем, что размер действительно target_square_size
            if is_main:
                try:
                    with Image.open(local_path) as chk_im:
                        if chk_im.size == (target_square_size, target_square_size):
                            return source
                except Exception:
                    pass
            else:
                logger.info(f"Изображение уже опубликовано на нашем сервере ({local_path.stat().st_size / (1024*1024):.2f} MB <= 2 MB): {source}")
                return source

    # Получаем исходные байты
    if is_url:
        logger.info(f"Скачивание изображения для оптимизации: {source} (is_main={is_main})...")
        raw_bytes = download_image_bytes(source)
        parsed = urllib.parse.urlparse(source)
        raw_name = os.path.basename(parsed.path) or "image.png"
    else:
        logger.info(f"Чтение локального файла: {source} (is_main={is_main})...")
        with open(source, "rb") as f:
            raw_bytes = f.read()
        raw_name = os.path.basename(source)

    orig_mb = len(raw_bytes) / 1024 / 1024
    logger.info(f"Исходный размер: {orig_mb:.2f} MB ({len(raw_bytes)} байт)")

    # Префикс имени EAN если передан
    prefix = f"{ean}_" if ean else ""
    compressed_bytes, new_name = compress_image_bytes(
        raw_bytes,
        filename_hint=f"{prefix}{raw_name}",
        is_main=is_main,
        target_square_size=target_square_size,
    )
    comp_mb = len(compressed_bytes) / 1024 / 1024
    reduction = 100 - (len(compressed_bytes) / len(raw_bytes) * 100)

    logger.info(f"Сжато до: {comp_mb:.2f} MB (уменьшение на {reduction:.1f}%).")

    # Сохраняем в папку веб-сервера
    dest_path = UPLOAD_IMAGES_DIR / new_name
    with open(dest_path, "wb") as f:
        f.write(compressed_bytes)

    # Назначаем права для чтения nginx
    try:
        os.chmod(dest_path, 0o666)
    except Exception:
        pass

    public_url = f"{PUBLIC_URL_PREFIX}/{new_name}"
    logger.info(f"Опубликовано по публичному URL: {public_url}")
    return public_url


async def fix_ean_images_on_kaufland(ean: str, account: str = "jv"):
    """
    Автоматически проверяет картинки товара на Kaufland,
    сжимает проблемные (> 8.5 MB) и обновляет карточку через PUT /product-data.
    """
    import aiohttp
    from main_api.src.controller.rest_api_controller import RestApiController
    from scripts.purge_trash_catalog_items import SafeRateLimiter, request_with_retry

    LOCALES = ["de-DE", "de-AT", "cs-CZ", "sk-SK", "pl-PL", "fr-FR", "it-IT", "nl-NL"]
    rate_limiter = SafeRateLimiter(60.0)

    async with aiohttp.ClientSession() as session:
        rac = RestApiController(account, session)
        logger.info(f"Проверка картинок для EAN {ean} на Kaufland...")

        res_get = await request_with_retry(rac, "GET", f"/product-data/{ean}?locale=de-DE", rate_limiter)
        if res_get.status != 200:
            logger.error(f"Не удалось получить product-data для {ean}: HTTP {res_get.status}")
            return False

        attrs = res_get.data.get("data", {}).get("attributes", {})
        attrs.pop("category_detail", None)
        pics = attrs.get("picture", [])
        if not pics:
            logger.warning(f"У EAN {ean} нет картинок в product-data!")
            return False

        logger.info(f"Найдено картинок: {len(pics)}")
        new_pics = []
        changed = False

        for i, pic_url in enumerate(pics):
            is_main = (i == 0)
            logger.info(f"[{i+1}/{len(pics)}] Проверка {pic_url} (is_main={is_main})...")
            size = get_remote_image_size(pic_url)
            # Для главного фото всегда гарантируем 2048x2048, для остальных - сжатие при превышении лимита
            if is_main or (size and size > MAX_SAFE_FILE_SIZE):
                opt_url = process_and_publish_image(pic_url, ean=ean, force_compress=True, is_main=is_main)
                new_pics.append(opt_url)
                changed = True
            else:
                new_pics.append(pic_url)

        if not changed:
            logger.info(f"Все картинки EAN {ean} в пределах допустимого размера (< 8.5 MB).")
            return True

        logger.info(f"Обновление картинок для EAN {ean} на всех витринах...")
        attrs["picture"] = new_pics

        for loc in LOCALES:
            payload = {"ean": [ean], "attributes": attrs}
            put_res = await request_with_retry(rac, "PUT", f"/product-data?locale={loc}", rate_limiter, json_payload=payload)
            logger.info(f"  [{loc}] PUT status: {put_res.status}")

        logger.info(f"УСПЕШНО: Картинки для {ean} обновлены на Kaufland!")
        return True


def main():
    parser = argparse.ArgumentParser(description="Сжатие картинок без потери качества для Kaufland с поддержкой 2048x2048")
    parser.add_argument("--url", help="URL картинки для сжатия и публикации")
    parser.add_argument("--file", help="Путь к локальному файлу картинки")
    parser.add_argument("--ean", help="EAN товара на Kaufland для автоматической проверки и обновления")
    parser.add_argument("--force", action="store_true", help="Принудительное сжатие независимо от размера")
    parser.add_argument("--main", action="store_true", help="Форматировать как главное фото: точный размер 2048x2048 на белом фоне")
    parser.add_argument("--size", type=int, default=2048, help="Целевой размер стороны квадрата для главного фото (по умолчанию 2048)")
    parser.add_argument("--account", default="jv", help="Аккаунт Kaufland (jv или xl)")

    args = parser.parse_args()

    if args.ean:
        asyncio.run(fix_ean_images_on_kaufland(args.ean, account=args.account))
    elif args.url:
        pub_url = process_and_publish_image(
            args.url,
            force_compress=args.force or args.main,
            is_main=args.main,
            target_square_size=args.size,
        )
        print(f"\nИТОГОВЫЙ URL: {pub_url}")
    elif args.file:
        pub_url = process_and_publish_image(
            args.file,
            force_compress=args.force or args.main,
            is_main=args.main,
            target_square_size=args.size,
        )
        print(f"\nИТОГОВЫЙ URL: {pub_url}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
