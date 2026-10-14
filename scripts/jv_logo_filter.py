"""
jv_logo_filter.py
Автономный модуль для обнаружения и фильтрации слайдов с логотипом JV Möbel
при выгрузке товаров в аккаунты других брендов (например, XL Möbel).

Использует 3-уровневую систему проверки:
  1. Fast URL string match (без сети по стоп-словам и ID файлов Afterbuy).
  2. Exact MD5 hash & file size match (база известных хэшей логотипа поставщиков).
  3. Visual detector (анализ геометрии и пропорций текста через Pillow + NumPy).
"""

import asyncio
import io
import hashlib
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

import httpx
import numpy as np
from PIL import Image
Image.MAX_IMAGE_PIXELS = None

logger = logging.getLogger("jv_logo_filter")

# =========================================================================
# База известных бинарных отпечатков логотипа JVMÖBEL (Tier 2)
# =========================================================================
KNOWN_JV_LOGO_MD5 = {
    "4ab83837d7f151e5bdc1f310854c97f0",  # original 41627 bytes
    "2ea8fd6021a2ce0db0688a0aa68d97b3",  # 63607 bytes (1600×1066) – Alpino
    "ffa81c811370b53921850ac8512a3f2e",  # 84950 bytes (1600×1066) – ADA, BUROBIG
    "d5da4f64b2b6bb7db041d2c5c491128d",  # 40484 bytes (1280×852) – 448_MERLI
    "5246e51f4144bf9863e0336610691dbb",  # 79019 bytes – Titi/Latte
    "be9fbd51347cc79a646abba4abb31aa4",  # 79034 bytes – Titi/VOYAGER
    "52c9f174d1e627204c2287d56075627f",  # 78953 bytes – Titi/T_R_LYE
    "e680aae52185cb2913a7d74ac8ab554e",  # 78997 bytes – Titi/Latte
    "4a6eb59af028c86dbcfa8dcb9bfdf6f1",  # 79002 bytes – Titi/Latte
    "ef7bc7e3024202eaa9cbbb1176ee73b8",  # 78992 bytes – Titi/BIANCA
    "94df7cb37eadb523cdf8ef1c2009afa7",  # 78992 bytes – Titi/POLICE
    "b16f6564e7676734f4956bee39cbb8d1",  # 132044 bytes – Farbauswahl / ADA LIMA
}
KNOWN_JV_LOGO_SIZES = {41627, 63607, 84950, 40484, 78953, 78992, 78997, 79002, 79019, 79034, 132044}

# Кэш результатов проверки URL в памяти
_LOGO_URL_CACHE: Dict[str, bool] = {}


# =========================================================================
# Tier 1: Быстрая проверка URL без сети
# =========================================================================
def is_forbidden_brand_image_url(account: str, url: str) -> bool:
    """
    Быстрая строковая проверка ссылки на логотип или инфографику.
    Блокирует ссылки с упоминанием логотипов, таблиц цветов (Farbauswahl) или ID слайдов Afterbuy.
    """
    if not url:
        return False
    u_lower = str(url).lower()
    # Стоп-слова в URL
    if any(token in u_lower for token in (
        "jvmoebel", "jv-moebel", "jv_moebel", "jvmebel", "jv-mebel", "jv_mebel",
        "logo_jv", "logo-jv", "logojv",
        "farbauswahl", "farbkarte", "farbmuster", "stoffauswahl", "farbpalette"
    )):
        return True
    # Номера папок/файлов слайда логотипа и инфографик JV в Afterbuy
    if any(p in u_lower for p in ("53416", "32719", "87756", "47672", "94987", "49804", "36304")):
        return True
    return False


# =========================================================================
# Tier 3: Визуальный анализатор геометрии (Computer Vision)
# =========================================================================
def is_jv_logo_by_visual(image_bytes: bytes) -> bool:
    """
    Распознает логотип JVMÖBEL по геометрическим пропорциям:
    узкий вытянутый по горизонтали блок темного текста на белом фоне по центру кадра.
    Срабатывает даже если поставщик сохранил JPEG с новым уровнем сжатия (новый MD5).
    """
    if not image_bytes or len(image_bytes) < 4000:
        return False
    try:
        im = Image.open(io.BytesIO(image_bytes)).convert("L")
        arr = np.array(im)
        
        # Пиксели темнее 225 считаются контентом (текст/логотип)
        mask = arr < 225
        dark_ratio = float(np.mean(mask))

        # Логотип — это редкий текст на белом фоне (от 2% до 18% темных пикселей)
        if not (0.02 <= dark_ratio <= 0.18):
            return False

        rows = np.any(mask, axis=1)
        cols = np.any(mask, axis=0)
        if not np.any(rows) or not np.any(cols):
            return False

        ymin, ymax = int(np.where(rows)[0][0]), int(np.where(rows)[0][-1])
        xmin, xmax = int(np.where(cols)[0][0]), int(np.where(cols)[0][-1])
        w = xmax - xmin
        h = ymax - ymin
        if h == 0 or w == 0:
            return False

        # Соотношение сторон: блок текста JV в 2.7–3.5 раза шире, чем выше (в среднем ~3.1)
        aspect = w / h
        if not (2.7 <= aspect <= 3.5):
            return False

        # Текст центрирован по горизонтали и вертикали
        img_w, img_h = im.size
        center_x_ratio = (xmin + xmax) / 2 / img_w
        center_y_ratio = (ymin + ymax) / 2 / img_h
        if not (0.3 <= center_x_ratio <= 0.7):
            return False
        if not (0.2 <= center_y_ratio <= 0.8):
            return False

        # Высота блока надписи занимает < 65% высоты кадра (мебель обычно занимает > 70%)
        content_height_ratio = h / img_h
        if content_height_ratio > 0.65:
            return False

        return True
    except Exception as e:
        logger.debug(f"Visual logo check failed: {e}")
        return False


def dhash(im: Image.Image, hash_size: int = 8) -> int:
    """Вычисляет разностный перцептивный хэш (difference hash, dHash)."""
    im_gray = im.convert("L").resize((hash_size + 1, hash_size), Image.Resampling.LANCZOS)
    pixels = np.array(im_gray)
    diff = pixels[:, 1:] > pixels[:, :-1]
    return sum([2 ** i for (i, v) in enumerate(diff.flatten()) if v])


CANONICAL_JV_DHASHES = [
    0x0832d0c2a0681500,  # 84950 bytes (ADA, BUROBIG, LNSMN)
    0x0032d0c2a0680100,  # 78992 bytes (Titi)
    0x0830d0c2a0681500,  # 63607 bytes (Vanitti Mobili, Alpino)
    0xe2e66e22cccccd70,  # 132044 bytes (Farbauswahl / Fabric selection slide)
]


def is_jv_logo_by_dhash(image_bytes: bytes, max_distance: int = 6) -> bool:
    """
    Проверяет перцептивный хэш dHash на сходство с известными версиями слайда JV Möbel.
    Устойчив к смене уровня компрессии JPEG, EXIF-метаданных и незначительным артефактам.
    """
    if not image_bytes or len(image_bytes) < 1000:
        return False
    try:
        im = Image.open(io.BytesIO(image_bytes))
        h = dhash(im)
        for canon in CANONICAL_JV_DHASHES:
            dist = bin(h ^ canon).count("1")
            if dist <= max_distance:
                return True
    except Exception as e:
        logger.debug(f"dHash check error: {e}")
    return False


# =========================================================================
# Основная проверка и фильтрация изображений
# =========================================================================
async def is_foreign_brand_image(account: str, url: str, client: Optional[httpx.AsyncClient] = None) -> bool:
    """
    Проверяет, является ли изображение чужим логотипом для указанного аккаунта.
    """
    if not url:
        return False

    acc = str(account).upper()
    cache_key = f"{acc}:{url}"
    if cache_key in _LOGO_URL_CACHE:
        return _LOGO_URL_CACHE[cache_key]

    # Tier 1: Проверка URL
    if is_forbidden_brand_image_url(acc, url):
        _LOGO_URL_CACHE[cache_key] = True
        return True

    close_client = False
    try:
        if client is None:
            client = httpx.AsyncClient(timeout=8.0, headers={"User-Agent": "Mozilla/5.0"})
            close_client = True

        resp = await client.get(url)
        if resp.status_code == 200:
            raw = resp.content
            file_size = len(raw)
            md5 = hashlib.md5(raw).hexdigest()

            # Tier 2: Совпадение по точному MD5
            if md5 in KNOWN_JV_LOGO_MD5:
                logger.info(f"Exact MD5 match for JV logo: {url} ({file_size} bytes)")
                _LOGO_URL_CACHE[cache_key] = True
                return True

            # Tier 2.5: Совпадение по перцептивному хэшу dHash
            if is_jv_logo_by_dhash(raw):
                logger.info(f"Perceptual dHash match for JV logo: {url} ({file_size} bytes)")
                _LOGO_URL_CACHE[cache_key] = True
                return True

            # Tier 2.6: Известный размер файла + геометрический анализ (Tier 3)
            if file_size in KNOWN_JV_LOGO_SIZES and is_jv_logo_by_visual(raw):
                logger.info(f"Known size + Visual match for JV logo: {url} ({file_size} bytes)")
                _LOGO_URL_CACHE[cache_key] = True
                return True

    except Exception as e:
        logger.debug(f"Failed to fetch image {url}: {e}")
    finally:
        if close_client and client:
            try:
                await client.aclose()
            except Exception:
                pass

    _LOGO_URL_CACHE[cache_key] = False
    return False


async def filter_brand_images(account: str, images: List[str]) -> List[str]:
    """
    Асинхронный фильтр списка ссылок:
    Вырезает все слайды с логотипом JV Möbel (актуально и для JV, и для XL на Kaufland).
    """
    if not images:
        return []

    clean = []
    async with httpx.AsyncClient(
        timeout=8.0,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0"}
    ) as client:
        for img in images:
            if not img:
                continue
            try:
                is_logo = await is_foreign_brand_image(account, img, client)
            except Exception:
                is_logo = False

            if not is_logo:
                clean.append(img)
            else:
                logger.info(f"Filtered out JV logo slide: {img}")
    return clean


def filter_brand_images_sync(account: str, images: List[str]) -> List[str]:
    """
    Синхронный быстрый фильтр (только Tier-1 URL и уже закэшированные в памяти ссылки).
    Не делает HTTP-запросов.
    """
    if not images:
        return []

    clean = []
    acc = str(account).upper()
    for img in images:
        if not img:
            continue
        cache_key = f"{acc}:{img}"
        if _LOGO_URL_CACHE.get(cache_key) is True:
            continue
        if is_forbidden_brand_image_url(acc, img):
            _LOGO_URL_CACHE[cache_key] = True
            continue
        clean.append(img)
    return clean


# =========================================================================
# Детекция и фильтрация людей на фото (YOLOv8)
# =========================================================================
_PEOPLE_URL_CACHE: Dict[str, bool] = {}
_YOLO_MODEL = None


def get_yolo_model(model_path: Optional[str] = None):
    """
    Ленивая инициализация и кэширование модели YOLOv8.
    """
    global _YOLO_MODEL
    if _YOLO_MODEL is None:
        try:
            from ultralytics import YOLO

            candidates = [
                model_path,
                "/home/Altera/workdir/kaufland/yolov8n.pt",
                str(Path(__file__).resolve().parent.parent.parent / "yolov8n.pt"),
                "yolov8n.pt",
            ]
            chosen = None
            for c in candidates:
                if c and Path(c).exists():
                    chosen = str(c)
                    break
            if not chosen:
                chosen = "yolov8n.pt"

            _YOLO_MODEL = YOLO(chosen)
            logger.info(f"YOLO model loaded successfully from {chosen}")
        except Exception as e:
            logger.error(f"Failed to load YOLO model: {e}")
            _YOLO_MODEL = None
    return _YOLO_MODEL


def image_has_people(image_bytes: bytes, conf_threshold: float = 0.35) -> bool:
    """
    Детектирует наличие людей (моделей) на изображении с помощью YOLOv8.
    Класс 'person' (COCO class 0).
    Возвращает True, если на фото обнаружены люди с уверенностью >= conf_threshold.
    """
    if not image_bytes or len(image_bytes) < 100:
        return False
    model = get_yolo_model()
    if model is None:
        return False
    try:
        im = Image.open(io.BytesIO(image_bytes))
        results = model.predict(im, verbose=False, conf=conf_threshold)
        for r in results:
            for box in r.boxes:
                cls_id = int(box.cls[0])
                cls_name = model.names.get(cls_id, "")
                if cls_name == "person" or cls_id == 0:
                    return True
        return False
    except Exception as e:
        logger.warning(f"Error checking people in image: {e}")
        return False


async def filter_people_images(images: List[str], client: Optional[httpx.AsyncClient] = None) -> List[str]:
    """
    Асинхронный фильтр изображений на наличие людей.
    Kaufland отклоняет фото товаров с людьми (моделями).
    """
    if not images:
        return []

    clean = []
    close_client = False
    if client is None:
        client = httpx.AsyncClient(
            timeout=10.0,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        close_client = True

    try:
        for img in images:
            if not img or not img.startswith("http"):
                continue
            if img in _PEOPLE_URL_CACHE:
                if not _PEOPLE_URL_CACHE[img]:
                    clean.append(img)
                else:
                    logger.info(f"Filtered out image with people (cached): {img}")
                continue

            try:
                resp = await client.get(img)
                if resp.status_code == 200:
                    has_people = await asyncio.to_thread(image_has_people, resp.content)
                    _PEOPLE_URL_CACHE[img] = has_people
                    if has_people:
                        logger.info(f"Filtered out image with people: {img}")
                    else:
                        clean.append(img)
                else:
                    logger.debug(f"Failed HTTP {resp.status_code} for {img}, keeping")
                    clean.append(img)
            except Exception as e:
                logger.debug(f"Failed to check people for {img}: {e}")
                clean.append(img)
    finally:
        if close_client:
            await client.aclose()

    return clean
