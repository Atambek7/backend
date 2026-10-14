from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")
load_dotenv()

# Подключение к Базе Данных
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Ключи для Kaufland
JV_CLIENT_KEY = os.getenv("JV_CLIENT_KEY")
JV_SECRET_KEY = os.getenv("JV_SECRET_KEY")
XL_CLIENT_KEY = os.getenv("XL_CLIENT_KEY")
XL_SECRET_KEY = os.getenv("XL_SECRET_KEY")

controllers_base = {
    "jv": {
        "client_key": JV_CLIENT_KEY,
        "secret_key": JV_SECRET_KEY,
    },
    "xl": {
        "client_key": XL_CLIENT_KEY,
        "secret_key": XL_SECRET_KEY,
    },
}

storefronts_by_account = {
    "jv": ["de", "cz", "sk", "pl", "at", "fr", "it", "nl", "es"],
    "xl": ["de", "cz", "sk", "pl", "at", "fr", "it"],
}

# Общий список всех витрин (для валидации и обратной совместимости)
storefronts = ["de", "cz", "sk", "pl", "at", "fr", "it", "nl", "es"]


def get_account_storefronts(account: str | None = None) -> list[str]:
    """
    Возвращает список поддерживаемых витрин для аккаунта.
    JV: de, cz, sk, pl, at, fr, it, nl, es.
    XL: de, cz, sk, pl, at, fr, it (без nl и es).
    Если аккаунт не указан или неизвестен, возвращает общий список storefronts.
    """
    if not account:
        return list(storefronts)
    acc = str(account).strip().lower()
    return list(storefronts_by_account.get(acc, storefronts))

# Базовые URL и учетные данные API
KAUFLAND_API_BASE_URL = os.getenv("KAUFLAND_API_BASE_URL", "https://sellerapi.kaufland.com/v2")
AFTERCOOL_BASE_URL = os.getenv("AFTERCOOL_BASE_URL", "http://217.154.165.51:8000/api")
AFTERCOOL_USERNAME = os.getenv("AFTERCOOL_USERNAME", "Miras")
AFTERCOOL_PASSWORD = os.getenv("AFTERCOOL_PASSWORD", "Miras112112")

# Курсы валют и надбавки для витрин
EXCHANGE_RATES = {
    "PLN": float(os.getenv("EXCHANGE_RATE_PLN", "4.7")),
    "CZK": float(os.getenv("EXCHANGE_RATE_CZK", "25.4")),
    "ITP": float(os.getenv("SURCHARGE_IT", "1.05")),
}

# URL Путь для моей API
MAIN_HOST = os.getenv("MAIN_HOST")

# GPT ключ для API
GPT_API_KEY = os.getenv("GPT_API_KEY")

# FTP creds для kaufland картинок
FTP_HOST: str | None = os.getenv("FTP_HOST")
FTP_USERNAME: str | None = os.getenv("FTP_USERNAME")
FTP_PASSWORD: str | None = os.getenv("FTP_PASSWORD")

XL_CONTACT_DATA = {
    "address": "Am Flugplatz 26, 88483 Burgrieden",
    "email_address": "info@xlmoebel.de",
    "name": "XL MOEBEL GmbH",
    "phone_number": "07392 - 93 78 44 5",
    "url": "https://www.xlmoebel.de/xlmoebel-kontakt-zu-unserem-luxusmoebel-store",
}

XL_MANUFACTURER = "XL MOEBEL GmbH"

JV_CONTACT_DATA = {
    "address": "Am Flugplatz 28, 88483 Burgrieden",
    "email_address": "info@jvmoebel.de",
    "name": "AEA GmbH & Co. KG",
    "phone_number": "07392 - 93 78 44 0",
    "url": "https://www.jvmoebel.de/Infos/Kontakt.htm",
}

JV_MANUFACTURER = "AEA GmbH & Co. KG"

SSH_HOST = os.getenv("SSH_HOST")
SSH_USER = os.getenv("SSH_USER")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH")

REMOTE_BASE_DIR = {
    "jv": "/var/lib/productbaseapi/data/JV/JV_PRODUCT/JV_NEW/HTML/",
    "xl": "/var/lib/productbaseapi/data/XL/XL_PRODUCT/XL_NEW/HTML/",
}

CELERY_REDIS_BROKER_DB = int(os.getenv("CELERY_REDIS_DB", default="1"))
CELERY_REDIS_RESULT_DB = CELERY_REDIS_BROKER_DB + 1
