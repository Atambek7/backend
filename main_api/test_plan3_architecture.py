import asyncio
import tempfile
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from django.test import TestCase

import config
from main_api.src.clients.kaufland_api_client import KauflandApiClient
from main_api.src.controller.kaufland_controller import KauflandController
from main_api.src.controller.rest_api_controller import RestApiController
from main_api.src.utils.cache_utils import BoundedLRUCache
from main_api.src.utils.checkpoint_utils import MigrationCheckpoint


class Plan3ArchitectureTestCase(TestCase):
    """
    Тесты для Плана 3: Архитектурная устойчивость, LRU-кэши, единый API-клиент и миграционные чекпоинты.
    """

    # -------------------------------------------------------------------------
    # 3.1: BoundedLRUCache с ограничением памяти и TTL
    # -------------------------------------------------------------------------
    def test_lru_cache_respects_maxsize(self):
        """При превышении maxsize самые старые неиспользуемые элементы вытесняются."""
        cache = BoundedLRUCache(maxsize=3, ttl_sec=None)
        cache["a"] = 1
        cache["b"] = 2
        cache["c"] = 3
        self.assertEqual(len(cache), 3)

        # Обращаемся к 'a', делая её недавно использованной
        _ = cache["a"]

        # Добавляем 4-й элемент — должна вытесниться 'b'
        cache["d"] = 4
        self.assertEqual(len(cache), 3)
        self.assertIn("a", cache)
        self.assertNotIn("b", cache)
        self.assertIn("c", cache)
        self.assertIn("d", cache)

    def test_lru_cache_respects_ttl_expiration(self):
        """Элементы с истекшим TTL автоматически удаляются и не возвращаются."""
        cache = BoundedLRUCache(maxsize=10, ttl_sec=0.05)
        cache["temp_key"] = "temp_value"
        self.assertIn("temp_key", cache)
        self.assertEqual(cache.get("temp_key"), "temp_value")

        # Ждем истечения TTL
        time.sleep(0.06)
        self.assertNotIn("temp_key", cache)
        self.assertIsNone(cache.get("temp_key"))

    def test_kaufland_controller_uses_bounded_lru_cache(self):
        """KauflandController инициализирует кэши EAN как BoundedLRUCache вместо бесконечных dict."""
        controller = KauflandController(MagicMock(), "jv")
        self.assertIsInstance(controller._ean_cache["unit_ids"], BoundedLRUCache)
        self.assertIsInstance(controller._ean_cache["product_ids"], BoundedLRUCache)
        self.assertIsInstance(controller._ean_cache["main_data"], BoundedLRUCache)

    # -------------------------------------------------------------------------
    # 3.2: Канонический KauflandApiClient и HMAC
    # -------------------------------------------------------------------------
    def test_kaufland_api_client_hmac_signature(self):
        """Проверка генерации канонической подписи HMAC-SHA256."""
        secret = "test_secret_key"
        sig1 = KauflandApiClient.sign_request(
            method="GET",
            uri="https://sellerapi.kaufland.com/v2/products",
            body="",
            timestamp="1700000000",
            secret_key=secret,
        )
        self.assertTrue(isinstance(sig1, str))
        self.assertEqual(len(sig1), 64)  # SHA-256 hex digest length

        # Одинаковые параметры должны давать идентичную подпись
        sig2 = KauflandApiClient.sign_request(
            method="GET",
            uri="https://sellerapi.kaufland.com/v2/products",
            body="",
            timestamp="1700000000",
            secret_key=secret,
        )
        self.assertEqual(sig1, sig2)

    def test_kaufland_api_client_compact_json_serialization(self):
        """Сериализация JSON для подписи не должна содержать лишних пробелов."""
        body = {"title": "Sofa", "price": 4999}
        serialized = KauflandApiClient.serialize_body(body)
        self.assertEqual(serialized, '{"title":"Sofa","price":4999}')

    def test_kaufland_api_client_validates_known_accounts(self):
        """Клиент принимает только валидные аккаунты 'jv' или 'xl'."""
        session_mock = MagicMock()
        client_jv = KauflandApiClient(account="jv", session=session_mock)
        self.assertEqual(client_jv.account, "jv")

        with self.assertRaises(ValueError):
            KauflandApiClient(account="invalid_account", session=session_mock)

    # -------------------------------------------------------------------------
    # 3.3: Централизованные курсы валют в config.py
    # -------------------------------------------------------------------------
    def test_config_exchange_rates_integration(self):
        """Расчёт цены на витринах использует централизованные курсы из config.EXCHANGE_RATES."""
        session_mock = MagicMock()
        rac = RestApiController("jv", session_mock)

        # 100 EUR:
        # CZ: 100 * 25.4 = 2540 CZK -> 254000
        # PL: 100 * 4.7 = 470 PLN -> 47000
        # IT: 100 * 1.05 = 105 EUR -> 10500
        self.assertEqual(rac.make_price("cz", 100.0), 254000)
        self.assertEqual(rac.make_price("pl", 100.0), 47000)
        self.assertEqual(rac.make_price("it", 100.0), 10500)
        self.assertEqual(rac.make_price("de", 100.0), 10000)

    # -------------------------------------------------------------------------
    # 3.4: Структурированный MigrationCheckpoint
    # -------------------------------------------------------------------------
    def test_migration_checkpoint_workflow(self):
        """Проверка работы менеджера чекпоинтов с форматом JSONL."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            cp_path = Path(tmp_dir) / "test_migration_checkpoint.jsonl"
            cp = MigrationCheckpoint(checkpoint_path=cp_path, account="jv")

            required_sfs = ["de", "cz", "pl"]
            ean = "4069943370027"

            # Сначала не завершено
            self.assertFalse(cp.is_ean_fully_completed(ean, required_sfs))

            # Записываем витрину 'de' как SUCCESS
            cp.record(ean=ean, storefront="de", status="SUCCESS", category_id=123)
            self.assertTrue(cp.is_storefront_completed(ean, "de"))
            self.assertFalse(cp.is_storefront_completed(ean, "cz"))
            self.assertFalse(cp.is_ean_fully_completed(ean, required_sfs))

            # Записываем 'cz' как FAILED, а 'pl' как SUCCESS
            cp.record(ean=ean, storefront="cz", status="FAILED", error="Category not found")
            cp.record(ean=ean, storefront="pl", status="SUCCESS", category_id=123)

            # EAN всё ещё не завершён (из-за cz)
            self.assertFalse(cp.is_ean_fully_completed(ean, required_sfs))

            # Проверяем сводку
            summary = cp.get_summary()
            self.assertEqual(summary["SUCCESS"], 2)
            self.assertEqual(summary["FAILED"], 1)

            # Проверяем сохранение и перезагрузку из файла
            cp_reloaded = MigrationCheckpoint(checkpoint_path=cp_path, account="jv")
            self.assertTrue(cp_reloaded.is_storefront_completed(ean, "de"))
            self.assertTrue(cp_reloaded.is_storefront_completed(ean, "pl"))
            self.assertFalse(cp_reloaded.is_storefront_completed(ean, "cz"))

            # Исправляем 'cz' на SUCCESS
            cp_reloaded.record(ean=ean, storefront="cz", status="SUCCESS", category_id=123)
            self.assertTrue(cp_reloaded.is_ean_fully_completed(ean, required_sfs))
