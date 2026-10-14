import asyncio
import json
import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from scripts.reupload_aftercool_catalog import (
    CheckpointManager,
    FailedItemsManager,
    GlobalRateLimiter,
    InFlightTracker,
    Stats,
    get_default_shipping_group,
    get_or_decide_category,
    process_item,
    purge_ean_from_account,
)
from main_api.services.catalog_reconciler import CatalogReconciler


class DummyLogger:
    def info(self, msg, *args, **kwargs): pass
    def warning(self, msg, *args, **kwargs): pass
    def error(self, msg, *args, **kwargs): pass
    def debug(self, msg, *args, **kwargs): pass


class PurgeAndResumeSafetyTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        self.logger = DummyLogger()
        self.rate_limiter = GlobalRateLimiter(rate=100.0)

    def tearDown(self):
        self.temp_dir.cleanup()

    async def test_checkpoint_manager_fast_path_no_unnecessary_rewrite(self):
        """Проверяет, что remove_completed не перезаписывает диск, если EAN не было в чекпоинте."""
        cp_file = self.temp_path / "completed_test.txt"
        err_file = self.temp_path / "errors_test.txt"
        cp_file.write_text("4069943370027\n4069943370034\n", encoding="utf-8")

        mgr = CheckpointManager("test", custom_cp=cp_file, custom_err=err_file)
        await mgr.load_completed()
        self.assertEqual(len(mgr.completed_eans), 2)

        # Удаляем отсутствующий EAN
        with patch.object(CheckpointManager, "_sync_remove") as mock_remove:
            await mgr.remove_completed("4069943379999")
            mock_remove.assert_not_called()
        self.assertEqual(len(mgr.completed_eans), 2)

        # Удаляем присутствующий EAN
        await mgr.remove_completed("4069943370027")
        self.assertNotIn("4069943370027", mgr.completed_eans)
        self.assertEqual(len(mgr.completed_eans), 1)

    async def test_purge_ean_full_success(self):
        """Проверяет успешный purge: удаляет юниты, product-data, и очищает чекпоинт."""
        cp_file = self.temp_path / "completed_purge.txt"
        err_file = self.temp_path / "errors_purge.txt"
        test_ean = "4069943370027"
        cp_file.write_text(f"{test_ean}\n", encoding="utf-8")

        mgr = CheckpointManager("xl", custom_cp=cp_file, custom_err=err_file)
        await mgr.load_completed()

        mock_rac = MagicMock()
        # Mock GET /units -> returns 1 unit on 'de'
        async def mock_send(method, path, json=None):
            if method == "GET" and "/units" in path and "storefront=de" in path:
                return {"data": [{"id_unit": 12345}]}
            elif method == "GET" and "/units" in path:
                return {"data": []}
            elif method == "DELETE" and "/units/12345" in path:
                return {"success": True}
            elif method == "DELETE" and "/product-data/" in path:
                return {"success": True}
            return {}

        mock_rac.send_request = mock_send

        res = await purge_ean_from_account(
            ean=test_ean,
            account="xl",
            rac=mock_rac,
            rate_limiter=self.rate_limiter,
            cp_mgr=mgr,
            logger=self.logger,
            dry_run=False,
            reason="cross_account"
        )

        self.assertTrue(res["success"])
        self.assertEqual(len(res["errors"]), 0)
        self.assertIn("de:12345", res["deleted_units"])
        # Checkpoint должен быть удален при 100% успехе
        self.assertNotIn(test_ean, mgr.completed_eans)

    async def test_purge_ean_partial_failure_preserves_checkpoint(self):
        """Проверяет, что при ошибке удаления юнита чекпоинт НЕ удаляется и возвращается ошибка."""
        cp_file = self.temp_path / "completed_fail.txt"
        err_file = self.temp_path / "errors_fail.txt"
        test_ean = "4069943370027"
        cp_file.write_text(f"{test_ean}\n", encoding="utf-8")

        mgr = CheckpointManager("xl", custom_cp=cp_file, custom_err=err_file)
        await mgr.load_completed()

        mock_rac = MagicMock()
        async def mock_send(method, path, json=None):
            if method == "GET" and "/units" in path and "storefront=de" in path:
                return {"data": [{"id_unit": 99999}]}
            elif method == "DELETE" and "/units/99999" in path:
                # 500 error from API
                raise RuntimeError("500 Internal Server Error: failed to delete unit")
            elif method == "DELETE" and "/product-data/" in path:
                return {"success": True}
            return {"data": []}

        mock_rac.send_request = mock_send

        res = await purge_ean_from_account(
            ean=test_ean,
            account="xl",
            rac=mock_rac,
            rate_limiter=self.rate_limiter,
            cp_mgr=mgr,
            logger=self.logger,
            dry_run=False,
            reason="cross_account"
        )

        self.assertFalse(res["success"])
        self.assertGreater(len(res["errors"]), 0)
        # Чекпоинт ОБЯЗАН остаться в памяти/файле для возможности безопасного ретрая!
        self.assertIn(test_ean, mgr.completed_eans)
        # Ошибка должна быть записана в errors.txt
        self.assertTrue(err_file.exists())
        self.assertIn("PURGE", err_file.read_text(encoding="utf-8"))

    async def test_category_in_flight_coalescing_no_race(self):
        """Проверяет параллельное определение категории: фолловеры корректно ждут лидера без гонок."""
        mock_rac = MagicMock()
        call_count = 0

        async def slow_decide():
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.05)
            return {"data": [{"id_category": 46101, "name": "Couchgarnituren"}]}

        async def fake_retry(fn, **kw):
            return await slow_decide()

        with patch("scripts.reupload_aftercool_catalog.execute_with_retry", side_effect=fake_retry):
            tasks = [
                get_or_decide_category(
                    rac=mock_rac,
                    rate_limiter=self.rate_limiter,
                    title="Design Sofa Luxus",
                    description="Tolles Sofa",
                    manufacturer="Möbel",
                    price=499.0,
                    allow_fallback=True,
                    account="jv",
                    ean="4069943370027",
                    logger=self.logger,
                )
                for _ in range(5)
            ]
            results = await asyncio.gather(*tasks)

        # Все 5 запросов должны получить одинаковый результат
        for cat_id, cat_name in results:
            self.assertEqual(cat_id, 46101)
            self.assertEqual(cat_name, "Couchgarnituren")
        # Но API вызов был выполнен ровно 1 раз благодаря coalescing!
        self.assertEqual(call_count, 1)

    async def test_reconciler_uninitialized_fails_closed(self):
        """Проверяет, что неинициализированный reconciler вызывает fail-closed в process_item."""
        reconciler = CatalogReconciler()
        self.assertFalse(reconciler.is_initialized)

        # Прямой вызов должен вызвать RuntimeError
        with self.assertRaises(RuntimeError):
            reconciler.resolve_target_account("4069943370027")

        # В process_item неинициализированный reconciler должен вернуть FAILED
        mock_rac = MagicMock()
        cp_file = self.temp_path / "completed_uninit.txt"
        err_file = self.temp_path / "errors_uninit.txt"
        mgr = CheckpointManager("jv", custom_cp=cp_file, custom_err=err_file)

        item = {
            "product_id": 1,
            "ean": "4069943370027",
            "source_file": "test.xlsx",
            "name": "Test Item"
        }

        status = await process_item(
            item=item,
            account="jv",
            storefronts=["de"],
            rac=mock_rac,
            rate_limiter=self.rate_limiter,
            cp_mgr=mgr,
            allowed_factories={"test"},
            logger=self.logger,
            dry_run=True,
            allow_cat_fallback=True,
            reconciler=reconciler,
            reconcile_only=True
        )

        self.assertEqual(status, "FAILED")

    async def test_shipping_group_does_not_cache_none(self):
        """Проверяет, что shipping group не кеширует None при пустом ответе API."""
        mock_rac = MagicMock()
        async def mock_send(method, path, json=None):
            return {"data": []}
        mock_rac.send_request = mock_send
        cache = {}

        async def fake_retry(fn, **kw):
            return await fn()

        with patch("scripts.reupload_aftercool_catalog.execute_with_retry", side_effect=fake_retry):
            sg = await get_default_shipping_group(
                rac=mock_rac,
                rate_limiter=self.rate_limiter,
                sf="de",
                cache=cache,
                account="jv",
                logger=self.logger
            )
            self.assertIsNone(sg)
            # В кеше НЕ должно быть записи с None
            self.assertNotIn(("jv", "de"), cache)

    async def test_purge_rerun_after_partial_failure_resolves(self):
        """Проверяет сценарий повторного запуска после частичного сбоя purge."""
        cp_file = self.temp_path / "completed_rerun.txt"
        err_file = self.temp_path / "errors_rerun.txt"
        test_ean = "4069943370027"
        cp_file.write_text(f"{test_ean}\n", encoding="utf-8")

        mgr = CheckpointManager("xl", custom_cp=cp_file, custom_err=err_file)
        await mgr.load_completed()

        mock_rac = MagicMock()
        attempt = 1

        async def mock_send(method, path, json=None):
            nonlocal attempt
            if method == "GET" and "/units" in path and "storefront=de" in path:
                # В первом запуске юнит есть
                return {"data": [{"id_unit": 101}]} if attempt == 1 else {"data": []}
            elif method == "GET" and "/units" in path and "storefront=pl" in path:
                return {"data": [{"id_unit": 202}]}
            elif method == "GET" and "/units" in path:
                return {"data": []}
            elif method == "DELETE" and "/units/101" in path:
                return {"success": True}
            elif method == "DELETE" and "/units/202" in path:
                if attempt == 1:
                    raise RuntimeError("500 Internal Server Error: transient network glitch")
                return {"success": True}
            elif method == "DELETE" and "/product-data/" in path:
                return {"success": True}
            return {}

        mock_rac.send_request = mock_send

        # Запуск 1: сбой на pl:202
        res1 = await purge_ean_from_account(
            ean=test_ean,
            account="xl",
            rac=mock_rac,
            rate_limiter=self.rate_limiter,
            cp_mgr=mgr,
            logger=self.logger,
            dry_run=False,
            reason="cross_account"
        )
        self.assertFalse(res1["success"])
        self.assertIn(test_ean, mgr.completed_eans)

        # Запуск 2: повторный прогон после восстановления сети
        attempt = 2
        res2 = await purge_ean_from_account(
            ean=test_ean,
            account="xl",
            rac=mock_rac,
            rate_limiter=self.rate_limiter,
            cp_mgr=mgr,
            logger=self.logger,
            dry_run=False,
            reason="cross_account"
        )
        self.assertTrue(res2["success"])
        self.assertEqual(len(res2["errors"]), 0)
        # Чекпоинт теперь успешно снят
        self.assertNotIn(test_ean, mgr.completed_eans)

    async def test_purge_ean_status_400_fatal_on_validation_or_general_error(self):
        """Проверяет, что HTTP 400 при DELETE /product-data/ (не связанный с неподдерживаемой локалью) считается ошибкой."""
        cp_file = self.temp_path / "completed_400_err.txt"
        err_file = self.temp_path / "errors_400_err.txt"
        test_ean = "4069943370027"
        cp_file.write_text(f"{test_ean}\n", encoding="utf-8")

        mgr = CheckpointManager("xl", custom_cp=cp_file, custom_err=err_file)
        await mgr.load_completed()

        mock_rac = MagicMock()
        async def mock_send(method, path, json=None):
            if method == "GET" and "/units" in path:
                return {"data": []}
            elif method == "DELETE" and "/product-data/" in path:
                raise RuntimeError("400 Bad Request: Units are still attached to this product or invalid state")
            return {}

        mock_rac.send_request = mock_send

        res = await purge_ean_from_account(
            ean=test_ean,
            account="xl",
            rac=mock_rac,
            rate_limiter=self.rate_limiter,
            cp_mgr=mgr,
            logger=self.logger,
            dry_run=False,
            reason="cross_account"
        )

        self.assertFalse(res["success"])
        self.assertGreater(len(res["errors"]), 0)
        # Checkpoint НЕ должен удаляться при ошибке 400
        self.assertIn(test_ean, mgr.completed_eans)

    async def test_purge_ean_status_400_non_fatal_on_unsupported_locale(self):
        """Проверяет, что HTTP 400 при DELETE /product-data/ игнорируется, если локаль/витрина не сконфигурирована."""
        cp_file = self.temp_path / "completed_400_ok.txt"
        err_file = self.temp_path / "errors_400_ok.txt"
        test_ean = "4069943370027"
        cp_file.write_text(f"{test_ean}\n", encoding="utf-8")

        mgr = CheckpointManager("xl", custom_cp=cp_file, custom_err=err_file)
        await mgr.load_completed()

        mock_rac = MagicMock()
        async def mock_send(method, path, json=None):
            if method == "GET" and "/units" in path:
                return {"data": []}
            elif method == "DELETE" and "/product-data/" in path:
                raise RuntimeError("400 Bad Request: Locale 'sk-SK' is not configured or not active for this seller storefront")
            return {}

        mock_rac.send_request = mock_send

        res = await purge_ean_from_account(
            ean=test_ean,
            account="xl",
            rac=mock_rac,
            rate_limiter=self.rate_limiter,
            cp_mgr=mgr,
            logger=self.logger,
            dry_run=False,
            reason="cross_account"
        )

        self.assertTrue(res["success"])
        self.assertEqual(len(res["errors"]), 0)
        # Checkpoint успешно удален
        self.assertNotIn(test_ean, mgr.completed_eans)

    async def test_failed_items_manager_persistence_and_resolution(self):
        """Проверяет жизненный цикл FailedItemsManager: запись, сохранение на диск, загрузку и разрешение."""
        dlq_file = self.temp_path / ".failed_items_test_acc.json"
        with patch("scripts.reupload_aftercool_catalog.BACKEND_DIR", self.temp_path):
            mgr = FailedItemsManager("test_acc")
            self.assertEqual(mgr.pending_count(), 0)

            item = {"product_id": 999, "ean": "4069943370027", "name": "Failed Item"}
            await mgr.record_failed("4069943370027", item, status="FAILED", error_msg="Timeout 504")

            self.assertEqual(mgr.pending_count(), 1)
            self.assertTrue(dlq_file.exists())

            # Загружаем новый экземпляр из файла
            mgr2 = FailedItemsManager("test_acc")
            loaded = await mgr2.load()
            self.assertIn("4069943370027", loaded)
            self.assertEqual(mgr2.pending_count(), 1)

            pending_items = mgr2.get_pending_items()
            self.assertEqual(len(pending_items), 1)
            self.assertEqual(pending_items[0]["product_id"], 999)

            # Разрешаем товар (успех)
            await mgr2.record_resolved("4069943370027")
            self.assertEqual(mgr2.pending_count(), 0)
            # Файл должен быть удален с диска при пустой очереди
            self.assertFalse(dlq_file.exists())


if __name__ == "__main__":
    unittest.main()
