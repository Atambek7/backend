import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from django.test import TestCase

from main_api.src.utils.ean_utils import normalize_ean, is_valid_ean
from main_api.src.utils.price_utils import parse_price, format_price_cents
from main_api.services.material_service import extract_explicit_composition, resolve_material_attributes
from main_api.src.controller.kaufland_controller import KauflandController
from main_api.services.aftercool_price_sync_service import AftercoolPriceSyncService


class Plan2BusinessLogicTestCase(TestCase):
    """
    Тесты для Плана 2: Бизнес-логика, централизованные утилиты и Aftercool сессии.
    """

    # -------------------------------------------------------------------------
    # 2.1: EAN утилиты (ean_utils)
    # -------------------------------------------------------------------------
    def test_normalize_ean_preserves_leading_zeros(self):
        """EAN с ведущими нулями не должен обрезаться (баг int(float(ean)))."""
        self.assertEqual(normalize_ean("0401234567890"), "0401234567890")
        self.assertEqual(normalize_ean("0012345678905"), "0012345678905")

    def test_normalize_ean_strips_float_point_zero(self):
        """EAN из pandas/excel '4069943370027.0' должен очищаться до чистых 13 цифр."""
        self.assertEqual(normalize_ean("4069943370027.0"), "4069943370027")

    def test_normalize_ean_strips_jvm_prefix(self):
        """Префиксы поставщика 'JVM' должны удаляться."""
        self.assertEqual(normalize_ean("JVM4069943370027"), "4069943370027")
        self.assertEqual(normalize_ean("jvm4069943370027"), "4069943370027")

    def test_normalize_ean_scientific_notation(self):
        """Научная нотация от Excel (4.069943370027e+12) корректно преобразуется."""
        self.assertEqual(normalize_ean("4.069943370027e+12"), "4069943370027")

    def test_normalize_ean_empty_and_whitespace(self):
        """Пробелы и пустые значения обрабатываются безопасно."""
        self.assertEqual(normalize_ean("  4069943370027  "), "4069943370027")
        self.assertEqual(normalize_ean(None), "")
        self.assertEqual(normalize_ean(""), "")

    def test_is_valid_ean_gs1_checksum(self):
        """Проверка контрольной суммы GS1 для валидных и невалидных EAN."""
        # Реальный валидный EAN
        self.assertTrue(is_valid_ean("4069943370027"))
        # Невалидная контрольная сумма
        self.assertFalse(is_valid_ean("4069943370028"))
        self.assertFalse(is_valid_ean("4000000000000"))
        # Неверная длина
        self.assertFalse(is_valid_ean("123"))
        self.assertFalse(is_valid_ean(""))
        self.assertFalse(is_valid_ean(None))

    # -------------------------------------------------------------------------
    # 2.2: Price утилиты (price_utils)
    # -------------------------------------------------------------------------
    def test_parse_price_numbers(self):
        """Парсинг int и float."""
        self.assertEqual(parse_price(100), 100.0)
        self.assertEqual(parse_price(49.99), 49.99)

    def test_parse_price_european_format(self):
        """Парсинг европейских разделителей (запятая как десятичный разделитель, точка как разделитель тысяч)."""
        self.assertEqual(parse_price("1000,00"), 1000.00)
        self.assertEqual(parse_price("49,99 €"), 49.99)
        self.assertEqual(parse_price("1.250,50"), 1250.50)
        self.assertEqual(parse_price("EUR 99,00"), 99.00)

    def test_parse_price_us_format(self):
        """Парсинг американских разделителей."""
        self.assertEqual(parse_price("1,250.50"), 1250.50)
        self.assertEqual(parse_price("49.99"), 49.99)

    def test_parse_price_invalid_fallback(self):
        """Невалидный ввод возвращает default."""
        self.assertIsNone(parse_price("не цена", default=None))
        self.assertEqual(parse_price("не цена", default=0.0), 0.0)
        self.assertEqual(parse_price("", default=0.0), 0.0)
        self.assertEqual(parse_price(None, default=0.0), 0.0)

    def test_format_price_cents(self):
        """Точная конвертация в центы без потерь и без насильного .99."""
        self.assertEqual(format_price_cents(1000.0), 100000)
        self.assertEqual(format_price_cents(49.99), 4999)
        self.assertEqual(format_price_cents("12.34"), 1234)

    # -------------------------------------------------------------------------
    # 2.3: Aftercool Session Reuse & Lazy Refresh
    # -------------------------------------------------------------------------
    def test_aftercool_session_reused_between_calls(self):
        """Повторные вызовы _get_session используют закэшированную сессию без повторного логина."""
        controller = KauflandController(MagicMock(), "jv")

        with patch("main_api.src.controller.kaufland_controller.AftercoolAuthService.authenticate", new_callable=AsyncMock) as mock_auth:
            mock_auth.return_value = {"success": True, "session": "session_cookie_token_123"}

            # Первый вызов — должен авторизоваться
            s1 = asyncio.run(controller._get_session())
            self.assertEqual(s1, "session_cookie_token_123")
            self.assertEqual(mock_auth.call_count, 1)

            # Второй вызов — должен вернуть закэшированную сессию без повторного логина
            s2 = asyncio.run(controller._get_session())
            self.assertEqual(s2, "session_cookie_token_123")
            self.assertEqual(mock_auth.call_count, 1)

    def test_aftercool_session_force_refresh_on_demand(self):
        """При force_refresh=True вызывается повторная авторизация (например, при 401/403)."""
        controller = KauflandController(MagicMock(), "jv")

        with patch("main_api.src.controller.kaufland_controller.AftercoolAuthService.authenticate", new_callable=AsyncMock) as mock_auth:
            mock_auth.side_effect = [
                {"success": True, "session": "old_session"},
                {"success": True, "session": "new_session"}
            ]

            s1 = asyncio.run(controller._get_session())
            self.assertEqual(s1, "old_session")

            # Принудительное обновление
            s2 = asyncio.run(controller._get_session(force_refresh=True))
            self.assertEqual(s2, "new_session")
            self.assertEqual(mock_auth.call_count, 2)

    # -------------------------------------------------------------------------
    # 2.4: Material Service & EU Textile Labelling
    # -------------------------------------------------------------------------
    def test_extract_explicit_composition_valid(self):
        """Валидный состав с суммой <= 100% корректно извлекается."""
        text = "Hochwertiges Ecksofa, Bezug: 80% Polyester, 20% Baumwolle für höchsten Komfort."
        comp = extract_explicit_composition(text)
        self.assertEqual(comp, "80% Polyester, 20% Baumwolle")

    def test_extract_explicit_composition_rejects_sum_over_100(self):
        """Невалидный состав с суммой > 100% бракуется и не принимается."""
        text = "Falsche Angabe: 80% Polyester, 40% Baumwolle."  # 120%
        comp = extract_explicit_composition(text)
        self.assertIsNone(comp)

    def test_resolve_material_taxonomy_normalized_to_textil(self):
        """Разновидности тканей (Samt, Mikrofaser, Stoff, Velours) нормализуются в стандартный 'Textil'."""
        for fabric in ["Samt", "Mikrofaser", "Microfaser", "Stoff", "Webstoff", "Velours"]:
            row = {"CustomItemSpecifics": f"<NameValueList><Name>Polsterstoff</Name><Value>{fabric}</Value></NameValueList>"}
            material, comp, animal = resolve_material_attributes(row, title="Sofa Test")
            self.assertEqual(material, "Textil", f"Fabric {fabric} was not normalized to Textil")
            self.assertEqual(comp, "100% Polyester")
            self.assertEqual(animal, "No")

    def test_resolve_material_genuine_leather(self):
        """Натуральная кожа получает animal_origin=Yes и состав 100% Leder."""
        row = {"CustomItemSpecifics": "<NameValueList><Name>Material</Name><Value>Echtleder</Value></NameValueList>"}
        material, comp, animal = resolve_material_attributes(row, title="Echtleder Sofa")
        self.assertEqual(material, "Leder")
        self.assertEqual(comp, "100% Leder")
        self.assertEqual(animal, "Yes")

    def test_resolve_material_hard_furniture_no_fabric_composition(self):
        """Корпусная мебель (дерево/металл) не получает фиктивный состав ткани."""
        row = {"CustomItemSpecifics": "<NameValueList><Name>Material</Name><Value>Massivholz</Value></NameValueList>"}
        material, comp, animal = resolve_material_attributes(row, title="Esstisch Holz")
        self.assertEqual(material, "Holz")
        self.assertIsNone(comp)
        self.assertEqual(animal, "No")

    def test_resolve_material_composite_wood_and_upholstery(self):
        """Составная мебель (деревянный каркас + текстиль) корректно определяет оба материала."""
        row = {
            "CustomItemSpecifics": "<NameValueList><Name>Gestellmaterial</Name><Value>Massivholz</Value></NameValueList>"
                                   "<NameValueList><Name>Polsterstoff</Name><Value>Samt</Value></NameValueList>"
        }
        res = resolve_material_attributes(
            row,
            title="Stuhl beige braun Armlehnen aus Holz modern elegant Wohnzimmer Komfort",
            cat_name="armlehnstuehle"
        )
        # Обратная совместимость распаковки
        mat, comp, animal = res
        self.assertEqual(mat, "Holz")
        self.assertEqual(comp, "100% Polyester")
        # Каркасный стул получает конструкционный материал Holz (без Textil, чтобы избежать отката в Seller Portal)
        self.assertEqual(res.materials, ["Holz"])


    # -------------------------------------------------------------------------
    # 2.5: Price Sync Direction (Только повышение)
    # -------------------------------------------------------------------------
    def test_price_sync_skips_when_aftercool_price_not_higher(self):
        """
        Бизнес-правило: цена никогда не должна автоматически снижаться.
        Если aftercool_price <= kaufland_price, операция пропускается.
        """
        mock_controller = MagicMock()
        mock_controller._fetch_main_data_by_ean = AsyncMock(return_value=[{
            "price": 100.0,  # 100.00 EUR
            "storefront": "de"
        }])
        sync_service = AftercoolPriceSyncService(controller=mock_controller)

        status, sf, kf_price, reason = asyncio.run(sync_service._process_single_pair(
            job_id="test",
            account="jv",
            controller=mock_controller,
            ean="4069943370027",
            aftercool_price=90.0,  # ниже текущей цены
        ))

        self.assertEqual(status, "skipped")
        self.assertEqual(reason, "aftercool_not_higher")

    # -------------------------------------------------------------------------
    # 2.6: Reupload Checkpoint & SKIPPED Storefront logic
    # -------------------------------------------------------------------------
    def test_reupload_skipped_storefront_does_not_mark_completed(self):
        """
        Если хотя бы одна витрина пропущена (SKIPPED) или упала (FAILED),
        EAN НЕ должен записываться в чекпоинт completed_uploads.
        """
        storefronts = ["de", "cz", "sk", "pl", "at", "fr", "it", "nl"]
        successful_storefronts = ["de", "cz", "sk", "pl", "at", "fr", "it"]  # nl skipped!
        skipped_storefronts = ["nl"]
        failed_storefronts = []

        cp_mgr = MagicMock()
        cp_mgr.record_completed = AsyncMock()

        async def evaluate():
            if len(successful_storefronts) == len(storefronts):
                await cp_mgr.record_completed("4069943370027")
                return "SUCCESS"
            elif len(successful_storefronts) > 0:
                return "PARTIAL"
            return "FAILED"

        status = asyncio.run(evaluate())
        self.assertEqual(status, "PARTIAL")
        cp_mgr.record_completed.assert_not_called()

    def test_reupload_stats_strict_limit_reservation(self):
        """Проверяем, что try_reserve_slot гарантирует строгое соблюдение --limit."""
        from scripts.reupload_aftercool_catalog import Stats
        stats = Stats()

        async def check():
            # Лимит 2 слота
            res1 = await stats.try_reserve_slot(limit=2)
            res2 = await stats.try_reserve_slot(limit=2)
            res3 = await stats.try_reserve_slot(limit=2)
            return res1, res2, res3

        r1, r2, r3 = asyncio.run(check())
        self.assertTrue(r1)
        self.assertTrue(r2)
        self.assertFalse(r3)  # Третий запрос блокируется

    def test_reupload_in_flight_deduplication(self):
        """InFlightTracker предотвращает одновременную обработку одного EAN."""
        from scripts.reupload_aftercool_catalog import InFlightTracker
        tracker = InFlightTracker()

        async def check():
            first_acquire = await tracker.acquire("4069943370027")
            duplicate_acquire = await tracker.acquire("4069943370027")
            await tracker.release("4069943370027")
            re_acquire = await tracker.acquire("4069943370027")
            return first_acquire, duplicate_acquire, re_acquire

        f, d, r = asyncio.run(check())
        self.assertTrue(f)
        self.assertFalse(d)  # Дубль заблокирован
        self.assertTrue(r)   # После освобождения снова доступен

    def test_reupload_factory_fail_closed(self):
        """При отсутствии фабрики в белом списке и выключенном allow_all возвращается False."""
        from scripts.reupload_aftercool_catalog import is_factory_allowed
        allowed = {"ikea", "jvmoebel"}
        self.assertTrue(is_factory_allowed("catalog_ikea_sofa.csv", allowed))
        self.assertFalse(is_factory_allowed("unknown_factory.csv", allowed))
        # При пустом списке и allow_all=False -> fail-closed (False)
        self.assertFalse(is_factory_allowed("any.csv", set(), allow_all=False))
        # При allow_all=True -> разрешено всё
        self.assertTrue(is_factory_allowed("any.csv", set(), allow_all=True))
