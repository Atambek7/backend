import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from django.test import TestCase

import config
from config import get_account_storefronts, storefronts_by_account
from main_api.src.controller.kaufland_controller import KauflandController
from main_api.src.controller.rest_api_controller import RestApiController
from main_api.src.servises.json_mapper import _normalize_ean, map_json_item
from main_api.services.aftercool_service import AftercoolService


class Plan1CriticalFixesTestCase(TestCase):
    """
    Тесты для Плана 1: Критические баги данных (P0).
    """

    # -------------------------------------------------------------------------
    # 1.1 и 1.2: Форматирование цены (format_price) и обработка float
    # -------------------------------------------------------------------------
    def test_format_price_user_requirement_1000_eur(self):
        """1000 EUR должно конвертироваться ровно в 100000 центов (1000,00 €)."""
        self.assertEqual(KauflandController.format_price(1000.0), 100000)
        self.assertEqual(RestApiController.format_price(1000.0), 100000)

    def test_format_price_exact_cents(self):
        """Цены с центами не должны округляться до .99 или обрезаться int()."""
        self.assertEqual(KauflandController.format_price(49.99), 4999)
        self.assertEqual(RestApiController.format_price(49.99), 4999)
        self.assertEqual(KauflandController.format_price(12.34), 1234)
        self.assertEqual(RestApiController.format_price(12.34), 1234)

    def test_make_price_czk_pln_tax(self):
        """Проверяем конвертацию цен для витрин с курсами."""
        controller = KauflandController(MagicMock(), "jv")

        async def check():
            price_de = await controller._make_price("de", 100.0)
            price_cz = await controller._make_price("cz", 100.0)
            price_pl = await controller._make_price("pl", 100.0)
            price_it = await controller._make_price("it", 100.0)
            return price_de, price_cz, price_pl, price_it

        de, cz, pl, it = asyncio.run(check())
        self.assertEqual(de, 10000)          # 100.00 EUR -> 10000 центов
        self.assertEqual(cz, 254000)         # 100 * 25.4 = 2540 CZK -> 254000
        self.assertEqual(pl, 47000)          # 100 * 4.7 = 470 PLN -> 47000
        self.assertEqual(it, 10500)          # 100 * 1.05 = 105 EUR -> 10500

    # -------------------------------------------------------------------------
    # 1.3: Извлечение stock/amount из Aftercool (Menge/Bestand) и дефолт 20
    # -------------------------------------------------------------------------
    def test_map_json_item_extracts_menge_as_amount(self):
        """Если в Aftercool указана Menge, она должна извлекаться как amount."""
        raw_item = {
            "Artikelbeschreibung": "Test Sofa",
            "Startpreis": "500",
            "Menge": "7",
            "EAN": "4069943370027",
        }
        mapped = map_json_item(raw_item)
        self.assertEqual(mapped["amount"], 7)

    def test_map_json_item_default_amount_when_missing(self):
        """Если stock не указан в Aftercool, по умолчанию ставится 20."""
        raw_item = {
            "Artikelbeschreibung": "Test Sofa",
            "Startpreis": "500",
            "EAN": "4069943370027",
        }
        mapped = map_json_item(raw_item)
        self.assertEqual(mapped["amount"], 20)

    def test_map_json_item_preserves_zero_stock(self):
        """Если товар имеет явный stock=0, он должен сохраняться (а не превращаться в 20)."""
        raw_item = {
            "Artikelbeschreibung": "Test Sofa",
            "Startpreis": "500",
            "Menge": "0",
            "EAN": "4069943370027",
        }
        mapped = map_json_item(raw_item)
        self.assertEqual(mapped["amount"], 0)

    # -------------------------------------------------------------------------
    # 1.4: Разделение storefronts по аккаунтам (JV vs XL)
    # -------------------------------------------------------------------------
    def test_storefronts_split_jv_and_xl(self):
        """JV имеет nl и es; XL не имеет ни nl, ни es."""
        jv_sfs = get_account_storefronts("jv")
        xl_sfs = get_account_storefronts("xl")

        self.assertIn("nl", jv_sfs)
        self.assertIn("es", jv_sfs)
        self.assertEqual(len(jv_sfs), 9)

        self.assertNotIn("nl", xl_sfs)
        self.assertNotIn("es", xl_sfs)
        self.assertEqual(len(xl_sfs), 7)

    def test_kaufland_controller_uses_account_storefronts(self):
        """Контроллер JV и XL возвращает соответствующие витрины."""
        jv_controller = KauflandController(MagicMock(), "jv")
        xl_controller = KauflandController(MagicMock(), "xl")

        self.assertEqual(jv_controller.get_account_storefronts(), ["de", "cz", "sk", "pl", "at", "fr", "it", "nl", "es"])
        self.assertEqual(xl_controller.get_account_storefronts(), ["de", "cz", "sk", "pl", "at", "fr", "it"])

    # -------------------------------------------------------------------------
    # 1.5: _update_price_by_unit_id: не возвращать True при отсутствии юнитов
    # -------------------------------------------------------------------------
    def test_update_price_returns_false_when_no_units_found(self):
        """Если юнитов нет на витрине, _update_price_by_unit_id должен возвращать False."""
        controller = KauflandController(MagicMock(), "jv")
        controller._fetch_unit_id_by_ean = AsyncMock(return_value={})

        async def run():
            return await controller._update_price_by_unit_id("4069943370027", 100.0)

        result = asyncio.run(run())
        self.assertFalse(result)

    def test_update_price_returns_false_on_not_found_response(self):
        """Если PATCH возвращает 'not found', это не успех, а False."""
        controller = KauflandController(MagicMock(), "jv")
        controller._patch_unit_payload = AsyncMock(return_value={})
        controller._universal_request = AsyncMock(return_value={"message": "not found"})

        async def run():
            return await controller._update_price_by_unit_id(
                "4069943370027", 100.0, new_unit_id={"de": "unit_123"}
            )

        result = asyncio.run(run())
        self.assertFalse(result)

    # -------------------------------------------------------------------------
    # 1.6: upload_via_json: all([]) == True bug
    # -------------------------------------------------------------------------
    def test_upload_via_json_returns_none_for_empty_list(self):
        """upload_via_json не должен возвращать True, если список данных пустой."""
        controller = KauflandController(MagicMock(), "jv")

        async def run():
            return await controller.upload_via_json([])

        result = asyncio.run(run())
        self.assertIsNone(result)

    # -------------------------------------------------------------------------
    # 1.7: EAN нормализация — сохранение ведущих нулей
    # -------------------------------------------------------------------------
    def test_ean_normalization_preserves_leading_zeros(self):
        """EAN с ведущим нулем не должен терять ноль при нормализации."""
        ean_with_zero = "0401234567890"
        ean_float_string = "0401234567890.0"

        # KauflandController
        self.assertEqual(KauflandController.normalize_ean(ean_with_zero), ean_with_zero)
        self.assertEqual(KauflandController.normalize_ean(ean_float_string), ean_with_zero)

        # json_mapper
        self.assertEqual(_normalize_ean(ean_with_zero), ean_with_zero)
        self.assertEqual(_normalize_ean(ean_float_string), ean_with_zero)

        # AftercoolService
        self.assertEqual(AftercoolService._normalize_ean(ean_with_zero), ean_with_zero)
        self.assertEqual(AftercoolService._normalize_ean(ean_float_string), ean_with_zero)
