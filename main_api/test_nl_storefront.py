import asyncio
from unittest.mock import MagicMock
from django.test import TestCase

import config
from main_api.external_views import PatchProductView
from main_api.external_serializer import (
    ProductDataSerializer,
    PutDataSerializer,
)
from main_api.src.controller.kaufland_controller import KauflandController
from main_api.src.controller.rest_api_controller import RestApiController


class NLStorefrontTestCase(TestCase):
    def test_config_storefronts_contains_nl(self):
        """Проверяем, что 'nl' присутствует в глобальном списке витрин config.py."""
        self.assertIn("nl", config.storefronts)

    def test_to_locale_value_mapping(self):
        """Проверяем, что метод to_locale_value корректно возвращает 'nl-NL' для 'nl'."""
        view = PatchProductView()
        self.assertEqual(view.to_locale_value("nl"), "nl-NL")
        self.assertEqual(view.to_locale_value("de"), "de-DE")
        self.assertEqual(view.to_locale_value("cz"), "cs-CZ")

    def test_product_data_serializer_accepts_nl(self):
        """Проверяем, что сериализатор данных товара принимает storefront='nl'."""
        serializer = ProductDataSerializer(
            data={
                "ean": "4000000000000",
                "storefront": "nl",
                "controller": "jv",
                "price": "199.99",
            }
        )
        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertEqual(serializer.validated_data["storefront"], "nl")

    def test_put_data_serializer_accepts_nl(self):
        """Проверяем, что PutDataSerializer валидирует список витрин с 'nl'."""
        payload = {
            "ean": "4069943370027",
            "controller": "jv",
            "title": "Test Product",
            "description": "Test description",
            "picture": ["https://example.com/image.jpg"],
            "price": "99.99",
            "size": "L",
            "color": "Black",
            "material": "Wood",
            "delivery": 14,
            "height": "100.00",
            "length": "50.00",
            "width": "60.00",
            "storefronts": ["de", "nl", "at"],
        }
        serializer = PutDataSerializer(data=payload)
        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertIn("nl", serializer.validated_data["storefronts"])

    def test_kaufland_controller_make_price_for_nl(self):
        """Проверяем, что для Нидерландов цена рассчитывается в базовой валюте EUR (без надбавок)."""
        session_mock = MagicMock()
        controller = KauflandController(session_mock, "jv")

        async def run():
            price_de = await controller._make_price("de", 100.0)
            price_nl = await controller._make_price("nl", 100.0)
            return price_de, price_nl

        price_de, price_nl = asyncio.run(run())
        self.assertEqual(price_nl, price_de)

    def test_rest_api_controller_make_price_for_nl(self):
        """Проверяем расчет цены в RestApiController для 'nl'."""
        session_mock = MagicMock()
        controller = RestApiController("jv", session_mock)
        price_de = controller.make_price("de", 100.0)
        price_nl = controller.make_price("nl", 100.0)
        self.assertEqual(price_nl, price_de)
