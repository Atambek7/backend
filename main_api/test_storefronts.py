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


class StorefrontsTestCase(TestCase):
    def test_config_storefronts_contains_nl_and_es(self): 
        """Проверяем, что 'nl' и 'es' присутствуют в глобальном списке витрин config.py."""
        self.assertIn("nl", config.storefronts)
        self.assertIn("es", config.storefronts)

    def test_to_locale_value_mapping(self):
        """Проверяем, что метод to_locale_value корректно возвращает локали для всех стран."""
        view = PatchProductView()
        self.assertEqual(view.to_locale_value("de"), "de-DE")
        self.assertEqual(view.to_locale_value("at"), "de-AT")
        self.assertEqual(view.to_locale_value("cz"), "cs-CZ")
        self.assertEqual(view.to_locale_value("sk"), "sk-SK")
        self.assertEqual(view.to_locale_value("pl"), "pl-PL")
        self.assertEqual(view.to_locale_value("fr"), "fr-FR")
        self.assertEqual(view.to_locale_value("it"), "it-IT")
        self.assertEqual(view.to_locale_value("nl"), "nl-NL")
        self.assertEqual(view.to_locale_value("es"), "es-ES")

    def test_product_data_serializer_accepts_es(self):
        """Проверяем, что сериализатор данных товара принимает storefront='es'."""
        serializer = ProductDataSerializer(
            data={
                "ean": "4000000000000",
                "storefront": "es",
                "controller": "jv",
                "price": "199.99",
            }
        )
        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertEqual(serializer.validated_data["storefront"], "es")

    def test_put_data_serializer_accepts_es(self):
        """Проверяем, что PutDataSerializer валидирует список витрин с 'es'."""
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
            "storefronts": ["de", "nl", "es", "at"],
        }
        serializer = PutDataSerializer(data=payload)
        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertIn("es", serializer.validated_data["storefronts"])
        self.assertIn("nl", serializer.validated_data["storefronts"])

    def test_kaufland_controller_make_price_for_es(self):
        """Проверяем, что для Испании цена рассчитывается в базовой валюте EUR (без надбавок)."""
        session_mock = MagicMock()
        controller = KauflandController(session_mock, "jv")

        async def run():
            price_de = await controller._make_price("de", 100.0)
            price_es = await controller._make_price("es", 100.0)
            return price_de, price_es

        price_de, price_es = asyncio.run(run())
        self.assertEqual(price_es, price_de)

    def test_rest_api_controller_make_price_for_es(self):
        """Проверяем расчет цены в RestApiController для 'es'."""
        session_mock = MagicMock()
        controller = RestApiController("jv", session_mock)
        price_de = controller.make_price("de", 100.0)
        price_es = controller.make_price("es", 100.0)
        self.assertEqual(price_es, price_de)
