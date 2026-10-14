from unittest.mock import patch, AsyncMock
from rest_framework.test import APITestCase
from django.urls import reverse


class PatchProductViewTest(APITestCase):
    
    @patch("main_api.src.controller.rest_api_controller.RestApiController.send_request", new_callable=AsyncMock)
    def test_patch_product(self, mock_send):
        mock_send.return_value = {"status": 200, "data": {}}
        url = reverse("patch_product")
        
        payload = {
            "ean": "4343434343432",
            "storefront": "de",
            "controller": "jv",
            "price": 1999.2,
            "unit_id": "11111111",
            "description": "Some kind of description",
        }
                
        response = self.client.patch(url, data=payload, format="json")

        assert response.status_code == 200
        assert response.data["received_data"]["ean"] == "4343434343432"


class MainOperationsSingleEanCheckerTest(APITestCase):
    def setUp(self):
        from django.contrib.auth.models import User
        self.user = User.objects.create_user(username="checker_user", password="secret_password")
        self.client.force_authenticate(user=self.user)
        self.url = reverse("main_operations")

    @patch("main_api.src.controller.kaufland_controller.KauflandController._check_product_by_unit_id", new_callable=AsyncMock)
    def test_single_ean_checker_success(self, mock_check):
        mock_check.return_value = {
            "found": True,
            "storefronts": ["de", "cz"],
            "items": [
                {"ean": "4069943360264", "title": "Bett", "price": 2249.0, "storefront": "de"}
            ],
            "message": None,
        }
        payload = {
            "mode": "checker",
            "controller": "jv",
            "ean": "4069943360264",
        }
        response = self.client.post(self.url, data=payload, format="json")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data.get("found"), True)
        self.assertEqual(response.data.get("ean"), "4069943360264")
        self.assertEqual(len(response.data.get("items", [])), 1)

    @patch("main_api.src.controller.kaufland_controller.KauflandController._check_product_by_unit_id", new_callable=AsyncMock)
    def test_single_ean_checker_not_found(self, mock_check):
        mock_check.return_value = {
            "found": False,
            "storefronts": [],
            "items": [],
            "message": "not found",
        }
        payload = {
            "mode": "checker",
            "controller": "jv",
            "ean": "407210228358",
        }
        response = self.client.post(self.url, data=payload, format="json")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data.get("found"), False)
        self.assertEqual(response.data.get("message"), "not found")

    def test_single_ean_checker_missing_ean(self):
        payload = {
            "mode": "checker",
            "controller": "jv",
        }
        response = self.client.post(self.url, data=payload, format="json")
        self.assertEqual(response.status_code, 400)
        self.assertTrue("error" in response.data or "non_field_errors" in response.data)