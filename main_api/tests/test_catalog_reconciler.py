import sys
import unittest
import tempfile
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BACKEND_DIR))

from main_api.services.catalog_reconciler import (
    is_uk_source,
    select_canonical_item,
    CatalogReconciler,
)


class CatalogReconcilerTestCase(unittest.TestCase):
    def test_is_uk_source(self):
        self.assertTrue(is_uk_source("UK-SCORZANYSWIAT_482505.csv"))
        self.assertTrue(is_uk_source("uk-furniture.xlsx"))
        self.assertTrue(is_uk_source("factory_UK_warehouse.csv"))
        self.assertTrue(is_uk_source("factory_(UK).csv"))
        self.assertTrue(is_uk_source("warehouse_uk.csv"))

        # Valid EU factories must return False
        self.assertFalse(is_uk_source("SKORZANYSWIAT__PL__481958.csv"))
        self.assertFalse(is_uk_source("ABSY_(PL).csv"))
        self.assertFalse(is_uk_source("Accad_Italy_(IT).xlsx"))
        self.assertFalse(is_uk_source("Alikotuk_(TR).csv"))
        self.assertFalse(is_uk_source(None))
        self.assertFalse(is_uk_source(""))

    def test_select_canonical_item_filters_uk(self):
        items = [
            {
                "id": 1,
                "source_file": "UK-SCORZANYSWIAT_482505.csv",
                "name": "Footstool Leather UK",
                "price": "799",
                "row": {"Startpreis": "799", "Artikelbeschreibung": "English footstool"},
            },
            {
                "id": 2,
                "source_file": "SKORZANYSWIAT__PL__481958.csv",
                "name": "Chesterfield Hocker Leder",
                "price": "499",
                "row": {"Startpreis": "499", "Artikelbeschreibung": "Deutscher Hocker Leder"},
            },
        ]

        canonical = select_canonical_item(items)
        self.assertIsNotNone(canonical)
        self.assertEqual(canonical["id"], 2)
        self.assertEqual(canonical["source_file"], "SKORZANYSWIAT__PL__481958.csv")
        self.assertEqual(canonical["price"], "499")

    def test_select_canonical_item_all_uk(self):
        items = [
            {"id": 1, "source_file": "UK-SCORZANYSWIAT_482505.csv"},
            {"id": 2, "source_file": "warehouse_uk.csv"},
        ]
        canonical = select_canonical_item(items)
        self.assertIsNone(canonical)

    def test_resolve_target_account_matrix(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            overlap_file = Path(tmp_dir) / "overlap_test.txt"
            reconciler = CatalogReconciler(overlap_file=overlap_file)

            # Use valid GS1 EANs:
            # 4070863480994, 4067282583641, 4062292038303, 4062292017445
            reconciler.jv_eans = {"4067282583641", "4062292038303"}
            reconciler.xl_eans = {"4062292038303", "4070863480994"}
            reconciler.is_initialized = True

            # 1. ONLY_JV
            acc, status = reconciler.resolve_target_account("4067282583641")
            self.assertEqual(acc, "jv")
            self.assertEqual(status, "ONLY_JV")

            # 2. OVERLAP -> JV has priority
            acc, status = reconciler.resolve_target_account("4062292038303")
            self.assertEqual(acc, "jv")
            self.assertEqual(status, "OVERLAP")

            # 3. ONLY_XL
            acc, status = reconciler.resolve_target_account("4070863480994")
            self.assertEqual(acc, "xl")
            self.assertEqual(status, "ONLY_XL")

            # 4. ORPHAN
            acc, status = reconciler.resolve_target_account("4062292017445")
            self.assertIsNone(acc)
            self.assertEqual(status, "ORPHAN")

            # 5. INVALID_EAN
            acc, status = reconciler.resolve_target_account("12345")
            self.assertIsNone(acc)
            self.assertEqual(status, "INVALID_EAN")


if __name__ == "__main__":
    unittest.main()
