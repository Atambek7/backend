import unittest
from main_api.services.catalog_reconciler import (
    is_blacklisted_source,
    is_trash_title,
    is_uk_source,
    select_canonical_item,
)
from scripts.reupload_aftercool_catalog import is_factory_allowed


class TestTrashBlacklistSafety(unittest.TestCase):
    def test_is_blacklisted_source(self):
        trash_sources = [
            "Others_Loschen_502344.csv",
            "Others_Loschen_482403.csv",
            "DELETE_DONT_TOUCH_Go-Bi_1_502007.csv",
            "DELETE_DONT_TOUCH_Go-Bi_Kunstdruck_502096.csv",
            "Others_SOFORT_LIEFERBAR_DELETE_DONT_TOUCH__393532.csv",
            "Zaun_Tor__PL_NOT_FOR_MARKETPLACES_DONT_TOUCH__499447.csv",
            "SORTIERUNG_482963.csv",
            "UK_WRONG_ATTRIBUTE_WRONG_CONTACT_498217.csv",
            "Kromev__TR_Delete_Don_t_Touch__502037.csv",
            "DELETE_DONT_TOUCH_Weitao-NO_AKTIV__CN__478562.csv",
            "FEHLERBEHEBUNG_DONT_TOUCH_503636.csv",
            "Others_Only_Produktbeschreibung_DE_Dont_touch__501455.csv",
        ]
        for src in trash_sources:
            self.assertTrue(
                is_blacklisted_source(src),
                f"Source {src} should be detected as blacklisted",
            )

        valid_sources = [
            "ABSY_(PL).csv",
            "Alfitalia_(IT).csv",
            "SIMBA__PL__502334.csv",
            "BASSI_(TR).csv",
            "AZARDI_(PL).csv",
        ]
        for src in valid_sources:
            self.assertFalse(
                is_blacklisted_source(src),
                f"Source {src} should NOT be detected as blacklisted",
            )

    def test_is_trash_title(self):
        trash_titles = [
            "Delette",
            "delette",
            "Delete",
            "Product Removed",
            "Product removed new",
            "gelöscht",
            "geloscht",
            "löschen",
            "loschen",
            "test",
            "entfernt",
            "TopLine - Product removed",
            "TopLine [Product removed]",
            "Bed delette",
            "Sofa (delete)",
            "Product removed.",
            "...",
            "123",
            "ab",
        ]
        for t in trash_titles:
            self.assertTrue(
                is_trash_title(t),
                f"Title '{t}' should be detected as trash",
            )

        valid_titles = [
            "Chesterfield Sofa 3 Sitzer Couch Polster Samt Stoff",
            "Esstisch Massivholz Eiche Ausziehbar",
            "Luxus Doppelbett Boxspringbett 180x200",
            "Designer Stuhl Leder Esszimmer",
            "Stiftung Warentest Sehr Gut Matratze",
        ]
        for t in valid_titles:
            self.assertFalse(
                is_trash_title(t),
                f"Title '{t}' should NOT be detected as trash",
            )

    def test_is_factory_allowed_blocks_trash_even_with_allow_all(self):
        # Even with allow_all=True, trash files MUST be blocked
        self.assertFalse(
            is_factory_allowed("Others_Loschen_502344.csv", set(), allow_all=True)
        )
        self.assertFalse(
            is_factory_allowed("DELETE_DONT_TOUCH_Go-Bi_1_502007.csv", set(), allow_all=True)
        )
        self.assertFalse(
            is_factory_allowed("UK-GANASI_498371.csv", set(), allow_all=True)
        )

        # Valid suppliers are allowed with allow_all=True
        self.assertTrue(
            is_factory_allowed("ABSY_(PL).csv", set(), allow_all=True)
        )
        self.assertTrue(
            is_factory_allowed("SIMBA__PL__502334.csv", set(), allow_all=True)
        )

    def test_select_canonical_item_filters_trash_and_uk(self):
        items = [
            {
                "product_id": "1",
                "ean": "4062292209611",
                "name": "Delette",
                "source_file": "Others_Loschen_502344.csv",
            },
            {
                "product_id": "2",
                "ean": "4062292209611",
                "name": "Product Removed",
                "source_file": "DELETE_DONT_TOUCH_Go-Bi_1_502007.csv",
            },
        ]
        canonical = select_canonical_item(items, allow_all_factories=True)
        self.assertIsNone(canonical, "All trash items must result in None canonical item")

        valid_item = {
            "product_id": "3",
            "ean": "4062292209611",
            "name": "Lehnstuhl Samt Polsterstuhl Modern",
            "source_file": "SIMBA__PL__502334.csv",
            "row": {"Artikelbeschreibung": "Hochwertiger Stuhl mit Armlehne"},
        }
        items.append(valid_item)
        canonical = select_canonical_item(items, allow_all_factories=True)
        self.assertIsNotNone(canonical)
        self.assertEqual(canonical["product_id"], "3")


if __name__ == "__main__":
    unittest.main()
