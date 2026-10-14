from django.test import TestCase
from main_api.src.utils.text_utils import (
    sanitize_title,
    sanitize_description,
    is_blacklisted_category,
    resolve_fallback_category,
    generate_localized_description,
)
from main_api.src.utils.ean_utils import is_valid_ean


class TextUtilsTestCase(TestCase):
    def test_sanitize_title_stop_words_and_exclamation(self):
        title = "design Sofa 3 Sitzer Neu!"
        cleaned = sanitize_title(title)
        self.assertEqual(cleaned, "Design Sofa 3 Sitzer")

        title2 = "Super Angebot! Tolles Bett!! Reduziert!"
        cleaned2 = sanitize_title(title2)
        self.assertEqual(cleaned2, "Tolles Bett")

    def test_sanitize_title_material_conflict_resolution(self):
        title = "Leder Textil Ecksofa Couch"
        # If material is Leder -> Textil should be removed
        cleaned_leder = sanitize_title(title, material="Leder")
        self.assertEqual(cleaned_leder, "Leder Ecksofa Couch")

        # If material is Textil -> Leder should be removed
        cleaned_textil = sanitize_title(title, material="Textil")
        self.assertEqual(cleaned_textil, "Textil Ecksofa Couch")

    def test_sanitize_title_capitalization_and_spaces(self):
        title = "  schöne moderne Wohnwand   in Eiche  "
        cleaned = sanitize_title(title)
        self.assertEqual(cleaned, "Schöne moderne Wohnwand in Eiche")

    def test_sanitize_title_truncation(self):
        long_title = "A" * 300
        cleaned = sanitize_title(long_title)
        self.assertEqual(len(cleaned), 255)

    def test_sanitize_title_empty_or_none(self):
        self.assertEqual(sanitize_title(""), "")
        self.assertEqual(sanitize_title(None), "")
        self.assertEqual(sanitize_title("   "), "")

    def test_sanitize_title_idempotency(self):
        samples = [
            ("design Sofa 3 Sitzer Neu!", "Leder"),
            ("Super Angebot! Tolles Bett!! Reduziert!", None),
            ("Leder Textil Ecksofa Couch", "Textil"),
            ("Kleiderschrank 4-türig weiß Hochglanz", "Holz"),
            ("Schreibtisch höhenverstellbar", None),
            ("Günstig! Sale! Couchtisch Glas", None),
            ("Aktion: Esszimmerstuhl 4er Set", "Stoff"),
            ("Echtes Leder Sofa 2er", "Leder"),
            ("123 Regal Metall", "Metall"),
            ("Sonderangebot Polsterbett 180x200", None),
        ]
        for title, mat in samples:
            first = sanitize_title(title, material=mat)
            second = sanitize_title(first, material=mat)
            self.assertEqual(first, second, f"Failed idempotency on: '{title}'")

    def test_sanitize_description_control_chars_and_emojis(self):
        desc = "Modernes Sofa\x00\x08 mit Komfort 🛋️🔥 und Stil."
        cleaned = sanitize_description(desc)
        self.assertNotIn("\x00", cleaned)
        self.assertNotIn("\x08", cleaned)
        self.assertNotIn("🛋️", cleaned)
        self.assertNotIn("🔥", cleaned)
        self.assertTrue(cleaned.startswith("<p>"))
        self.assertTrue(cleaned.endswith("</p>"))

    def test_sanitize_description_wrapping_html(self):
        desc = "Dies ist ein einfaches Sofa ohne HTML-Tags."
        cleaned = sanitize_description(desc)
        self.assertTrue(cleaned.startswith("<p>"))
        self.assertTrue(cleaned.endswith("</p>"))

        # If already has HTML
        desc_html = "<div><p>Bereits formatierter Text mit vielen Details für den Raum.</p></div>"
        cleaned_html = sanitize_description(desc_html, min_length=50)
        self.assertFalse(cleaned_html.startswith("<p><div"))

    def test_sanitize_description_padding_min_length(self):
        short_desc = "Kurzes Sofa."
        cleaned = sanitize_description(short_desc, min_length=150)
        self.assertGreaterEqual(len(cleaned), 150)
        self.assertIn("Hochwertige Möbel für Ihr Zuhause", cleaned)

        # Already long description should not get padding
        long_text = "Ein sehr langes Sofa " * 15
        cleaned_long = sanitize_description(long_text, min_length=150)
        self.assertNotIn("Hochwertige Möbel für Ihr Zuhause", cleaned_long)

    def test_sanitize_description_empty_or_none(self):
        cleaned_none = sanitize_description(None)
        self.assertIn("Hochwertige Möbel für Ihr Zuhause", cleaned_none)
        self.assertTrue(cleaned_none.startswith("<p>"))
        self.assertTrue(cleaned_none.endswith("</p>"))

    def test_sanitize_description_idempotency(self):
        samples = [
            "Kurzes Sofa.",
            "<p>Ein bereits umhüllter Text mit minimaler Länge.</p>",
            "Modernes Sofa\x00 mit Komfort 🛋️ und Stil.",
            "<div>Hier ist ein HTML-Block mit verschiedenen Elementen und Möbelstücken für das Wohnzimmer.</div>",
            "Ein sehr langes Sofa mit vielen Details und Eigenschaften " * 5,
        ]
        for text in samples:
            first = sanitize_description(text, min_length=150)
            second = sanitize_description(first, min_length=150)
            self.assertEqual(first, second, f"Failed idempotency on: '{text}'")

    def test_category_blacklist_detection(self):
        # Опасная химия
        self.assertTrue(is_blacklisted_category("Entkalker"))
        self.assertTrue(is_blacklisted_category("WC-Reiniger für Sanitär"))
        self.assertTrue(is_blacklisted_category("Kaffeemaschinenreiniger flüssig"))
        
        # Продукты питания
        self.assertTrue(is_blacklisted_category("Pralinen"))
        self.assertTrue(is_blacklisted_category("Roséwein 0.75l"))
        self.assertTrue(is_blacklisted_category("Backmischungen für Kuchen"))
        self.assertTrue(is_blacklisted_category("Babynahrung"))

        # Косметика
        self.assertTrue(is_blacklisted_category("Feuchtigkeitscremes & Lotionen"))
        self.assertTrue(is_blacklisted_category("Seifen"))

        # Крупная бытовая техника
        self.assertTrue(is_blacklisted_category("Kühlschränke"))
        self.assertTrue(is_blacklisted_category("Geschirrspüler einbaufähig"))
        self.assertTrue(is_blacklisted_category("Waschmaschinen Frontlader"))
        self.assertTrue(is_blacklisted_category("Staubsauger kabellos"))

        # Легитимная мебель - НЕ должна быть в черном списке
        self.assertFalse(is_blacklisted_category("Klassische Sofas"))
        self.assertFalse(is_blacklisted_category("Polsterbetten"))
        self.assertFalse(is_blacklisted_category("Möbel"))
        self.assertFalse(is_blacklisted_category("Wohnwände"))
        self.assertFalse(is_blacklisted_category("Couchtische"))
        self.assertFalse(is_blacklisted_category("Schreibtische & Computertische"))
        self.assertFalse(is_blacklisted_category("Gartenmöbel"))

        # Граничные случаи
        self.assertFalse(is_blacklisted_category(""))
        self.assertFalse(is_blacklisted_category(None))

    def test_ean_validation_gate(self):
        # 9-значный внутренний артикул Aftercool не должен проходить валидацию EAN
        self.assertFalse(is_valid_ean("165357350"))
        
        # 12-значный неполный штрихкод
        self.assertFalse(is_valid_ean("123456789012"))

        # Валидный 13-значный EAN
        self.assertTrue(is_valid_ean("4062292093548"))
        self.assertTrue(is_valid_ean("4067282755529"))
        self.assertTrue(is_valid_ean("4069424794663"))

        # Невалидная контрольная сумма
        self.assertFalse(is_valid_ean("4062292093549"))

    def test_get_or_decide_category_blacklist_fallback(self):
        import asyncio
        from unittest.mock import AsyncMock, MagicMock
        from scripts.reupload_aftercool_catalog import get_or_decide_category, GlobalRateLimiter

        rac = MagicMock()
        rac.send_request = AsyncMock(return_value={"data": [{"id_category": 9999, "name": "Entkalker & Reiniger"}]})
        rate_limiter = GlobalRateLimiter(rate=50.0)

        # 1. Fallback allowed -> should return (46001, "Möbel")
        cat_id, cat_name = asyncio.run(
            get_or_decide_category(
                rac=rac,
                rate_limiter=rate_limiter,
                title="Spezial Entkalker Tabs",
                description="Entkalker für Kaffeemaschinen",
                manufacturer="TestChem",
                price=19.99,
                allow_fallback=True,
                account="jv",
                ean="4067282755529",
            )
        )
        self.assertEqual(cat_id, 46001)
        self.assertEqual(cat_name, "Möbel")

        # 2. Fallback disabled -> should return (None, None)
        rac.send_request = AsyncMock(return_value={"data": [{"id_category": 8888, "name": "WC-Reiniger Gel"}]})
        cat_id2, cat_name2 = asyncio.run(
            get_or_decide_category(
                rac=rac,
                rate_limiter=rate_limiter,
                title="Kraftvoller WC-Reiniger",
                description="Reinigungsmittel für Sanitär",
                manufacturer="TestClean",
                price=5.99,
                allow_fallback=False,
                account="jv",
                ean="4069424794663",
            )
        )
        self.assertIsNone(cat_id2)
        self.assertIsNone(cat_name2)

    def test_sanitize_title_leading_punctuation_and_composite_materials(self):
        # 1. Лидирующие дефисы и двоеточия после вырезания стоп-слов
        self.assertEqual(sanitize_title("Top-Angebot Sofa"), "Sofa")
        self.assertEqual(sanitize_title("Sale: Tolles Polsterbett!"), "Tolles Polsterbett")
        self.assertEqual(sanitize_title("- Super Couch -"), "Couch -")

        # 2. Комбинированные материалы (Leder / Textil) - ни один материал не должен удаляться
        combo = sanitize_title("Leder Textil Ecksofa", material="Leder / Textil")
        self.assertEqual(combo, "Leder Textil Ecksofa")

        combo2 = sanitize_title("Kunstleder Stoff Couch", material="Kunstleder / Stoff")
        self.assertEqual(combo2, "Kunstleder Stoff Couch")

    def test_sanitize_title_removes_dangling_letters_and_normalizes_shapes(self):
        # 1. Удаление висячих букв в начале строки (r, s, e), включая кавычки, скобки и дефисы
        self.assertEqual(
            sanitize_title("r Weißer Polstersessel Wohnzimmer Einsitzer Textilsessel"),
            "Weißer Polstersessel Wohnzimmer Einsitzer Textilsessel"
        )
        self.assertEqual(
            sanitize_title('"r Weißer Polstersessel"'),
            "Weißer Polstersessel"
        )
        self.assertEqual(
            sanitize_title("(r) Weißer Polstersessel"),
            "Weißer Polstersessel"
        )
        self.assertEqual(
            sanitize_title("r. Weißer Polstersessel"),
            "Weißer Polstersessel"
        )
        self.assertEqual(
            sanitize_title("r - Weißer Polstersessel"),
            "Weißer Polstersessel"
        )
        self.assertEqual(
            sanitize_title("s Wohnzimmer Sofa 3 Sitzer"),
            "Wohnzimmer Sofa 3 Sitzer"
        )
        self.assertEqual(
            sanitize_title("e Designer Sofa Sofas"),
            "Designer Sofa Sofas"
        )

        # 2. Удаление висячих букв-паразитов внутри и в конце строки (включая скобки и кавычки)
        self.assertEqual(
            sanitize_title("Weißer Polstersessel Wohnzimmer Sitzmöbel Einfarbiger r Sessel"),
            "Weißer Polstersessel Wohnzimmer Sitzmöbel Einfarbiger Sessel"
        )
        self.assertEqual(
            sanitize_title('Weißer Polstersessel Wohnzimmer Sitzmöbel Einfarbiger (r) Sessel'),
            "Weißer Polstersessel Wohnzimmer Sitzmöbel Einfarbiger Sessel"
        )
        self.assertEqual(
            sanitize_title('Weißer Polstersessel Wohnzimmer Sitzmöbel Einfarbiger "r" Sessel'),
            "Weißer Polstersessel Wohnzimmer Sitzmöbel Einfarbiger Sessel"
        )
        self.assertEqual(
            sanitize_title("Dreisitzer r Sessel"),
            "Dreisitzer Sessel"
        )
        self.assertEqual(
            sanitize_title("Sofa r"),
            "Sofa"
        )

        # 3. Нормализация U-Form / L-Form (включая U - Form, u form, l form)
        self.assertEqual(
            sanitize_title("U Form Sofa Couch"),
            "U-Form Sofa Couch"
        )
        self.assertEqual(
            sanitize_title("U - Form Sofa"),
            "U-Form Sofa"
        )
        self.assertEqual(
            sanitize_title("u form sofa couch"),
            "U-Form sofa couch"
        )
        self.assertEqual(
            sanitize_title("L Form Ecksofa"),
            "L-Form Ecksofa"
        )
        self.assertEqual(
            sanitize_title("l form ecksofa"),
            "L-Form ecksofa"
        )

        # 4. Сохранение размеров, множителей с 'x' и сложных дефисных слов (E-Bike, T-Shirt)
        self.assertEqual(
            sanitize_title("4 x Stuhl"),
            "4 x Stuhl"
        )
        self.assertEqual(
            sanitize_title("200 x 90 cm"),
            "200 x 90 cm"
        )
        self.assertEqual(
            sanitize_title("2er Sofa"),
            "2er Sofa"
        )
        self.assertEqual(
            sanitize_title("E-Bike Garage"),
            "E-Bike Garage"
        )
        self.assertEqual(
            sanitize_title("T-Shirt Schrank"),
            "T-Shirt Schrank"
        )
        self.assertEqual(
            sanitize_title("Verona V-Abschlussseitenwand"),
            "Verona V-Abschlussseitenwand"
        )

    def test_is_blacklisted_category_seifenspender_exception(self):
        # Дозаторы мыла и мыльницы (аксессуары для ванной) не должны блокироваться
        self.assertFalse(is_blacklisted_category("Seifenspender & Seifenschalen"))
        self.assertFalse(is_blacklisted_category("Badaccessoires Seifenspender"))

        # Но чистое мыло (косметика/дрогери) должно блокироваться
        self.assertTrue(is_blacklisted_category("Seifen"))
        self.assertTrue(is_blacklisted_category("Körperpflege -> Seifen"))

    def test_get_default_shipping_group_and_caching(self):
        import asyncio
        from unittest.mock import AsyncMock, MagicMock
        from scripts.reupload_aftercool_catalog import get_default_shipping_group, GlobalRateLimiter

        rac_jv = MagicMock()
        rac_jv.controller = "jv"
        rac_jv.send_request = AsyncMock(
            return_value={
                "data": [
                    {"id_shipping_group": 101, "name": "Standard", "is_default": False},
                    {"id_shipping_group": 202, "name": "Default NL JV", "is_default": True},
                ]
            }
        )

        rac_xl = MagicMock()
        rac_xl.controller = "xl"
        rac_xl.send_request = AsyncMock(
            return_value={
                "data": [
                    {"id_shipping_group": 999, "name": "Default NL XL", "is_default": True},
                ]
            }
        )

        rate_limiter = GlobalRateLimiter(rate=50.0)
        cache = {}

        # 1. JV fetch -> id 202
        sg_id_jv = asyncio.run(get_default_shipping_group(rac_jv, rate_limiter, "nl", cache, account="jv"))
        self.assertEqual(sg_id_jv, 202)
        self.assertEqual(cache.get(("jv", "nl")), 202)

        # 2. XL fetch -> id 999 (изолирован от JV)
        sg_id_xl = asyncio.run(get_default_shipping_group(rac_xl, rate_limiter, "nl", cache, account="xl"))
        self.assertEqual(sg_id_xl, 999)
        self.assertEqual(cache.get(("xl", "nl")), 999)

        # 3. При ошибке сети None не кешируется
        rac_err = MagicMock()
        rac_err.send_request = AsyncMock(side_effect=Exception("Network timeout"))
        sg_err = asyncio.run(get_default_shipping_group(rac_err, rate_limiter, "de", cache, account="err"))
        self.assertIsNone(sg_err)
        self.assertNotIn(("err", "de"), cache)

    def test_is_blacklisted_category_expanded(self):
        # 1. Бытовая химия (CLP)
        self.assertTrue(is_blacklisted_category("Entkalker"))
        self.assertTrue(is_blacklisted_category("Küche & Haushalt -> Entkalker Tabs"))
        self.assertTrue(is_blacklisted_category("WC-Reiniger & WC-Steine"))
        self.assertTrue(is_blacklisted_category("Reinigungsmittel"))
        self.assertTrue(is_blacklisted_category("Desinfektionsmittel"))

        # 2. Продукты питания и напитки (LMIV)
        self.assertTrue(is_blacklisted_category("Pralinen & Trüffel"))
        self.assertTrue(is_blacklisted_category("Roséwein"))
        self.assertTrue(is_blacklisted_category("Süßwaren & Lutscher"))
        self.assertTrue(is_blacklisted_category("Babynahrung"))
        self.assertTrue(is_blacklisted_category("Lebensmittel"))

        # 3. Косметика и уход
        self.assertTrue(is_blacklisted_category("Feuchtigkeitscremes & Gesichtscremes"))
        self.assertTrue(is_blacklisted_category("Duschgel & Seifen"))
        self.assertTrue(is_blacklisted_category("Hautpflege"))

        # 4. Бытовая техника
        self.assertTrue(is_blacklisted_category("Kühlschränke & Gefriergeräte"))
        self.assertTrue(is_blacklisted_category("Geschirrspüler"))
        self.assertTrue(is_blacklisted_category("Waschmaschinen"))

        # 5. Мусорные категории и курьезы
        self.assertTrue(is_blacklisted_category("Sonstiges"))
        self.assertTrue(is_blacklisted_category("other"))
        self.assertTrue(is_blacklisted_category("Weihnachtsmusik"))
        self.assertTrue(is_blacklisted_category("Gießkannen"))

        # 6. Круглые кровати (Round beds) только если в названии нет 'rund'
        self.assertTrue(is_blacklisted_category("Round beds", title="Luxus Designer Doppelbett 180x200"))
        self.assertFalse(is_blacklisted_category("Round beds", title="Luxus Designer Rundbett 200x200"))
        self.assertFalse(is_blacklisted_category("Rundbetten", title="Rundes Lederbett modern"))

        # 7. Легитимные мебельные категории НЕ должны блокироваться
        self.assertFalse(is_blacklisted_category("Klassische Sofas", title="Sofa 3-Sitzer"))
        self.assertFalse(is_blacklisted_category("Couchgarnituren", title="Ecksofa"))
        self.assertFalse(is_blacklisted_category("Polsterbetten", title="Doppelbett"))
        self.assertFalse(is_blacklisted_category("Esszimmerstühle", title="Stuhl Set"))
        self.assertFalse(is_blacklisted_category("Schubladenschränke", title="Kommode weiß"))

    def test_resolve_fallback_category_heuristics(self):
        # 1. Диваны и софы
        cat_id, cat_name = resolve_fallback_category("Modernes Ecksofa L-Form Leder")
        self.assertEqual(cat_id, 5961)
        self.assertEqual(cat_name, "couchgarnituren")

        cat_id, cat_name = resolve_fallback_category("Klassisches Sofa 3-Sitzer Samt")
        self.assertEqual(cat_id, 45081)
        self.assertEqual(cat_name, "klassische-sofas")

        # 2. Кровати
        cat_id, cat_name = resolve_fallback_category("Boxspringbett 180x200 mit Matratze")
        self.assertEqual(cat_id, 2751)
        self.assertEqual(cat_name, "polsterbetten")

        # 3. Кресла
        cat_id, cat_name = resolve_fallback_category("Gemütlicher Relaxsessel Leder braun")
        self.assertEqual(cat_id, 5831)
        self.assertEqual(cat_name, "cocktailsessel")

        # 4. Стулья
        cat_id, cat_name = resolve_fallback_category("Esszimmerstuhl 4er Set Holz")
        self.assertEqual(cat_id, 4211)
        self.assertEqual(cat_name, "esszimmerstuehle")

        # 5. Столы
        cat_id, cat_name = resolve_fallback_category("Couchtisch Glasplatte Beistelltisch")
        self.assertEqual(cat_id, 3541)
        self.assertEqual(cat_name, "beistelltische")

        # 6. Комоды
        cat_id, cat_name = resolve_fallback_category("Kommode Sideboard mit Schubladen")
        self.assertEqual(cat_id, 6141)
        self.assertEqual(cat_name, "schubladenschraenke")

        # 7. Шкафы
        cat_id, cat_name = resolve_fallback_category("Kleiderschrank 4-türig Schlafzimmer")
        self.assertEqual(cat_id, 3271)
        self.assertEqual(cat_name, "schlafzimmer")

        # 8. Неизвестный общий товар -> (46001, 'Möbel')
        cat_id, cat_name = resolve_fallback_category("Spezialartikel ohne Schlagwort")
        self.assertEqual(cat_id, 46001)
        self.assertEqual(cat_name, "Möbel")

    def test_generate_localized_description_all_locales(self):
        locales = ["de-DE", "fr-FR", "cs-CZ", "sk-SK", "pl-PL", "it-IT", "nl-NL"]
        title = "Luxus Designer Sofa 3-Sitzer"

        for loc in locales:
            desc = generate_localized_description(title=title, locale=loc, material="Echtleder")
            self.assertTrue(desc.startswith("<p>"), f"Locale {loc} description must start with <p>")
            self.assertTrue(desc.endswith("</p>"), f"Locale {loc} description must end with </p>")
            # Длина описания должна быть не менее 150 символов
            self.assertGreaterEqual(len(desc), 150, f"Locale {loc} description too short ({len(desc)} chars)")
            self.assertIn(title, desc, f"Locale {loc} must contain title")
            self.assertIn("Echtleder", desc, f"Locale {loc} must contain specified material")
            # Санитарная чистота (без управляющих символов и запрещенных тегов)
            self.assertNotIn("<script", desc)
            self.assertNotIn("onclick", desc)
            self.assertNotIn("!", desc)

    def test_consolidate_description_sections_split_dimensions(self):
        raw_html = (
            "<div><strong>Maße:</strong><br/><br/><strong>Hochbett</strong><br/>"
            "ca: 324 x 86 x 180 cm<br/>Höhe: 180 cm<br/>Liegefläche: ca: 80 x 200 cm<br/><br/>"
            "<strong>Farbe:</strong> Gelb<br/><br/>"
            "<strong>Produktbeschreibung:</strong><br/>Hochbett mit integriertem Schreibtisch.<br/><br/>"
            "<strong>Produktmerkmale:</strong><br/>• Hochbett mit Liegefläche: 80 x 200 cm<br/><br/>"
            "<strong>Maße:</strong><br/><br/>"
            "<strong>Schreibtisch:</strong><br/>Länge - 140 cm<br/>Breite - 50 cm<br/><br/>"
            "<strong>Kleiderschrank:</strong><br/>Länge - 60 cm<br/>Breite - 50 cm</div>"
        )
        cleaned = sanitize_description(raw_html)

        # 1. Должен быть только ОДИН блок Maße
        self.assertEqual(cleaned.count("<strong>Maße:</strong>"), 1)

        # 2. Порядок секций строго по требованию: Maße -> Farbe -> Produktmerkmale -> Produktbeschreibung
        idx_mase = cleaned.find("<strong>Maße:</strong>")
        idx_farbe = cleaned.find("<strong>Farbe:</strong>")
        idx_features = cleaned.find("<strong>Produktmerkmale:</strong>")
        idx_desc = cleaned.find("<strong>Produktbeschreibung:</strong>")

        self.assertGreater(idx_mase, -1)
        self.assertGreater(idx_farbe, -1)
        self.assertGreater(idx_features, -1)
        self.assertGreater(idx_desc, -1)

        self.assertLess(idx_mase, idx_farbe)
        self.assertLess(idx_farbe, idx_features)
        self.assertLess(idx_features, idx_desc)

        # 3. Маркеры в Produktmerkmale должны быть '-'
        self.assertIn("- Hochbett mit Liegefläche", cleaned)

        # 4. Все составные части (Hochbett, Schreibtisch, Kleiderschrank) должны быть внутри Maße после заголовка Maße:
        idx_hochbett = cleaned.find("<strong>Hochbett:</strong>")
        idx_schreibtisch = cleaned.find("<strong>Schreibtisch:</strong>")
        idx_kleiderschrank = cleaned.find("<strong>Kleiderschrank:</strong>")
        self.assertGreater(idx_hochbett, idx_mase)
        self.assertGreater(idx_schreibtisch, idx_mase)
        self.assertGreater(idx_kleiderschrank, idx_mase)
        self.assertLess(idx_hochbett, idx_farbe)
        self.assertLess(idx_schreibtisch, idx_farbe)
        self.assertLess(idx_kleiderschrank, idx_farbe)





