import re
from decimal import Decimal

from rest_framework import serializers

from config import storefronts, get_account_storefronts

"""
Поля для product-data:
    ean
    description
    picture
    title

    product_safety_contact:
        name
        email_address
        address
        phone_number
        url
"""

from main_api.src.utils.ean_utils import is_valid_ean, normalize_ean


class ProductDataSerializer(serializers.Serializer):
    ean = serializers.CharField(required=True, allow_blank=False, trim_whitespace=True)
    title = serializers.CharField(required=False, allow_blank=True, trim_whitespace=True)
    description = serializers.CharField(required=False, allow_blank=True, trim_whitespace=True)
    picture = serializers.ListField(required=False, allow_empty=True, child=serializers.CharField())
    picture_urls = serializers.ListField(required=False, allow_empty=True, child=serializers.URLField())
    unit_id = serializers.CharField(required=False, allow_blank=True, trim_whitespace=True)
    storefront = serializers.CharField(required=False, default="de", allow_blank=True, trim_whitespace=True)
    price = serializers.DecimalField(required=False, max_digits=10, decimal_places=2)
    controller = serializers.ChoiceField(choices=["jv", "xl"], required=True)
    
    
    def to_internal_value(self, data):
        allowed = {field: data[field] for field in self.fields if field in data}
        return super().to_internal_value(allowed)
    
    
class DeleteDataSerializer(serializers.Serializer):
    controller = serializers.ChoiceField(choices=["jv", "xl"], required=True)
    

class PutDataSerializer(serializers.Serializer):
    ean = serializers.CharField(required=True, allow_blank=False, trim_whitespace=True)
    controller = serializers.ChoiceField(choices=["jv", "xl"], required=True)
    title = serializers.CharField(required=True, allow_blank=False, trim_whitespace=True)
    description = serializers.CharField(required=True, allow_blank=False, trim_whitespace=True)
    picture = serializers.ListField(
        required=False,
        allow_empty=False,
        child=serializers.URLField(),
    )
    picture_urls = serializers.ListField(
        required=False,
        allow_empty=False,
        child=serializers.URLField(),
    )

    price = serializers.DecimalField(
        required=True,
        max_digits=10,
        decimal_places=2,
        min_value=Decimal("0.01"),
    )
    size = serializers.CharField(required=True, allow_blank=False, trim_whitespace=True)
    color = serializers.CharField(required=True, allow_blank=False, trim_whitespace=True)
    material = serializers.CharField(required=True, allow_blank=False, trim_whitespace=True)
    material_composition = serializers.CharField(
        required=False,
        allow_blank=True,
        trim_whitespace=True,
        default="",
    )
    delivery = serializers.IntegerField(min_value=0)
    height = serializers.DecimalField(max_digits=8, decimal_places=2, min_value=Decimal("0"))
    length = serializers.DecimalField(max_digits=8, decimal_places=2, min_value=Decimal("0"))
    width = serializers.DecimalField(max_digits=8, decimal_places=2, min_value=Decimal("0"))
    amount = serializers.IntegerField(required=False, min_value=0, default=20)
    id_offer = serializers.CharField(required=False, allow_blank=True, trim_whitespace=True)
    storefronts = serializers.ListField(
        required=False,
        allow_empty=False,
        child=serializers.ChoiceField(choices=storefronts),
    )

    def validate_ean(self, value):
        val = str(value).strip()
        if not is_valid_ean(val):
            raise serializers.ValidationError(
                f"Invalid EAN barcode '{val}'. Must be a valid barcode with correct check digit (e.g. 13-digit EAN)."
            )
        return val

    def validate_material(self, value):
        val = str(value).strip()
        if re.search(r"[а-яА-ЯёЁ]", val):
            raise serializers.ValidationError(
                f"Material must be specified in German. Received Cyrillic value: '{val}'. "
                f"Allowed examples: 'Holz', 'Sperrholz', 'Textil', 'Leder', 'Metall', 'Kunststoff'."
            )
        return val

    def validate(self, attrs):
        picture = attrs.get("picture") or attrs.get("picture_urls")
        if not picture:
            raise serializers.ValidationError(
                {"picture": "Provide at least one image URL in picture or picture_urls."}
            )
        attrs["picture"] = picture
        attrs.pop("picture_urls", None)
        attrs["storefronts"] = attrs.get("storefronts") or get_account_storefronts(
            attrs.get("controller")
        )
        if not attrs.get("id_offer"):
            attrs["id_offer"] = attrs["ean"]

        mat_comp = (attrs.get("material_composition") or "").strip()
        if mat_comp.lower() in ["no information required", "no information", "none", "null"]:
            raise serializers.ValidationError(
                {
                    "material_composition": (
                        "Invalid value. Kaufland rejects placeholder values like 'No information required'. "
                        "Provide a valid fiber composition (e.g. '100% Polyester', '100% Leder') or omit the field."
                    )
                }
            )
        attrs["material_composition"] = mat_comp
        return attrs


class ActivateProductSerializer(serializers.Serializer):
    controller = serializers.ChoiceField(choices=["jv", "xl"], required=True)
