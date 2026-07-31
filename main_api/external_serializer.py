from decimal import Decimal

from rest_framework import serializers

from config import storefronts

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

class ProductDataSerializer(serializers.Serializer):
    ean = serializers.CharField(required=True, allow_blank=False, trim_whitespace=True)
    title = serializers.CharField(required=False, allow_blank=True, trim_whitespace=True)
    description = serializers.CharField(required=False, allow_blank=True, trim_whitespace=True)
    picture = serializers.ListField(required=False, allow_empty=True, child=serializers.CharField())
    picture_urls = serializers.ListField(required=False, allow_empty=True, child=serializers.URLField())
    unit_id = serializers.CharField(required=False, allow_blank=True, trim_whitespace=True)
    storefront = serializers.CharField(required=True, allow_blank=True, trim_whitespace=True)
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

    def validate(self, attrs):
        picture = attrs.get("picture") or attrs.get("picture_urls")
        if not picture:
            raise serializers.ValidationError(
                {"picture": "Provide at least one image URL in picture or picture_urls."}
            )
        attrs["picture"] = picture
        attrs.pop("picture_urls", None)
        attrs["storefronts"] = attrs.get("storefronts") or storefronts
        if not attrs.get("id_offer"):
            attrs["id_offer"] = attrs["ean"]
        return attrs


class ActivateProductSerializer(serializers.Serializer):
    controller = serializers.ChoiceField(choices=["jv", "xl"], required=True)
