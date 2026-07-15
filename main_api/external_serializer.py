from rest_framework import serializers

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
    title = serializers.CharField(required=False, allow_blank=True, trim_whitespace=True)
    description = serializers.CharField(required=True, allow_blank=True, trim_whitespace=True)
    picture = serializers.ListField(required=True, allow_empty=True, child=serializers.CharField())

    price = serializers.CharField(required=True, trim_whitespace=True)
    size = serializers.CharField(required=True, allow_blank=False, trim_whitespace=True)
    color = serializers.CharField(required=True, allow_blank=False, trim_whitespace=True)
    material = serializers.CharField(required=True, allow_blank=False, trim_whitespace=True)
    delivery = serializers.IntegerField()
    height = serializers.IntegerField()
    length = serializers.IntegerField()
    width = serializers.IntegerField()  


class ActivateProductSerializer(serializers.Serializer):
    controller = serializers.ChoiceField(choices=["jv", "xl"], required=True)