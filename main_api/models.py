from django.db import models
from django.contrib.auth.models import User

from django.utils import timezone
# Create your models here.


class AdminRequestLog(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    endpoint = models.CharField(max_length=255)
    method = models.CharField(max_length=10)
    timestamp = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.user.username} - {self.endpoint} [{self.method}]"


class ImageStorage(models.Model):
    ean = models.IntegerField()
    path = models.ImageField(upload_to="images/")
    
    
class Unit(models.Model):

    class ControllerChoices(models.TextChoices):
        JV = "jv", "JV"
        XL = "xl", "XL"
    
    
    ean = models.CharField(max_length=13)
    unit = models.JSONField(default=dict)
    storefront = models.CharField(max_length=10, blank=True)
    
    controller = models.CharField(max_length=50, choices=ControllerChoices.choices)
    created_at = models.DateTimeField(default=timezone.now)
    
    
    def __str__(self):
        return f"{self.ean} - {self.storefront} - {self.created_at}"