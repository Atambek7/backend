from django.urls import path
from rest_framework_simplejwt.views import TokenRefreshView

from main_api.views import (
    MainOperationsView,
    UploadCollectionsViaJsonView,
    UploadCollectionsViaJsonJwtView,
    ProtectedView,
    HealthCheckView,
    ProductByEanView,
)
from main_api.external_views import (
    AftercoolLoginView,
    RetreiveProductView, 
    PatchProductView, 
    DeleteProductView,
    PutProductView,
    DeactivateProductView,
    ActivateProductView
)
from main_api.auth.views import CustomTokenObtainSlidingView

urlpatterns = [
    path("health/", HealthCheckView.as_view(), name="health"),
    path("products/ean/change/", PatchProductView.as_view(), name="patch_product"),
    path("products/ean/<str:ean>/", ProductByEanView.as_view(), name="product_by_ean"),
    path("kaufland_main/", MainOperationsView.as_view(), name="main_operations"),
    path(
        "kaufland_main/upload_json/",
        UploadCollectionsViaJsonView.as_view(),
        name="upload_collections",
    ),
    path(
        "kaufland_main/upload_json/jwt/",
        UploadCollectionsViaJsonJwtView.as_view(),
        name="upload_collections_jwt",
    ),
    path("protected/", ProtectedView.as_view(), name="protected"),
    path("token/", CustomTokenObtainSlidingView.as_view(), name="token_obtain_pair"),
    path("token/refresh/", TokenRefreshView.as_view(), name="token_refresh"),
    path("aftercool_login/", AftercoolLoginView.as_view(), name="aftercool_login"),
    
    # External API
    path(
        "products/product/ean/", RetreiveProductView.as_view(), name="retrieve_product"
    ),
    path("products/delete/<str:ean>", DeleteProductView.as_view(), name="delete_product"),
    path("products/upload/", PutProductView.as_view(), name="put_product"),
    # path("fabrics/", RetreiveProductView.as_view(), name="retrieve_fabric"),
    path("products/deactivate/<str:ean>", DeactivateProductView.as_view(), name="deactivate_product"),
    path("products/activate/<str:ean>", ActivateProductView.as_view(), name="activate_product"),
]
