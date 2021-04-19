from django.shortcuts import redirect
from django.urls import path, include
from drf_spectacular.views import SpectacularAPIView, SpectacularRedocView, SpectacularSwaggerView
from .views import rainways_area_of_interest_analysis, ApiDefaultRouter

router = ApiDefaultRouter()

urlpatterns = [

    # --------------------------
    # documentation
    
    path('schema/', SpectacularAPIView.as_view(), name='schema'),
    path('docs/swagger-ui/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('docs/redoc/', SpectacularRedocView.as_view(url_name='schema'), name='redoc'),

    # --------------------------
    # DRF-registered routes
    path('', include(router.urls)),

    # --------------------------
    # custom routes (for function-based views)
    # path('acsa/', redirect('acsa/aoi-analysis/')),
    path('acsa/aoi-analysis/', rainways_area_of_interest_analysis),
]