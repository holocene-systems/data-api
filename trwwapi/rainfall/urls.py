from django.urls import path, include
from rest_framework.urlpatterns import format_suffix_patterns

from .views import ApiDefaultRouter
# high-level vews
from .views import RainfallGarrApiView, RainfallGaugeApiView, RainfallRtrrApiView
# low-level views
from .views import GarrObservationViewSet, GaugeObservationViewSet, RtrrObservationViewSet

# -----------------------------------------------
# router for low-level API routes
router = ApiDefaultRouter()

router.register(r'garr-table', GarrObservationViewSet)
router.register(r'gauge-table', GaugeObservationViewSet)
router.register(r'rtrr-table', RtrrObservationViewSet)

# -----------------------------------------------
# API URLs for the Views

urlpatterns = [
    # high-level routes
    path('v2/garr/', RainfallGarrApiView.as_view()),
    path('v2/gauge/', RainfallGarrApiView.as_view()),
    path('v2/rtrr/', RainfallGarrApiView.as_view()),
    # low-level routes
    path('', include(router.urls))
]