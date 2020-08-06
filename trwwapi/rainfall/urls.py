from django.urls import path, include
from rest_framework.urlpatterns import format_suffix_patterns

from .views import ApiDefaultRouter
# high-level vews
from .views import RainfallGarrApiView, RainfallGaugeApiView, RainfallRtrrApiView, RainfallRtrgApiView
# low-level views
from .views import GarrObservationViewSet, GaugeObservationViewSet, RtrrObservationViewSet, RtrgbservationViewSet, ReportEventsViewSet

# -----------------------------------------------
# router for low-level API routes
router = ApiDefaultRouter()

router.register(r'calibrated-radar', GarrObservationViewSet)
router.register(r'calibrated-gauge', GaugeObservationViewSet)
router.register(r'realtime-radar', RtrrObservationViewSet)
router.register(r'realtime-gauge', RtrgbservationViewSet)
router.register(r'rainfall-events', ReportEventsViewSet)

# -----------------------------------------------
# API URLs for the Views

urlpatterns = [
    # high-level routes - multiple routes-per-view represent multiple naming 
    # conventions
    path('v2/pixel/historic/', RainfallGarrApiView.as_view()),
    path('v2/pixel/calibrated/', RainfallGarrApiView.as_view()),    
    path('v2/radar/historic/', RainfallGarrApiView.as_view()),
    path('v2/radar/calibrated/', RainfallGarrApiView.as_view()),
    path('v2/pixel/historic/<str:jobid>/', RainfallGarrApiView.as_view()),
    path('v2/pixel/calibrated/<str:jobid>/', RainfallGarrApiView.as_view()),    
    path('v2/radar/historic/<str:jobid>/', RainfallGarrApiView.as_view()),
    path('v2/radar/calibrated/<str:jobid>/', RainfallGarrApiView.as_view()),

    path('v2/pixel/realtime/', RainfallRtrrApiView.as_view()),
    path('v2/pixel/raw/', RainfallRtrrApiView.as_view()),
    path('v2/radar/realtime/',  RainfallRtrrApiView.as_view()),
    path('v2/radar/raw/',  RainfallRtrrApiView.as_view()), 
    path('v2/pixel/realtime/<str:jobid>/', RainfallRtrrApiView.as_view()),
    path('v2/pixel/raw/<str:jobid>/', RainfallRtrrApiView.as_view()),
    path('v2/radar/realtime/<str:jobid>/',  RainfallRtrrApiView.as_view()),
    path('v2/radar/raw/<str:jobid>/',  RainfallRtrrApiView.as_view()),

    path('v2/gauge/historic/', RainfallGaugeApiView.as_view()),
    path('v2/gauge/calibrated/', RainfallGaugeApiView.as_view()),
    path('v2/gauge/historic/<str:jobid>/', RainfallGaugeApiView.as_view()),
    path('v2/gauge/calibrated/<str:jobid>/', RainfallGaugeApiView.as_view()),

    path('v2/gauge/realtime/', RainfallRtrgApiView.as_view()),
    path('v2/gauge/raw/', RainfallRtrgApiView.as_view()),
    path('v2/gauge/realtime/<str:jobid>/', RainfallRtrgApiView.as_view()),
    path('v2/gauge/raw/<str:jobid>/', RainfallRtrgApiView.as_view()),
    
    # low-level routes
    path('', include(router.urls))
]