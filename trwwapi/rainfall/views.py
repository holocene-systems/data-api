# from django.contrib.auth.models import User, Group

from datetime import datetime, timedelta
from django.utils.safestring import mark_safe
from django.utils.timezone import localtime
from django.db.models import Q
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.shortcuts import redirect
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework import viewsets, permissions, routers
from rest_framework.decorators import api_view
from rest_framework.response import Response
from marshmallow import ValidationError
from time import sleep
from django_rq import job, get_queue

from ..utils import _parse_request
from .serializers import (
    GarrObservationSerializer, 
    GaugeObservationSerializer, 
    RtrrObservationSerializer, 
    RtrgObservationSerializer,
    ReportEventSerializer,
    RequestSchema,
    ResponseSchema,
    parse_and_validate_args
)
from .selectors import handle_request_for
from .models import (
    GarrObservation, 
    GaugeObservation, 
    RtrrObservation, 
    RtrgObservation,
    ReportEvent, 
    Pixel, 
    Gauge, 
    MODELNAME_TO_GEOMODEL_LOOKUP
)
from .api_civicmapper.config import (
    DELIMITER, 
    INTERVAL_15MIN, 
    INTERVAL_DAILY, 
    INTERVAL_HOURLY, 
    INTERVAL_MONTHLY,
    INTERVAL_SUM,
    TZ,
    F_CSV
)
from .api_civicmapper.core import (
    parse_sensor_ids, 
    parse_sensor_ids, 
    parse_datetime_args, 
    query_pgdb,
    aggregate_results_by_interval,
    apply_zerofill,
    format_results
)


# -------------------------------------------------------------------
# API ROOT VIEW

class ApiRouterRootView(routers.APIRootView):
    """
    Controls appearance of the API root view
    """

    def get_view_name(self):
        return "3RWW Rainfall Data API"

    def get_view_description(self, html=False):
        text = "Get 3RWW high-resolution rainfall data"
        if html:
            return mark_safe(f"<p>{text}</p>")
        else:
            return text

class ApiDefaultRouter(routers.DefaultRouter):
    APIRootView = ApiRouterRootView

# -------------------------------------------------------------------
# HIGH-LEVEL API VIEWS
# these are the views that do the work for us


class RainfallGaugeApiView(APIView):

    def get(self, request, *args, **kwargs):
        return handle_request_for(GaugeObservation, request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return handle_request_for(GaugeObservation, request, *args, **kwargs)


class RainfallGarrApiView(APIView):

    def get(self, request, *args, **kwargs):
        return handle_request_for(GarrObservation, request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return handle_request_for(GarrObservation, request, *args, **kwargs)


class RainfallRtrrApiView(APIView):

    def get(self, request, *args, **kwargs):
        return handle_request_for(RtrrObservation, request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return handle_request_for(RtrrObservation, request, *args, **kwargs)


class RainfallRtrgApiView(APIView):

    def get(self, request, *args, **kwargs):
        return handle_request_for(RtrgObservation, request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return handle_request_for(RtrgObservation, request, *args, **kwargs)


# -------------------------------------------------------------------
# LOW LEVEL API VIEWS
# These return paginated data from the tables in the database as-is.
# They show up in the django-rest-framework's explorable API pages.

class ReportEventsViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = ReportEvent.objects.all()
    serializer_class = ReportEventSerializer
    permission_classes = [permissions.IsAuthenticatedOrReadOnly]
    lookup_field = 'event_label'


class GarrObservationViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = GarrObservation.objects.all()
    serializer_class  = GarrObservationSerializer
    permission_classes = [permissions.IsAuthenticatedOrReadOnly]
    lookup_field = 'timestamp'


class GaugeObservationViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = GaugeObservation.objects.all()
    serializer_class  = GaugeObservationSerializer
    permission_classes = [permissions.IsAuthenticatedOrReadOnly]
    lookup_field='timestamp'


class RtrrObservationViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = RtrrObservation.objects.all()
    serializer_class  = RtrrObservationSerializer
    permission_classes = [permissions.IsAuthenticatedOrReadOnly]
    lookup_field='timestamp'

class RtrgbservationViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = RtrgObservation.objects.all()
    serializer_class  = RtrgObservationSerializer
    permission_classes = [permissions.IsAuthenticatedOrReadOnly]
    lookup_field='timestamp'