# from django.contrib.auth.models import User, Group

from datetime import datetime, timedelta
from django.utils.safestring import mark_safe
from django.utils.timezone import localtime
from django.db.models import Q
from django.core.exceptions import ObjectDoesNotExist
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework import viewsets, permissions, routers
from rest_framework.decorators import api_view
from rest_framework.response import Response
from marshmallow import ValidationError

from .serializers import (
    GarrObservationSerializer, 
    GaugeObservationSerializer, 
    RtrrObservationSerializer, 
    ReportEventSerializer,
    RequestSchema,
    ResponseSchema,
    parse_and_validate_args
)
from .models import (
    AWSAPIGWMock, 
    GarrObservation, 
    GaugeObservation, 
    RtrrObservation, 
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
# HELPERS

def _handler(postgres_table_model, request):
    """Generic function for handling GET or POST requests of either the GARR, Gauge, or RTRR tables, 
    used for the high-level ReST API endpoints.

    Modeled off of the handler.py script from the original serverless version of this codebase.
    """
    messages = []

    # **parse** the arguments from the query string or body, depending on request method
    if request.method == 'GET':
        raw_args = request.query_params.dict()
    else:
        raw_args = request.data

    # handle missing arguments here:
    # Rollup = Sum
    # Format = Time-oriented
    # start and end datetimes: will attemp to look for the last rainfall event, 
    # otherwise will fallback to looking for 4 hours of the latest available data.

    if not raw_args:
        raw_args = {}
    
    if all(['start_dt' not in raw_args.keys(),'end_dt' not in raw_args.keys()]):
        latest_report = ReportEvent.objects.first()
        # if there are events available, fall back to one of those
        if latest_report:
            # We explicitly convert to localtime here because Django assumes datetimes
            # in the DB are stored as UTC (even with timezone offset stored there)
            raw_args['start_dt'] = localtime(latest_report.start_dt, TZ).isoformat()
            raw_args['end_dt'] = localtime(latest_report.end_dt, TZ).isoformat()
            messages.append("Using the latest available rainfall event data by a default.")
        # otherwise get a a sna of latest available data
        else:
            try:
                last_data_point = postgres_table_model.objects.latest('timestamp')
                # print(last_data_point)
                latest = raw_args['end_dt'] = localtime(last_data_point.timestamp, TZ)
                before = latest - timedelta(hours=4)
                raw_args['start_dt'] = before.isoformat()
                raw_args['end_dt'] = latest.isoformat()
                messages.append("Using the latest available rainfall data by a default.")
            except ObjectDoesNotExist:
                messages.append("Unable to retrieve data: no arguments provided; unable to default to latest data.")
                response = ResponseSchema(
                    status_code=400,
                    message=messages
                )
                return Response(data=response.as_dict(), status=status.HTTP_400_BAD_REQUEST)                

        # raw_args['rollup'] = INTERVAL_SUM
        # raw_args['f'] = 'time' #sensor

                # response = ResponseSchema(
                #     status_code=400,
                #     message="No arguments provided in the request. See documentation for example requests.",
                # )
                # return Response(data=response.as_dict(), status=status.HTTP_400_BAD_REQUEST)

    # print(raw_args)

    # -------------------------------------------------------------------
    # validate the request arguments

    # **validate** the arguments using a marshmallow model
    # this will convert datetimes to the proper format, check formatting, etc.
    try:
        # print("parse_and_validate_args")
        args = parse_and_validate_args(raw_args)
        # print(args)
    # return errors from validation
    except ValidationError as e:
        messages.append("{1}. See documentation for example requests.".format(e.messages))
        # print(e.messages)
        response = ResponseSchema(
            status_code=400,
            message=messages
        )
        return Response(data=response.as_dict(), status=status.HTTP_400_BAD_REQUEST)
    except KeyError as e:
        messages.append("Invalid request arguments ({0}). See documentation for example requests".format(e))
        response = ResponseSchema(
            status_code=400,
            message=messages
        )
        return Response(data=response.as_dict(), status=status.HTTP_400_BAD_REQUEST)

    # -------------------------------------------------------------------
    # build a query from request args and submit it 

    # here we figure out all possible datetimes and sensor ids. The combination of these
    # make up the primary keys in the database
    
    # parse the datetime parameters into a complete list of all possible date times
    # print("parse_datetime_args")
    dts = parse_datetime_args(args['start_dt'], args['end_dt'], args['rollup'])

    # TODO: check for  datetime + rollup parameters here
    if any([
        # 15-minute: < 1 week 
        args['rollup'] == INTERVAL_15MIN and len(dts) > (24 * 4 * 7),
        # hourly < 1 month
        args['rollup'] == INTERVAL_HOURLY and len(dts) > (4 * 24 * 31),
        # daily: < 3 months
        args['rollup'] == INTERVAL_DAILY and len(dts) > (4 * 24 * 90),
        # monthly: < 1 year
        args['rollup'] == INTERVAL_MONTHLY and len(dts) > (4 * 24 * 366),
        # sum: < 1 year
        args['rollup'] == INTERVAL_SUM and len(dts) > (4 * 24 * 366)
    ]):
        messages.append("The submitted request would generate a larger response than we can manage for you right now. Use one of the following combinations of rollup and datetime ranges: 15-minute: < 1 week; hourly < 1 month; daily: < 3 months; monthly: < 1 year; sum: < 1 year. Please either reduce the date/time range queried or increase the time interval for roll-up parameter.")
        response = ResponseSchema(
            status_code=400,
            response_data=dict((k, args[k]) for k in ['rollup', 'start_dt', 'end_dt'] if k in args),
            message=messages
        )
        return Response(data=response.as_dict(), status=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE)

    
    # parse sensor ID string to a list. if not provided, the subsequent query will return all sensors.
    sensor_ids = []
    if args['sensor_ids']:
        sensor_ids = [str(i) for i in id_string.split(DELIMITER)]

    # use parsed args and datetime list to query the database
    try:
        # print("query_pgdb")
        results = query_pgdb(postgres_table_model, sensor_ids, dts)
    except Exception as e:
        messages.append("Could not retrieve records from the database. Error(s): {0}".format(str(e)))
        response = ResponseSchema(
            status_code=500,
            message=messages
        )
        return Response(data=response.as_dict(), status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # -------------------------------------------------------------------
    # post process the query results, if any

    if len(results) > 0:
        
        # perform selects and/or aggregations based on zerofill and interval args
        # print("aggregate_results_by_interval")
        aggregated_results = aggregate_results_by_interval(results, args['rollup'])
        #print("aggregated results\n", etl.fromdicts(aggregated_results))
        # print("apply_zerofill")
        zerofilled_results = apply_zerofill(aggregated_results, args['zerofill'], dts)
        # transform the data to the desired format, if any
        # (by default it just returns the DynamoDB response: a list of dicts)
        # print("format_results")
        response_data = format_results(zerofilled_results, args['f']) #, ref_geojson)

        # return the result

        # if the request was for a csv in the legacy teragon format, then we only return that
        if args['f'] in F_CSV:
            print('returning legacy CSV format')
            return Response(response_data, status=status.HTTP_200_OK, content_type="text/csv")
        else:
            response = ResponseSchema(
                status_code=200,
                request_args=args,
                response_data=response_data,
                message=messages
            )
            return Response(response.as_dict(), status=status.HTTP_200_OK)

    else:
        # return the result
        messages.append("No records returned.")
        response = ResponseSchema(
            status_code=200,
            request_args=args,
            message=messages
        )
        return Response(response.as_dict(), status=status.HTTP_204_NO_CONTENT)

    

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
# these are the ones that do the work for us

class RainfallGarrApiView(APIView):

    def get(self, request, *args, **kw):
        return _handler(GarrObservation, request)

    # def post(self, request, *args, **kw):
    #     return _handler(GarrObservation, request)


class RainfallGaugeApiView(APIView):

    def get(self, request, *args, **kw):
        return _handler(GaugeObservation, request)


class RainfallRtrrApiView(APIView):

    def get(self, request, *args, **kw):
        return _handler(RtrrObservation, request)





# -------------------------------------------------------------------
# LOW LEVEL API VIEWS
# these return paginated data from the tables in the database as-is.

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

    # t = GarrObservation.objects\
    #     .filter(timestamp__gt="2020-03-30")\
    #     .extra(
    #         select={'sensors': """SELECT jsonb_object_agg(key, value) FROM jsonb_each(data) WHERE key IN (%s)"""}
    #         select_params=(someparam)
    #     )
    # t = GarrObservation.objects.filter(timestamp__gt="2020-03-30").extra(select={'val': """SELECT jsonb_object_agg(key, value) FROM jsonb_each(data) WHERE key IN (%s)"""}, select_params=", ".join(['123134', '159138']))


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