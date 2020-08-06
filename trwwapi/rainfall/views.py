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
from .models import (
    AWSAPIGWMock, 
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
# HELPERS

class DebugMessages():

    def __init__(self, messages=[], debug=settings.DEBUG):
        self.messages = messages
        self.show = debug

    def add(self, msg):
        if self.show:
            print(msg)
        self.messages.append(msg)

def _parse_request(request):
    """parse the django request object
    """

    # **parse** the arguments from the query string or body, depending on request method
    if request.method == 'GET':
        raw_args = request.query_params.dict()
    else:
        raw_args = request.data

    return raw_args

@job
def _handler(postgres_table_model, raw_args=None):

    # print("request made to", postgres_table_model, raw_args)

    """Generic function for handling GET or POST requests of either the GARR, Gauge, or RTRR tables, 
    used for the high-level ReST API endpoints.

    Modeled off of the handler.py script from the original serverless version of this codebase.
    """
    messages = DebugMessages(debug=True)

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
            messages.add("Using the latest available rainfall event data by a default.")
        # otherwise get a a sna of latest available data
        else:
            try:
                last_data_point = postgres_table_model.objects.latest('timestamp')
                # print(last_data_point)
                latest = raw_args['end_dt'] = localtime(last_data_point.timestamp, TZ)
                before = latest - timedelta(hours=4)
                raw_args['start_dt'] = before.isoformat()
                raw_args['end_dt'] = latest.isoformat()
                messages.add("Using the latest available rainfall data by a default.")
            except ObjectDoesNotExist:
                messages.add("Unable to retrieve data: no arguments provided; unable to default to latest data.")
                response = ResponseSchema(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    messages=messages.messages
                )
                # return Response(data=response.as_dict(), status=status.HTTP_400_BAD_REQUEST)                
                # return response.as_dict()

        # raw_args['rollup'] = INTERVAL_SUM
        # raw_args['f'] = 'time' #sensor

                # response = ResponseSchema(
                #     status_code=400,
                #     messages="No arguments provided in the request. See documentation for example requests.",
                # )
                # return Response(data=response.as_dict(), status=status.HTTP_400_BAD_REQUEST)

    # print(raw_args)

    # -------------------------------------------------------------------
    # validate the request arguments

    # **validate** the arguments using a marshmallow model
    # this will convert datetimes to the proper format, check formatting, etc.
    try:
        print("parse_and_validate_args")
        args = parse_and_validate_args(raw_args)
        # print(args)
    # return errors from validation
    except ValidationError as e:
        messages.add("{1}. See documentation for example requests.".format(e.messages))
        # print(e.messages)
        response = ResponseSchema(
            status_code=status.HTTP_400_BAD_REQUEST,
            messages=messages.messages
        )
        # return Response(data=response.as_dict(), status=status.HTTP_400_BAD_REQUEST)
        # return response.as_dict()
    except KeyError as e:
        messages.add("Invalid request arguments ({0}). See documentation for example requests".format(e))
        response = ResponseSchema(
            status_code=status.HTTP_400_BAD_REQUEST,
            messages=messages.messages
        )
        # return Response(data=response.as_dict(), status=status.HTTP_400_BAD_REQUEST)
        #return response.as_dict()

    # -------------------------------------------------------------------
    # build a query from request args and submit it 

    # here we figure out all possible datetimes and sensor ids. The combination of these
    # make up the primary keys in the database
    
    # parse the datetime parameters into a complete list of all possible date times
    print("parse_datetime_args")
    dts = parse_datetime_args(args['start_dt'], args['end_dt'], args['rollup'])
    # print(dts)

    # TODO: check for  datetime + rollup parameters here
    # if any([
    #     # 15-minute: < 1 week 
    #     args['rollup'] == INTERVAL_15MIN and len(dts) > (24 * 4 * 7),
    #     # hourly < 1 month
    #     args['rollup'] == INTERVAL_HOURLY and len(dts) > (4 * 24 * 31),
    #     # daily: < 3 months
    #     args['rollup'] == INTERVAL_DAILY and len(dts) > (4 * 24 * 90),
    #     # monthly: < 1 year
    #     args['rollup'] == INTERVAL_MONTHLY and len(dts) > (4 * 24 * 366),
    #     # sum: < 1 year
    #     args['rollup'] == INTERVAL_SUM and len(dts) > (4 * 24 * 366)
    # ]):
    #     messages.add("The submitted request would generate a larger response than we can manage for you right now. Use one of the following combinations of rollup and datetime ranges: 15-minute: < 1 week; hourly < 1 month; daily: < 3 months; monthly: < 1 year; sum: < 1 year. Please either reduce the date/time range queried or increase the time interval for roll-up parameter.")
    #     response = ResponseSchema(
    #         status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
    #         response_data=dict((k, args[k]) for k in ['rollup', 'start_dt', 'end_dt'] if k in args),
    #         messages=messages.messages
    #     )
    #     # return Response(data=response.as_dict(), status=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE)
    #     # return response.as_dict()

    
    # parse sensor ID string to a list. if not provided, the subsequent query will return all sensors.
    sensor_ids = []
    if args['sensor_ids']:
        sensor_ids = [str(i) for i in args['sensor_ids'].split(DELIMITER)]

    # use parsed args and datetime list to query the database
    try:
        print("query_pgdb")
        results = query_pgdb(postgres_table_model, sensor_ids, dts)
    except Exception as e:
        messages.add("Could not retrieve records from the database. Error(s): {0}".format(str(e)))
        response = ResponseSchema(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            messages=messages.messages
        )
        #return Response(data=response.as_dict(), status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        #return response.as_dict()

    # -------------------------------------------------------------------
    # post process the query results, if any

    if len(results) > 0:
        
        # perform selects and/or aggregations based on zerofill and interval args
        print("aggregate_results_by_interval")
        aggregated_results = aggregate_results_by_interval(results, args['rollup'])
        #print("aggregated results\n", etl.fromdicts(aggregated_results))
        print("apply_zerofill")
        zerofilled_results = apply_zerofill(aggregated_results, args['zerofill'], dts)
        # transform the data to the desired format, if any
        print("format_results")
        response_data = format_results(zerofilled_results, args['f']) #, ref_geojson)

        # return the result
        print("completed, returning the results")

        # if the request was for a csv in the legacy teragon format, then we only return that
        if args['f'] in F_CSV:
            print('returning legacy CSV format')
            return Response(response_data, status=status.HTTP_200_OK, content_type="text/csv")
        else:
            response = ResponseSchema(
                status_code=status.HTTP_200_OK,
                request_args=args,
                response_data=response_data,
                messages=messages.messages
            )
            #return Response(response.as_dict(), status=status.HTTP_200_OK)
            #return response.as_dict()

    else:
        # return the result
        messages.add("No records returned.")
        response = ResponseSchema(
            status_code=status.HTTP_200_OK,
            request_args=args,
            messages=messages.messages
        )
        #return Response(response.as_dict(), status=status.HTTP_200_OK)
    
    print("RESPONSE", response.as_dict())
    return response.as_dict()

    

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

def handle_request_for(rainfall_model, request, *args, **kwargs):
    """Helper function that routes requests relative to the job queue.
    """

    raw_args = _parse_request(request)
    print(args, kwargs, raw_args)

    # if the incoming request includes the jobid path argument,
    # then we check for the job in the queue and return its status
    if 'jobid' in kwargs.keys():

        q = get_queue()
        queued_job_ids = q.job_ids
        job = q.fetch_job(kwargs['jobid'])
        print("fetched job", job)

        # if that job exists then we return the status and results, if any
        if job:
            print("{} is in the queue".format(job.id))
            
            # in this case, the absolute URI is the one that got us here,
            # and includes the job id.
            job_url = request.build_absolute_uri()
            job_meta = {
                "job-id": job.id,
                "job-url": job_url
            }
            # job status: one of [queued, started, deferred, finished, failed]
            # (comes direct from Python-RQ)
            job_status = job.get_status()

            # if result isn't None, then the job is completed (may be a success 
            # or failure)
            if job.result:

                # mash up job metadata with any that comes from the 
                # completed task
                meta = job.result['meta']
                meta.update(job_meta)

                # assemble the response object. In addition to the results, 
                # status, and meta, it returns the request arguments **as they
                # were interpreted by the parsers** (this is a good way to see
                # if the arguments were submitted correctly)
                response = ResponseSchema(
                    # queued, started, deferred, finished, or failed
                    status_message=job_status,
                    request_args=job.result['args'],
                    messages=job.result['messages'],
                    response_data=job.result['data'],
                    meta=meta
                )
            else:
                # if there is no result, we return with an updated status 
                # but nothing else will change
                response = ResponseSchema(
                    # queued, started, deferred, finished, or failed
                    request_args=raw_args,
                    status_message=job_status,
                    # messages=['running job {0}'.format(job.id)],
                    meta=job_meta
                )                           

            return Response(response.as_dict(), status=response.status_code)
        else:
            # if the job ID wasn't found, we kick it back.
            response = ResponseSchema(
                request_args={},
                status_message="does not exist",
                messages=['The requested job {} does not exist.'.format(kwargs['jobid'])],
                meta=job_meta
            )
            return Response(response.as_dict(), status=response.status_code)

    # If not, this is a new request. Queue it up and return the job status
    # and a URL for checking on the job status
    else:
        print("This is a new request.", raw_args)
        job = _handler.delay(rainfall_model, raw_args)
        job_url = "{0}{1}/".format(request.build_absolute_uri(request.path), job.id)
        response = ResponseSchema(
            # queued, started, deferred, finished, or failed
            request_args=raw_args,
            status_message=job.get_status(),
            messages=['running job {0}'.format(job.id)],
            meta={
                "job-id": job.id,
                "job-url": job_url
            }
        )

        # return redirect(job_url)
        return Response(response.as_dict(), status=status.HTTP_200_OK)

class RainfallGaugeApiView(APIView):

    def get(self, request, *args, **kwargs):
        return handle_request_for(GaugeObservation, request, *args, **kwargs)

    def post(self, request, *args, **kw):
        return handle_request_for(GaugeObservation, request, *args, **kwargs)


class RainfallGarrApiView(APIView):

    def get(self, request, *args, **kwargs):
        return handle_request_for(GarrObservation, request, *args, **kwargs)

    def post(self, request, *args, **kw):
        return handle_request_for(GarrObservation, request, *args, **kwargs)


class RainfallRtrrApiView(APIView):

    def get(self, request, *args, **kwargs):
        return handle_request_for(RtrrObservation, request, *args, **kwargs)

    def post(self, request, *args, **kw):
        return handle_request_for(RtrrObservation, request, *args, **kwargs)


class RainfallRtrgApiView(APIView):

    def get(self, request, *args, **kwargs):
        return handle_request_for(RtrgObservation, request, *args, **kwargs)

    def post(self, request, *args, **kw):
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