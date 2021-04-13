import json
from pathlib import PurePosixPath
from os import environ

import petl as etl
from marshmallow import ValidationError
from pynamodb.exceptions import GetError

from .core import (
    parse_apigw_event, 
    parse_and_validate_args, 
    parse_datetime_args, 
    parse_sensor_ids,
    query_ddb_exact, 
    aggregate_results_by_interval, 
    apply_zerofill,
    format_results
)
from .models import ResponseSchema
from .utils import datetime_encoder
from .config import REF_DOCUMENTATION_URL

def handler(event, context):

    # # Log the event argument for debugging and for use in local development.
    #print(event)

    # -------------------------------------------------------------------
    # handle requests

    # parse the resource path
    p = PurePosixPath(event['resource']).name
    print(event['httpMethod'], p, event['body'])

    # handle an empty request body
    if (
        (event['httpMethod'] == 'POST' and not event['body']) or (event['httpMethod'] == 'GET' and not event['queryStringParameters'])
    ):
        response = ResponseSchema(
            status_code=400,
            message="No arguments provided in the request. See documentation at {0} for example requests.".format(REF_DOCUMENTATION_URL),
        )
        return response.as_dict()

    # -------------------------------------------------------------------
    # parse and validate arguments

    # **parse** the arguments from the query string; 
    # determine which table we'll be looking at for the request
    # determine which reference geojson might be used
    if event['httpMethod'] == 'GET':
        raw_args, ddb_table, ref_geojson = parse_apigw_event(event['resource'], event['queryStringParameters'])
    else:
        raw_args, ddb_table, ref_geojson = parse_apigw_event(event['resource'], event['body'])

    # **validate** the arguments using a marshmallow model
    # this will convert datetimes to the proper format
    try:
        args = parse_and_validate_args(raw_args)
    # return errors from validation
    except ValidationError as e:
        print(e.messages)
        response = ResponseSchema(
            status_code=400,
            message="{1}. See documentation at {0} for example requests.".format(e.messages, REF_DOCUMENTATION_URL)
        )
        return response.as_dict()
    except KeyError as e:
        response = ResponseSchema(
            status_code=400,
            message="Invalid request arguments ({1}). See documentation at {0} for example requests".format(REF_DOCUMENTATION_URL, e)
        )
        return response.as_dict()
        
    print("request args:", datetime_encoder(args))

    # -------------------------------------------------------------------
    # build a query from request args and submit it 

    # here we figure out all possible datetimes and sensor ids. The combination of these
    # make up the primary keys in the database
    
    # parse the datetime parameters into a complete list of all possible date times
    dts = parse_datetime_args(args['start_dt'], args['end_dt'], args['rollup'])
    # parse sensor ID string to a list, falling back to all IDs for a sensor if none were provided
    sensor_ids = parse_sensor_ids(args['sensor_ids'], ref_geojson)

    # use parsed args and datetime list to query the database
    try:
        results = query_ddb_exact(ddb_table, sensor_ids, dts)
    except GetError as e:
        print(e)
        response = ResponseSchema(
            status_code=500,
            message="Could not retrieve records from the database. Error(s): {0}".format(str(e))
        )
        return response.as_dict()

    # -------------------------------------------------------------------
    # post process the query results, if any

    if len(results) > 0:

        # perform selects and/or aggregations based on zerofill and interval args
        aggregated_results = aggregate_results_by_interval(results, args['rollup'])
        #print("aggregated results\n", etl.fromdicts(aggregated_results))
        zerofilled_results = apply_zerofill(aggregated_results, args['zerofill'], dts)
        # transform the data to the desired format, if any
        # (by default it just returns the DynamoDB response: a list of dicts)
        response_data = format_results(zerofilled_results, args['f'], ref_geojson)

        # return the result
        response = ResponseSchema(
            status_code=200,
            request_args=args,
            response_data=response_data
        )

    else:
        # return the result
        response = ResponseSchema(
            status_code=200,
            request_args=args,
            message="No records returned."
        )            

    return response.as_dict()