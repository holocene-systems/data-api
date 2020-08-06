"""core logic supporting implementation of a legacy (Teragon) API, provided for backwards-compatible
"""

from pathlib import PurePosixPath
from datetime import datetime, timedelta
from urllib.parse import parse_qs
from collections import OrderedDict
from dateutil.parser import parse
from dateutil import tz
import petl as etl
import pandas as pd
import numpy as np
from tenacity import retry, wait_random_exponential, stop_after_attempt, stop_after_delay
import geojson

from django.db.models import Q


from .models import RequestSchema, RainfallObservation, TableGARR15, TableGauge15, TableRTRR15
from .utils import datetime_range
from .config import (
    DATA_DIR,
    TZI,
    RAINGAUGE_RESOURCE_PATH,
    PIXEL_RESOURCE_PATH,
    RTRR_RESOURCE_PATH,
    PIXEL_DELIMITER,
    GAUGE_DELIMITER,
    DELIMITER,
    INTERVAL_15MIN,
    INTERVAL_HOURLY,
    INTERVAL_DAILY,
    INTERVAL_SUM,
    TZ,
    TZINFOS,
    F_CSV,
    F_GEOJSON,
    F_JSON,
    F_MD,
    F_ALL,
    F_ARRAYS
)

# CONSTANTS ---------------------------------------------------------

DDB_TABLE_LOOKUP = {
    RAINGAUGE_RESOURCE_PATH: TableGauge15,
    PIXEL_RESOURCE_PATH: TableGARR15,
    RTRR_RESOURCE_PATH: TableRTRR15
}

REF_GEOJSON_LOOKUP = {
    RAINGAUGE_RESOURCE_PATH: DATA_DIR / 'gauges.geojson',
    PIXEL_RESOURCE_PATH: DATA_DIR / 'pixels.geojson',
    RTRR_RESOURCE_PATH: DATA_DIR / 'pixels.geojson'
}

RAINFALL_BASE_MODEL_REF = RainfallObservation()

# STATIC DATA REFERENCES --------------------------------------------
# Create complete lists of pixel and gauge IDs, to use as default for 
# request if not available

# HELPERS -----------------------------------------------------------

def parse_args_to_dict(body, list_delimiter=DELIMITER, replacement_delimiter=DELIMITER):
    """Use urllib query string parser to turn the body or querty string of the request into a
    dictionary, after handling any potentially non-standard delimiters
    """
    cleaned_body = body.replace(list_delimiter, replacement_delimiter)
    parsed_body = parse_qs(cleaned_body)
    return {k: v[0] for k, v in parsed_body.items()}

def parse_apigw_event(resource, request_args):
    """parse the resource path and request args (which may be in body or query string) from the request into objects
    
    :param resource: path of the api resource; resource key in the event json
    :type resource: str
    :param body: body of request; body key in the event json
    :type body: str
    :return: body contents as a dictionary, associated table as a PynamoDB Table object
    :rtype: (dict, PynamoDB.model)
    """

    args, table = None, None

    # get the name of the resource from the resource path
    # e.g., pixel, raingauge, realtime. Those constants stored in config.py
    p = PurePosixPath(resource).name

    # if the args are a dictionary (i.e., from the query string), use as-is
    if isinstance(request_args, dict):
        args = request_args
    # parse the body string into a dictionary, handling any weirdly delimited args that might affect parsing
    elif isinstance(request_args, str):
        if p == RAINGAUGE_RESOURCE_PATH:
            args = parse_args_to_dict(request_args, GAUGE_DELIMITER, DELIMITER)
        elif p == PIXEL_RESOURCE_PATH:
            args = parse_args_to_dict(request_args, PIXEL_DELIMITER, DELIMITER)
    else:
        return None, None, None
    
    # use the parsed name to get the model for the table that this 
    # request is to be associated with
    table = DDB_TABLE_LOOKUP[p]
    geo = REF_GEOJSON_LOOKUP[p]
    print("database table:", table.Meta.table_name)

    return args, table, geo

def parse_and_validate_args(raw_args,):
    """validate request args and parse using Marshmallow pre-processor schema.
    
    :param raw_args: kwargs parsed from request
    :type raw_args: dict
    :return: validated kwargs
    :rtype: dict
    """
    request_schema = RequestSchema()
    return request_schema.load(raw_args)

def parse_sensor_ids(id_string, fallback_ref_geojson, delimiter=DELIMITER):
    """parse the DELIMITER-joined string of ids provided via the HTTP request
    body or query string in a list of IDs. IDs are always strings.

    **If no id_string is provided, this is where we fallback to all IDs, 
    derived from the fallback_ref_geojson file.**
    
    :param id_string: DELIMITER-joined string of ids provided via the HTTP request
    body or query string.
    :type id_string: str
    :param fallback_ref_geojson: path to geojson file for the sensor
    :type fallback_ref_geojson: str
    :param delimiter: delimiter of the id_string; used to split it into array 
        defaults to the constant set in config.py
    :type DELIMITER: str
    :return: [description]
    :rtype: [type]
    """
    # return a list of sensors ids from the DELIMITER-joined string that 
    # comes from the HTTP request body or query string
    sensor_ids = []
    if id_string:
        sensor_ids = [str(i) for i in id_string.split(delimiter)]
        if len(sensor_ids) > 0:
            return sensor_ids
    # in the absence of that, get all the sensor IDs by default
    with open(fallback_ref_geojson) as fp:
        fc = geojson.load(fp)
    return list(map(lambda f: str(f['id']), fc['features']))

def parse_datetime_args(start_dt, end_dt, interval=None, delta=15):
    """parse start and end datetimes into a list of all datetimes between 
    (inclusive). We do this so we can make targeted querys of on DynamoDB--
    no scan/filter required.

    For intervals other than base, adjust the datetimes so that enough records 
    are acquired for aggregation later.

    In the original Teragon API, hourly and daily params are in effect unions 
    of the input start/end with available data.; i.e. a request for 6 hours 
    of rainfall but with an interval of "daily" returns the rainfall total 
    for that day regardless of the hours spec'd; a request from noon on 
    day 1 through noon on day 3 gets complete daily totals for the 3 days.
    """

    # ---------------------------------
    # handle missing start/end params

    # if start and end provided
    if start_dt and end_dt:
        #print("start_dt and end_dt")
        # we're good
        pass
    # if only start or end provided, set the one that's missing to the other one.
    elif (start_dt and not end_dt):
        #print("(start_dt and not end_dt)")
        end_dt = start_dt
    elif (end_dt and not start_dt):
        #print("(end_dt and not start_dt):")
        start_dt = end_dt
    # in case we made it this far without either:
    else:
        raise ValueError

    # put them in the right order
    dts = [start_dt, end_dt]
    dts.sort()
    start_dt, end_dt = dts[0], dts[1]
    #print("dts list", dts)

    # ---------------------------------
    # adjust start and end params based on interval

    if interval == INTERVAL_DAILY:
        # => if interval=daily, then we'll need all intervals for both days
        
        # 'round down' to beginning of this day
        start_dt = start_dt.replace(hour=0, minute=0)
        # 'round up' beginning of next day from end_dt
        #end_dt = end_dt + timedelta(days=1)
        #end_dt = end_dt.replace(hour=0, minute=0)
        end_dt = end_dt.replace(hour=23, minute=45)

        # Maybe This? if interval=daily, then set end time as one day from start time 

    elif interval == INTERVAL_HOURLY:
        # NOTE: this isn't doing anything if we don't enable minutes as a an arg

        # => if interval=hourly, then get intervals for overlapping hours
        start_dt = start_dt.replace(minute=0) # 'round down' to beginning of this hour
        end_dt = end_dt + timedelta(hours=1) # 'round up' beginning of next hour from end_dt
        end_dt = end_dt.replace(minute=0)
        # Maybe This? if interval=hourly, then set end time as one hour from start time

    else:
        # NOTE: we might need to do things here with this if we enable minutes as a an arg
        # => if interval=15-minute (default), then, round to nearest quarter hour; set the end to the same time
        # => if interval=15-minute (default), then we'll get all intervals
        pass
    
    dts = [
        dt.isoformat() for dt in 
        datetime_range(
            start_dt, 
            end_dt, 
            timedelta(minutes=delta)
        )
    ]
    # print(len(dts), "datetimes to be queried")
    return dts

@retry(stop=(stop_after_attempt(5) | stop_after_delay(60)), wait=wait_random_exponential(multiplier=2, max=30), reraise=True)
def query_ddb_exact(pynamodb_table, sensor_ids, all_datetimes):
    """query the *exact records* needed from the db
    
    :param pynamodb_table: [description]
    :type pynamodb_table: [type]
    :param id_string: [description]
    :type id_string: [type]
    :param all_datetimes: [description]
    :type all_datetimes: [type]
    :return: PETL table containing results
    :rtype: petl table 
    """
    print("querying table", pynamodb_table.Meta.table_name)

    # construct the batch-get query args by creating a list of
    # all combinations of sensor ids with date/times
    # (i.e., create the exact list of the primary keys in the table that we'll get)
    item_keys = []
    for i in sensor_ids: # parse_sensor_ids(id_string):
        for each_dt in all_datetimes:
            item_keys.append((i, each_dt))
    #print(item_keys)
    # batch-get records from DDB, and map resulting PynamoDB 
    # objects to a list of dictionaries
    records = []
    for item in pynamodb_table.batch_get(item_keys):
        records.append(item.attribute_values)
    print("returned", len(records), "records")
    return records

@retry(stop=(stop_after_attempt(5) | stop_after_delay(60)), wait=wait_random_exponential(multiplier=2, max=30), reraise=True)
def query_pgdb(postgres_table_model, sensor_ids, all_datetimes):

    tablename = postgres_table_model.objects.model._meta.db_table
    print("querying {0}".format(tablename))

    queryset = postgres_table_model.objects.filter(
        Q(timestamp__gte=all_datetimes[0]),
        Q(timestamp__lte=all_datetimes[-1])
    )

    rows = postgres_table_model.as_dataframe(queryset).to_dict(orient='records')

    # if sensor ids are spec'd we filter the rest out of the rows first.
    if sensor_ids:
        for row in rows:
            for sensor_id, observation in row['data'].items():
                row['data'] = dict((k, row['data'][k]) for k in sensor_ids if k in row['data'])
    
    # then we create a new table where every row represents a single observation for a single sensor
    newrows = []
    for row in rows:
        for sensor_id, observation in row['data'].items():
            newrows.append(dict(ts=row['timestamp'].isoformat(), id=sensor_id, val=observation[0], src=observation[1]))
    # print(newrows)
    return newrows

def _rollup_date(dts, interval=None):
    """format date/time string based on interval spec'd for summation
    """
    # print(dts, type(dts))
    s = "%Y-%m" # initial datetime string format - always years and months
    if interval == INTERVAL_DAILY:
        s += "-%d" # add days
    elif interval == INTERVAL_HOURLY:
        s += "-%dT%H" # add days and hours
    # by default returns year and month
    dt = parse(dts).strftime(s)
    # print(dt)
    return dt

def _sumround(i):
    """sum all values in iterable `i`, and round the result to 
    the 5th decimal place
    """
    return round(sum([n for n in i if n]), 5)

def _listset(i):
    """create a list of unique values from iterable `i`, and 
    return those as comma-separated string
    """
    return ", ".join(list(set(i)))

def _minmax(i):
    vals = list(set(i))
    return "{0}/{1}".format(min(vals), max(vals))
    return "{0}/{1}".format(
        TZ.localize(parse(min(vals))), 
        TZ.localize(parse(max(vals)))
    )

def aggregate_results_by_interval(query_results, rollup):
    """aggregate the values in the query results based on the rollup args

    Aggregation is performed for:

    * hourly or daily time intervals
    * total

    NOTE: in order to handle potential No-Data values in the DB during aggregation, we
    convert them to 0. The `src` field then indicates if any values in the rollup were N/D.
    Then, if the value field in the aggregated row still shows 0 after summation, *and*
    the src field shows N/D, we turn that zero into None. If there was a partial reading
    (e.g., the sensor has values for the first half hour but N/D for the second, and we are 
    doing an hourly rollup), then the values will stay there, but the source field will indicate
    both N/D and whatever the source was for the workable sensor values.
    """
    # print("rollup", rollup)
    if rollup in [INTERVAL_DAILY, INTERVAL_HOURLY]:

        petl_aggs = OrderedDict(
            val=('val', _sumround), # sum the rainfall vales
            src=('src', _listset) # create a list of all rainfall sources included in the rollup
        )

        t = etl\
            .fromdicts(query_results)\
            .convert(
                'ts', 
                lambda v: _rollup_date(v, rollup), # convert datetimes to their rolled-up value
                failonerror=True
            )\
            .convert(
                'val', 
                lambda v: 0 if v is None else v, # convert rainfall values to 0 if no-data
                failonerror=True
            )\
            .aggregate(
                ('ts', 'id'), 
                petl_aggs # aggregate rainfall values (sum) and sources (list) for each timestamp+ID combo,
            )\
            .convert(
                'ts', 
                lambda v: TZ.localize(parse(v)).isoformat(), # convert that datetime to iso format w/ timezone
                failonerror=True
            )\
            .convert(
                'val', 
                lambda v, r: None if ('N/D' in r.src and v == 0) else v, # replace 0 values with no data if aggregated source says its N/D
                pass_row=True,
                failonerror=True
            )\
            .sort('id')

        # print(t)

        return list(etl.dicts(t))

    elif rollup in [INTERVAL_SUM]:

        petl_aggs = OrderedDict(
            val=('val', _sumround), # sum the rainfall vales
            src=('src', _listset), # create a list of all rainfall sources included in the rollup
            ts=('ts', _minmax) # create a iso datetime range string from the min and max datetimes found
        )

        t = etl\
            .fromdicts(query_results)\
            .aggregate(
                'id', 
                petl_aggs # aggregate rainfall values (sum) and sources (list), and datetimes (str) for each ID,
            )\
            .convert(
                'val', 
                lambda v, r: None if ('N/D' in r.src and v == 0) else v, # replace 0 values with no data if aggregated source says its N/D
                pass_row=True
            )\
            .sort('id')

        return list(etl.dicts(t))
            
    else:
        # ensure that all records have the same shape:
        return list(etl.dicts(etl.fromdicts(query_results)))

def apply_zerofill(transformed_results, zerofill, dts):
    """applies zerofill, which is to say, if zerofill==False, determines
    if *all* sensors for a given time interval report zero, and removes all those
    records from the response. The result is table where any given time interval
    is guaranteed to have rainfall values > 0 for at least one sensor. 
    
    This potentially this shortens up the response quite a bit...but because of 
    the way we're storing data and the PETL select method used for identifying 
    candidate records, this process might be pretty slow.
    """
    
    if zerofill:
        return transformed_results
    else:
        tables = []
        t = etl.fromdicts(transformed_results)
        for dt in dts:
            s = etl.select(t, lambda rec: rec.ts == dt and rec.val > 0)
            tables.append(s)

        if tables:
            return list(etl.stack(*tables).dicts())
        else:
            # return an empty table
            return [{k: None for k in RAINFALL_BASE_MODEL_REF.get_attributes().keys()}]

def _format_as_geojson(results, geojson_path):
    """NOTE: NOT WORKING
    """

    # read the geojson into a PETL table object
    with open(geojson_path) as fp:
        fc = geojson.load(fp)
    features_table = etl.fromdicts(fc.features).convert('id', str)

    # join the results to the geojson features, then shoehorn the results into the properties object of each feature
    # put the ID into the id property of the feature
    features = etl\
        .fromdicts(results)\
        .leftjoin(features_table, 'id')\
        .sort(('ts', 'id'))\
        .aggregate(key=('id', 'type', 'geometry'), aggregation=list, value=['src', 'val', 'ts'])\
        .fieldmap(
            {
                'id':'id',
                'type':'type',
                'geometry':'geometry',
                'properties': lambda rec: (
                    dict(
                        data=[dict(src=r[0],val=r[1],ts=r[2]) for r in rec.value],
                        total=sum([r[1] for r in rec.value if r[1]])
                    )
                )
            },
            failonerror=True
        ).dicts()

    return geojson.FeatureCollection(features=list(features))

def _format_teragon(results):
    """convert the query results (an array of dictionaries) to a cross-tab
    with a metadata column for data source

    TODO: this uses both PETL and Pandas to achieve the desired results; 
    pick one or the other
    """
    # use petl for this part of the transformation
    t = etl.fromdicts(results)
    #print(etl.header(t))
    
    t2 = etl\
        .melt(t, key=['ts', 'id'])\
        .convert('id', lambda v: "{}-src".format(v), where=lambda r: r.variable == 'src')\
        .convert('value', float, where=lambda r: r.variable == 'val')\
        .cutout('variable')\
        .sort(['ts', 'id'])
    #print(etl.header(t2))

    df = etl\
        .rename(t2, 'ts','timestamp')\
        .todataframe()
    
    # Use pandas for the pivoting
    df2 = pd.pivot_table(
        df, 
        index=["timestamp"],
        columns=["id"],
        values=["value"],
        aggfunc=lambda x: ' '.join(x) if isinstance(x, str) else np.sum(x)
    )
    # replace the multi-index field with a single row
    df2.columns = df2.columns.get_level_values(1)
    # return as a PETL table
    #return etl.fromdataframe(df2, include_index=True).rename('index', 'timestamp
    return df2.to_csv()

def _groupby(results, key='ts', sortby='id'):

    key_by_these = sorted(list(set(map((lambda r: r[key]), results))))

    other_fields = [f for f in results[0].keys() if f != key]

    remapped = []

    for key_by_this in key_by_these:
        x = [i for i in map((lambda r: r if r[key] == key_by_this else None),  results) if i]
        data = [{f: xi[f] for f in other_fields} for xi in x]
        if sortby:
            data = sorted(data, key=lambda k: k[sortby])
        remapped.append({
            key: key_by_this,
            "data": data
        })
    return remapped

def format_results(results, f): #, ref_geojson):
    """handle parsing the format argument to convert 
    the results 'table' into one of the desired formats

    By default, this just passes through the DynamoDB response: a list of dicts
    
    :param results: [description]
    :type results: [type]
    :param f: [description]
    :type f: [type]
    :param ref_geojson: [description]
    :type ref_geojson: [type]
    :return: [description]
    :rtype: [type]
    """
    
    # make submitted value lowercase, to simplify comparison
    f = f.lower()
    # fall back to JSON if no format provided
    if f not in F_ALL:
        f = F_JSON[0]

    # JSON format 
    if f in F_JSON:

        if f == 'time':
            # grouped by timestamp
            return _groupby(results, key='ts', sortby='id')
        elif f == 'sensor':
            # grouped by id
            return _groupby(results, key='id', sortby='ts')
        else:
            # (list of dicts)
            return results

    # GEOJSON format (GeoJSON Feature collection; results under 'data' key within properties)
    # elif f in F_GEOJSON:
    #     return _format_as_geojson(results, ref_geojson)

    # ARRAYS format (2D table)
    elif f in F_ARRAYS:
        # nested arrays
        t = etl.fromdicts(results)
        h = list(etl.header(t))
        return list(etl.data(t)).insert(0,h)

    elif f in F_CSV:
        return _format_teragon(results)

    # elif f in F_MD:
    #     return results

    else:
        return results