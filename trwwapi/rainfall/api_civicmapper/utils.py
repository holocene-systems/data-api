import json
from os import environ
from datetime import datetime, timedelta
from dateutil.parser import parse
from pytz import timezone

def dt_parser(dt_string, tz_string, tzi, tzinfos):

    if dt_string:
        tz = timezone(tz_string)

        # try:
        dt = parse(dt_string, tzinfos=tzinfos)

        if dt.tzinfo is None:
            return tz.localize(dt).astimezone(tzi).isoformat()
        else:
            return dt.isoformat()

    return None

def datetime_range(dt_start, dt_end, delta):
    if dt_start == dt_end:
        yield dt_start
    else:
        current = dt_start
        while current < dt_end:
            yield current
            current += delta

def datetime_encoder(obj):
    return json.loads(json.dumps(obj, cls=DatetimeStringEncoder))

class DatetimeStringEncoder(json.JSONEncoder):
    """use to convert any values in a dict stored as datetime objects
    into ISO-formatted datetime strings, for export to JSON
    """

    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super(DatetimeStringEncoder, self).default(o)


def get_envvar_w_fallback(name, fallback=None):
    """check for and retrieve an environment variable.
    Optionally return a fallback value if either the key is not found
    or if it's found but has an empty value.
    
    :return: [description]
    :rtype: [type]
    """
    if name in environ.keys():
        if environ[name].strip():
            return environ[name]
    return fallback