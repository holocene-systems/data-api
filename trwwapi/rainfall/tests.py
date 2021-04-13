from django.test import Client as client
import pytest

from dateutil.parser import parse

from .api_v2.core import (
    parse_datetime_args, 
    _minmax,
    _rollup_date
)
from .api_v2.utils import dt_parser
from .api_v2.config import (
    TZ, TZI, TZ_STRING, TZINFOS, 
    INTERVAL_15MIN, INTERVAL_HOURLY, INTERVAL_DAILY, INTERVAL_SUM
)

# @pytest.mark.django_db
# def test_rainfall_garr_response(client):
#     response = client.get('/rainfall/v2/garr/')
#     assert response.status_code == 200

# @pytest.mark.django_db
# def test_tracts_api_response(client):
#     response = client.get('/rainfall/v2/gauge/')
#     assert response.status_code == 200

class TestRequestDatetimeParamParsing:
    """The codebase works with datetimes in a bunch of places.

    When the request is received, RequestSchema parses datetimes
    to make sure they're valid and have timezones, and will
    convert to the configured **local timezone** as needed.
    
    When querying the database, the start and end datetimes are 
    rounded, depending on the interval selected, to the nearest 15-min
    or hour interval.

    In the database, datetimes are stored with timezone *in UTC* in timezone a 
    `timestamptz` field. The query (via the Django ORM), returns them
    as `datetime` objects with `tzinfo` as `UTC`.

    The timestamps returned from the database are parsed again to convert
    them to the local timezone, in a couple different of places in 
    `core.aggregate_results_by_interval`. For time requests for hourly or 
    daily rollups:

    * core._rollup_date
    * TZ.localize(parse(v)).isoformat()

    For summation:

    * core._minmax

    Note that if there is no rollup (i.e., 15-min intervals), they're read 
    back in as-is.
    """


    # --------------------------------
    # timezone handling for individual datetimes
    # tests dt_parser, used during request handling
    # 
    # timestamps in the database are stored with a timezone; requests can come 
    # in any format. we need to be able to handle those, and always return a 
    # timezone-aware datetime string

    def test_iso8061_w_exact_tz(self):
        assert "2020-04-08T12:00:00-04:00" == dt_parser("2020-04-08T12:00:00-04:00", tz_string=TZ_STRING, tzi=TZI, tzinfos=TZINFOS)

    def test_iso8061_w_other_tz(self):
        assert "2020-04-08T12:00:00-04:00" == dt_parser("2020-04-08T16:00:00+00:00", tz_string=TZ_STRING, tzi=TZI, tzinfos=TZINFOS)

    def test_iso8061_w_utcz(self):
        assert "2020-04-08T12:00:00-04:00" == dt_parser("2020-04-08T16:00:00.000Z", tz_string=TZ_STRING, tzi=TZI, tzinfos=TZINFOS)

    def test_iso8061_no_tz(self):
        assert "2020-04-08T12:00:00-04:00" == dt_parser("2020-04-08T12:00", tz_string=TZ_STRING, tzi=TZI, tzinfos=TZINFOS)

    def test_other_dt_formats_w_no_tz(self):
        pass 


    # --------------------------------
    # Test the logic for picking the right min and max datetimes, specifically
    # in cases where the results are to be aggregated to hour or day.
    # This tests what timestamps will be used to query the database with a
    # "timestamp > start_dt and timestamp <= end_dt" conditional

    def test_parse_args_daily_same_day(self):
        start, end = parse_datetime_args(parse("2020-04-07T06:00:00-04:00"), parse("2020-04-07T14:00:00-04:00"), interval=INTERVAL_DAILY)
        assert start.isoformat() == "2020-04-07T00:00:00-04:00"
        assert end.isoformat() == "2020-04-08T00:00:00-04:00"

    def test_parse_args_daily_diff_day(self):
        start, end = parse_datetime_args(parse("2020-04-17T11:00:00-04:00"), parse("2020-04-18T05:00:00-04:00"), interval=INTERVAL_DAILY)
        assert start.isoformat() == "2020-04-17T00:00:00-04:00"
        assert end.isoformat() == "2020-04-19T00:00:00-04:00"

    def test_parse_args_hourly_same_hour(self):
        start, end = parse_datetime_args(parse("2020-04-07T11:00:00-04:00"), parse("2020-04-07T11:47:00-04:00"), interval=INTERVAL_HOURLY)
        assert start.isoformat() == "2020-04-07T11:00:00-04:00"
        assert end.isoformat() == "2020-04-07T12:00:00-04:00"

    def test_parse_args_hourly_diff_hour_on_hour(self):
        start, end = parse_datetime_args(parse("2020-04-07T11:00:00-04:00"), parse("2020-04-07T13:00:00-04:00"), interval=INTERVAL_HOURLY)
        assert start.isoformat() == "2020-04-07T11:00:00-04:00"
        assert end.isoformat() == "2020-04-07T13:00:00-04:00"

    def test_parse_args_hourly_diff_hour_off_hour(self):
        start, end = parse_datetime_args(parse("2020-04-07T11:00:00-04:00"), parse("2020-04-07T13:15:00-04:00"), interval=INTERVAL_HOURLY)
        assert start.isoformat() == "2020-04-07T11:00:00-04:00"
        assert end.isoformat() == "2020-04-07T14:00:00-04:00"

    def test_parse_args_hourly_diff_hour_off_hours(self):
        start, end = parse_datetime_args(parse("2020-04-07T10:45:00-04:00"), parse("2020-04-07T13:15:00-04:00"), interval=INTERVAL_HOURLY)
        assert start.isoformat() == "2020-04-07T10:00:00-04:00"
        assert end.isoformat() == "2020-04-07T14:00:00-04:00"

    def test_parse_args_hourly_diff_day_hour_on_hour(self):
        start, end = parse_datetime_args(parse("2020-04-17T11:00:00-04:00"), parse("2020-04-18T05:00:00-04:00"), interval=INTERVAL_HOURLY)
        assert start.isoformat() == "2020-04-17T11:00:00-04:00"
        assert end.isoformat() == "2020-04-18T05:00:00-04:00"

    def test_parse_args_hourly_diff_day_hour_off_hour(self):
        start, end = parse_datetime_args(parse("2020-04-17T11:00:00-04:00"), parse("2020-04-18T05:20:00-04:00"), interval=INTERVAL_HOURLY)
        assert start.isoformat() == "2020-04-17T11:00:00-04:00"
        assert end.isoformat() == "2020-04-18T06:00:00-04:00"


    # --------------------------------
    # datetime ranges
    # a datetime range--created when user requests a rainfall total and not an
    # interval--should always be returned in the configured local TZ,
    # regardless of how it was submitted.

    def test_range_w_tz_offset(self):
        isoformat_w_tz_offset = _minmax(["2020-05-29T10:00:00-04:00", "2020-05-30T01:00:00-04:00"])
        assert isinstance(isoformat_w_tz_offset, str)
        assert isoformat_w_tz_offset == "2020-05-29T10:00:00-04:00/2020-05-30T01:00:00-04:00"

    def test_range_w_utc_z(self):
        isoformat_with_utc_z = _minmax(["2020-07-28T18:30:00.000Z", "2020-07-28T20:30:00.000Z"])
        assert isinstance(isoformat_with_utc_z, str)
        assert isoformat_with_utc_z == "2020-07-28T14:30:00-04:00/2020-07-28T16:30:00-04:00"

    def test_range_w_no_tz(self):
        no_tz = _minmax(["2020-05-01T04:00", "2020-05-31T23:59:59"])
        assert isinstance(no_tz, str)
        assert no_tz == "2020-05-01T04:00:00-04:00/2020-05-31T23:59:59-04:00"

    def test_range_w_mixed_formats_01(self):
        mixed_formats = _minmax(["2020-05-01T04:00:00.000Z", "2020-05-31T23:59:59-04:00"])
        assert isinstance(mixed_formats, str)
        assert mixed_formats == "2020-05-01T00:00:00-04:00/2020-05-31T23:59:59-04:00"

    # --------------------------------
    # test datetime rollup logic when aggregating results

    def test_rollup_daily(self):
        r = _rollup_date("2020-04-17T11:00:00-04:00", INTERVAL_DAILY)
        assert "2020-04-17" == r

    def test_rollup_hourly_on_hour(self):
        r = _rollup_date("2020-04-17T11:00:00-04:00", INTERVAL_HOURLY)
        assert "2020-04-17T10:00:00-04:00/2020-04-17T11:00:00-04:00" == r

    def test_rollup_hourly_off_hour(self):
        r = _rollup_date("2020-04-17T11:15:00-04:00", INTERVAL_HOURLY)
        assert "2020-04-17T11:00:00-04:00/2020-04-17T12:00:00-04:00" == r

    def test_rollup_other(self):
        r = _rollup_date("2020-04-17T11:18:00-04:00", INTERVAL_15MIN)
        assert "2020-04-17T11:18:00-04:00" == r