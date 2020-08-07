from django.test import Client as client
import pytest

from .api_civicmapper.core import parse_datetime_args, _minmax

# @pytest.mark.django_db
# def test_rainfall_garr_response(client):
#     response = client.get('/rainfall/v2/garr/')
#     assert response.status_code == 200

# @pytest.mark.django_db
# def test_tracts_api_response(client):
#     response = client.get('/rainfall/v2/gauge/')
#     assert response.status_code == 200

class TestCoreFunctions:

    # def test_parse_datetime_args():
    #     r1 = parse_datetime_args(["2020-05-29")
    #     r2 = parse_datetime_args("2010")
    #     r3 = parse_datetime_args("2010")
    #     r4 = parse_datetime_args("2010")
    
    def test_minmax_tz_parsing(self):
        isoformat_w_tz_offset = _minmax(["2020-05-29T10:00:00-04:00", "2020-05-30T01:00:00-04:00"])
        assert isinstance(isoformat_w_tz_offset, str)
        isoformat_with_utc_z = _minmax(["2020-07-28T18:30:00.000Z", "2020-07-28T20:30:00.000Z"])
        assert isinstance(isoformat_with_utc_z, str)
        mixed_formats = _minmax(["2020-05-01T04:00:00.000Z", "2020-05-31T23:59:59-04:00"])
        assert isinstance(mixed_formats, str)