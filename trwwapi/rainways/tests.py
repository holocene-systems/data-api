from django.test import TestCase, SimpleTestCase
from rest_framework.test import APIClient, APIRequestFactory
import requests

from .core import RwPublicResult
from .views import rainways_area_of_interest_analysis
from ..common.models import TrwwApiResponseSchema

class RwPublicTestCases(SimpleTestCase):

    client_class = APIClient
    

    def setUp(self):

        self.factory = APIRequestFactory()

        self.alco_parcel_pin = "0049K00062000000" #"0082H00001000002" 
        # URLs for parcel and soil feature services, respectively
        self.alco_parcel_url = "https://gisdata.alleghenycounty.us/arcgis/rest/services/OPENDATA/Parcels/MapServer/0/query"

        r = requests.get(
            self.alco_parcel_url,
            params=dict(
                where="PIN='{0}'".format(self.alco_parcel_pin),
                outFields='PIN',
                returnGeometry='true',
                outSR=2272,
                f='geojson'
            )
        )
        self.aoi_geojson = r.json()

    def test_aoi_analysis_e2e(self):

        request = self.factory.post(
            'acsa/aoi-analysis/',
            data={"geojson": self.aoi_geojson},
            format='json'
        )
        response = rainways_area_of_interest_analysis(request)
        print(response.data)
        self.assertEqual(response.status_code, 200)
        r = TrwwApiResponseSchema.Schema().load(response.data)
        self.assertIsInstance(r, TrwwApiResponseSchema)