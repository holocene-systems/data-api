from django.shortcuts import render
from django.utils.safestring import mark_safe
from rest_framework import routers
from rest_framework.decorators import api_view
from rest_framework.response import Response

from .core import RwPublicResult, RwPublicAnalysis
from ..common.models import TrwwApiResponseSchema

# -------------------------------------------------------------------
# API ROOT VIEW

class ApiRouterRootView(routers.APIRootView):
    """
    Controls appearance of the API root view
    """

    def get_view_name(self):
        return "3RWW Rainways API"

    def get_view_description(self, html=True):
        text = """<p>The 3RWW Rainways API provides a central location for documenting access to the data and processing resources from 3 Rivers Wet Weather and other organizations that are useful for sewer and stormwater modeling in Allegheny County, PA.</p><p>The goal of the Rainways API is to provide a shortcut around a lot of the otherwise tedious work of downloading and prepping the environmental, infrastructure, and geophysical data used in sewer and stormwater modeling. It doesn't represent a new database or central repository, only a quicker and more direct way to access existing datastores that are relevant to this kind of work. To that end, it is designed to:</p><ul><li>use and evolve to use the best available data</li><li>be used for locations that you are specifically interested in; it is not meant to be a general purpose data download portal (you can go elsewhere to get the underlying data)</li>"""
        if html:
            return mark_safe(text)
        else:
            return text

class ApiDefaultRouter(routers.DefaultRouter):
    APIRootView = ApiRouterRootView

# -------------------------------------------------------------------
# API Analytical Views

@api_view(['GET'])
def rainways_area_of_interest_analysis(request):
    """
    Given a GeoJSON, this returns summary statistics for intersecting layers of interest.
    
    This endpoint is used primarily for the public-facing Rainways web app.
    """

    # handle malformed data in request here:
    if 'geojson' not in request.data.keys():

        r = TrwwApiResponseSchema(
            args=request.data,
            status_code=400, 
            status='failed', 
            messages=['Include geojson in `geojson` object within the submitted json']
        )
        return Response(
            data=TrwwApiResponseSchema.Schema().dump(r),
            status=r.status_code
        )


    # conduct analysis
    analysis = RwPublicAnalysis(request.data['geojson'])
    
    analysis.slope_summary()
    analysis.soil_summary()
    analysis.sustain_summary()
    analysis.rainfall_summary()

    r = TrwwApiResponseSchema(
        # args={"geojson": analysis.aoi_geojson},
        data=RwPublicResult.Schema().dump(analysis.results), # response schema expects a dictionary here.
        status_code=200, 
        status='success', 
        messages=analysis.messages,
        meta={"count": len(analysis.aoi_gdf.index)}
    )

    return Response(data=TrwwApiResponseSchema.Schema().dump(r), status=r.status_code)