from dataclasses import dataclass
from django.contrib.gis.db import models
# from django.contrib.postgres.indexes import 
from django.db.models import JSONField
from django.db.models.constraints import UniqueConstraint
from .mixins import PandasModelMixin, TimestampedMixin


class RainfallObservationMeta(PandasModelMixin):
    """Base abstract model for all rainfall observations ORM models.
    Each record is a point in time. Observations by sensor are stored in JSON 
    'data' field, where keys are the sensor ids, and values are an array; 
    the first item in the array is the rainfall amount, the second is any 
    metadata about the recording. 
    
    An single record as a Python dictionary or JSON object then looks like this:

        {
            "timestamp": 2020-11-30T07:00:00+00:00
            "data": {
                "123456": [0.25, "G-4"],
                "234561": [0.17, "G-4"],
                ...
            }
        }

    """

    timestamp = models.DateTimeField(db_index=True)
    data = JSONField()

    class Meta:
        abstract = True
        ordering = ['-timestamp']
        constraints = [
            UniqueConstraint(fields=['timestamp'], name='%(class)s_uniq_timestamp_constraint')
        ]

    def __str__(self):
        return str(self.timestamp)
    

class GaugeObservation(RainfallObservationMeta):
    """Calibrated Rain Gauge data (historic)
    """
    pass


class GarrObservation(RainfallObservationMeta):
    """Gauge-Adjusted Radar Rainfall (historic)
    """    
    pass


class RtrrObservation(RainfallObservationMeta):
    """Raw Radar data (real-time)
    """        
    pass


class RtrgObservation(RainfallObservationMeta):
    """Raw Rain Gauge data (real-time)
    """    
    pass


class RainfallReport(TimestampedMixin):
    month_start = models.DateField()
    document = models.FileField()
    # events = models.ManyToManyField('RainfallEvent')


class RainfallEvent(PandasModelMixin, TimestampedMixin):

    report_label = models.CharField(max_length=255)
    event_label = models.CharField(max_length=255)
    start_dt = models.DateTimeField()
    end_dt = models.DateTimeField()
    # TODO: add a report_document relate field for access to a report model w/ the PDFs

    @property
    def duration(self):
        duration = self.end_dt - self.start_dt
        seconds = duration.total_seconds()
        hours = seconds // 3600
        # minutes = (seconds % 3600) // 60
        #seconds = seconds % 60
        return hours

    class Meta:
        ordering = ['-end_dt']

    def __str__(self):
        return self.event_label




# TODO: create a class for managing and validating the contents of 
# the RainfallObservationMeta data and metadata fields
# @dataclass
# class RainfallObservationsJSON():
# pass


class Pixel(PandasModelMixin):
    pixel_id = models.CharField(max_length=12)
    geom = models.PolygonField()

    def __str__(self):
        return self.pixel_id


class Gauge(PandasModelMixin):

    web_id = models.IntegerField(null=True)
    ext_id = models.CharField(max_length=10, null=True)
    nws_des = models.CharField(max_length=255, null=True)
    name = models.CharField(max_length=255)
    address = models.TextField(null=True)
    ant_elev = models.FloatField(null=True)
    elev_ft = models.FloatField(null=True)
    geom = models.PointField(null=True)

    def __str__(self):
        return "{0} - {1}".format(self.web_id, self.name)
    

# MODELNAME_TO_GEOMODEL_LOOKUP helps us dynamically select the correct geodata 
# for an observation model, since we don't enforce a relationship between the 
# observation models and sensor layer models
MODELNAME_TO_GEOMODEL_LOOKUP = {
    GarrObservation._meta.object_name: Pixel,
    RtrrObservation._meta.object_name: Pixel,
    GaugeObservation._meta.object_name: Gauge,
    RtrgObservation._meta.object_name: Gauge
}