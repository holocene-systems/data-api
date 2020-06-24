"""mixins.py

generic model mixins for the django-orm

"""

import pandas as pd
from django.contrib.gis.db import models
from django.core import serializers
from django.utils import timezone


class TimestampedMixin(models.Model):
    """Provides auto-populating "created" and "modified" fields
    """
    class Meta:
        abstract = True

    created = models.DateTimeField(editable=False, default=timezone.now)
    modified = models.DateTimeField(default=timezone.now)

    def save(self, *args, **kwargs):
        ''' On save, update timestamps '''
        if not self.id:
            self.created = timezone.now()
        self.modified = timezone.now()
        return super().save(*args, **kwargs)


class PandasModelMixin(models.Model):
    class Meta:
        abstract = True

    @classmethod
    def as_dataframe(cls, queryset=None, field_list=None):

        if queryset is None:
            queryset = cls.objects.all()
        if field_list is None:
            field_list = [_field.name for _field in cls._meta._get_fields(reverse=False)]

        data = []
        [data.append([obj.serializable_value(column) for column in field_list]) for obj in queryset]

        columns = field_list

        df = pd.DataFrame(data, columns=columns)

        return df

    @classmethod
    def as_dataframe_using_django_serializer(cls, queryset=None):

        if queryset is None:
            queryset = cls.objects.all()

        if queryset.exists():
            serialized_models = serializers.serialize(format='python', queryset=queryset)
            serialized_objects = [s['fields'] for s in serialized_models]
            data = [x.values() for x in serialized_objects]

            columns = serialized_objects[0].keys()

            df = pd.DataFrame(data, columns=columns)
        df = pd.DataFrame()

        return df

    @classmethod
    def as_dataframe_using_drf_serializer(cls, queryset=None, drf_serializer=None, field_list=None):
        from rest_framework import serializers


        if queryset is None:
            queryset = cls.objects.all()

        if drf_serializer is None:
            class CustomModelSerializer(serializers.ModelSerializer):
                class Meta:
                    model = cls
                    fields = field_list or '__all__'

            drf_serializer = CustomModelSerializer

        serialized_objects = drf_serializer(queryset, many=True).data
        data = [x.values() for x in serialized_objects]

        columns = drf_serializer().get_fields().keys()

        df = pd.DataFrame(data, columns=columns)

        return df

    @classmethod
    def as_geojson_using_drfg_serializer(cls, queryset=None, drfg_serializer=None, field_list=None, geo_field="geom"): #, id_field=None):
        from rest_framework import serializers

        if queryset is None:
            queryset = cls.objects.all()

        if drfg_serializer is None:
            class CustomModelSerializer(GeoFeatureModelSerializer):
                class Meta:
                    model = cls
                    fields = field_list or '__all__'
                    geo_field = geo_field

            drfg_serializer = CustomModelSerializer

        table_as_geojson_odicts = drfg_serializer(queryset, many=True).data

        return json.loads(json.dumps(table_as_geojson_odicts))

    @classmethod
    def as_dataframe_from_raw_query(cls, sql):
        r = cls.objects.raw(sql)
        cols = r.columns
        # convert the response to a list of dictionaries
        data = [{k: t.__dict__[k] for k in t.__dict__.keys() & set(cols)} for t in r]
        # return as a dataframe
        return pd.DataFrame(data, columns=cols)
