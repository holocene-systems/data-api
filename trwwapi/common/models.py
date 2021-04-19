"""Common data models and serializers
"""
from typing import List
# import pint
from dataclasses import dataclass, field, asdict
from django.db.models.aggregates import Sum
from marshmallow import Schema, fields, EXCLUDE, pre_load
from marshmallow_dataclass import class_schema
from marshmallow_dataclass import dataclass as mdc

@mdc
class TrwwApiResponseSchema:
    """Implements a consistent JSON response format. Inspired by AWS API Gateway 
    and [JSend](https://github.com/omniti-labs/jsend)).

    This uses marshmallow-dataclass to automatically get a schema for serialization.
    
    ```python
    >>> r = ResponseSchema(args={},meta={},data={}) # (with some actual parameters)
    >>> print(ResponseSchema.Schema().dump(r))
    >>> {args:{}, meta: {}, data: {}}
    ```

    When used with the Django Rest Framework, the dumped output is passed to the 'data' 
    argument of the Response object.
    """

    # request args **as parsed by the API**, defaults to {}
    args: dict = field(default_factory=dict)
    # TODO: add "datetime_encoder(args) if args"
    # contains job metadata and post-processing stats, including an auto-calc'd row count if response_data is parsed; defaults to None
    meta: dict = field(default_factory=dict)
    # any data returned by the API call. If the call returns no data, defaults to None
    data: dict = None
    # message, one of [queued, started, deferred, finished, failed]; defaults to success
    status: str = 'success'
    # http status code, defaults to 200
    status_code: int = 200 
    # list of messages
    messages: List[str] = field(default_factory=list)