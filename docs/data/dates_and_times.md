# Dates and Times

The sources of rainfall data we use all come with different representations of dates, times, and timezones:

* Datawise real-time rain gauge data has a Pacific timezone
* Vieux real-time radar and calibrated radar data has an Eastern timezone
* 3RWW/ALCOSAN calibrated gauge data has no timezone, but is local (Eastern) time.
* All the legacy rainfall data (migrated from Teragon in 2019) did not have a timezone, but is local (Eastern) time.

The data pipelines handle these sources accordingly, ultimately ensuring everything is converted to the appropriate local timezone--which we can do since this data is all geographically specific to Allegheny County, PA. We trade in ISO-8061 standard-formatted timestamps that include the timezone in the software; in storage, the medium has dictated whether the timezone offset is included.

Our intermediate + long-term storage in AWS S3 stores all date times in the local time, but not with a timezone offest.

When data is pass through S3 to the PostgreSQL database, the datetimes are
(again) given the local timezone. PostgreSQL (by default) converts any timestamp
that has a timezone to UTC:

> For timestamp with time zone, the internally stored value is always in UTC (Universal Coordinated Time, traditionally known as Greenwich Mean Time, GMT). An input value that has an explicit time zone specified is converted to UTC using the appropriate offset for that time zone. If no time zone is stated in the input string, then it is assumed to be in the time zone indicated by the system's TimeZone parameter, and is converted to UTC using the offset for the timezone zone.

The API then converts this back to Eastern timezone.