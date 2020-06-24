from os import environ
from pathlib import Path
from os.path import abspath
from dateutil import tz
from pytz import timezone
from .utils import get_envvar_w_fallback

SCRIPT_DIR = Path(abspath(__file__)).parent
DATA_DIR = SCRIPT_DIR / 'data'

# environment variables for cloud resources
AWS_REGION = get_envvar_w_fallback('AWS_REGION', 'us-east-2')
TARGET_TABLE_GARR15 = get_envvar_w_fallback('TABLE_GARR15', 'trww-rainfall-prod-3rww-GARR15')
TARGET_TABLE_GAUGE15 = get_envvar_w_fallback('TABLE_Gauge15', 'trww-rainfall-prod-3rww-Gauge15')
TARGET_TABLE_RTRR15 = get_envvar_w_fallback('TABLE_RTRR15', 'trww-rainfall-prod-3rww-RTRR15')

REF_DOCUMENTATION_URL = environ['URL_DOCS'] if 'URL_DOCS' in environ.keys() else 'https://3rww.github.io/api-docs/#rainfall-api'

# last item in API resource path, used for lookups and determining arg parsing params/methods
RAINGAUGE_RESOURCE_PATH = "raingauge"
PIXEL_RESOURCE_PATH = "pixel"
RTRR_RESOURCE_PATH = "realtime"

# constants for parsing API request args
DELIMITER = ","
PIXEL_DELIMITER = ","
GAUGE_DELIMITER = ","
INTERVAL_15MIN = "15-minute"
INTERVAL_HOURLY = "hourly"
INTERVAL_DAILY = "daily"
INTERVAL_MONTHLY = "monthly"
INTERVAL_SUM = "total"
INTERVAL_TRUTHS = [INTERVAL_15MIN, INTERVAL_HOURLY, INTERVAL_DAILY, INTERVAL_MONTHLY, INTERVAL_SUM]
ZEROFILL_TRUTHS = ['yes', 'true', '1', 'zerofill']

# configure timezone objects
TZ_STRING="America/New_York"
TZ = timezone(TZ_STRING)
TZI = tz.gettz(TZ_STRING)
TZINFOS = {'EDT': TZI, 'EST': TZI}

# Constants for data storage and attributes
TIMESTAMP_FIELD = 'Timestamp'
RAINFALL_FIELD = 'Rainfall (in)'
RAINFALL_NODATA_STRING = "N/D"
SOURCE_FIELD = 'Source'
ID_FIELD = 'SID'
INTERVAL_FIELD = 'Interval'
INTERVAL_15MIN = "15min"
INTERVAL_5MIN = "5min"
SENSOR_PIXELS = 'pixels'
SENSOR_GAUGES = 'gauges'
STATUS_CALIBRATED = 'calibrated'
STATUS_REALTIME = 'realtime'

# JSEND HTTP RESPONSE MESSAGE LOOKUP
JSEND_CODES = {
    200: 'success',
    400: 'fail',
    500: 'error'
}

# RESPONSE FORMAT TYPES
F_JSON = ['json', 'time', 'sensor']
F_GEOJSON = ['geo', 'geojson']
F_MD = [] #['md', 'markdown']
F_CSV = ['csv']
F_ARRAYS = ['arrays']
F_ALL = F_MD + F_CSV + F_GEOJSON + F_JSON + F_ARRAYS