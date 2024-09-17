import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from decouple import config
import logging
from dags.logger_config import log_file_path, error_log_file_path

# set up logging
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

error_logger = logging.getLogger("error_logger")
error_logger.setLevel(logging.ERROR)
error_handler = logging.FileHandler(error_log_file_path)
error_logger.addHandler(error_handler)

logger_handler = logging.FileHandler(log_file_path)
logger = logging.getLogger("logger")
logger.addHandler(logger_handler)

# API_KEY = config('API_KEY')
# CITY_NAMES = config('CITY_NAMES')
# COUNTRY_NAMES= config('COUNTRY')
# FIELDS = config('FIELDS')
# WEATHER_FIELDS_EXCLUDE = config('WEATHER_FIELDS_EXCLUDE')
# DATABASE_URL = config('DATABASE_URL')

API_KEY = "dba8d0d78964f7e8c91edf04f94e66b9"
CITY_NAMES = "lagos,ibadan,kano,accra"
COUNTRY_NAMES= "nigeria,ghana"
FIELDS = "name,lat,lon,country,state"
WEATHER_FIELDS_EXCLUDE = "minutely,hourly,daily,alerts"
DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost/weather_db"

def process_var(var:list) -> list:
    """
    Process the environmental variables from a string to a list of strings if 
    there is a comma in the string
    
    Args: var(list) 
    
    returns: list (A list of environmental variables)
    """
    if "," in var:
        return var.split(",")
    
    else:
        return var

CITY_NAMES = process_var(CITY_NAMES)
FIELDS = process_var(FIELDS)
WEATHER_FIELDS_EXCLUDE = process_var(WEATHER_FIELDS_EXCLUDE)
COUNTRY_NAMES = process_var(COUNTRY_NAMES)

print("city_names", CITY_NAMES, type(CITY_NAMES))
print("country_name", COUNTRY_NAMES, type(COUNTRY_NAMES))
print("fields", FIELDS, type(FIELDS))
print("weather_fields_exclude", WEATHER_FIELDS_EXCLUDE, type(WEATHER_FIELDS_EXCLUDE))
