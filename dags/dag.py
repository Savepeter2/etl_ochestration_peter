import requests
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dags.database import get_db
from dags.models import Weather
from dags.utils import query_existing_data, retrieve_country_code, retrieve_country_codes, get_data_from_country_code
from dags.config import (API_KEY, CITY_NAMES, 
                        error_logger, logger, 
                        COUNTRY_NAMES, FIELDS,
                        WEATHER_FIELDS_EXCLUDE)
from datetime import datetime
from datetime import datetime, timedelta
from typing import List, Union, Dict
from airflow.decorators import dag, task

@dag(schedule="@daily",
        start_date=datetime(year =2024, month=9, day=16, hour=18, minute=45),
        catchup=False, tags=['weather'],
        max__active_runs=1, render_template_as_native_obj=True) 


@task()
def get_country_code(countries: Union[List[str], str] = 
            "{{ dag_run.conf.get('countries', COUNTRY_NAMES) }}") -> Dict[str, Union[str, List[str]]]:
    
    """
    This function is used to get the country code of the country(s) specified

    Args:
        countries (Union[List[str], str]): List of country names to get the country code for or a single country name
    Returns:
        Dict[str, Union[str, List[str]]]: Dictionary containing status, message, and country code(s)
    """
    if isinstance(countries, str):
        country = countries.capitalize()
        # #print("country", country)
        country_code = retrieve_country_code(country)['country_codes']
        
    elif isinstance(countries, list):
        country_code_list = []
        for country_name in countries:
            country_name = country_name.capitalize()
            country_code = retrieve_country_codes(country_code_list,
                                                    country_name
                                                  )['country_codes']
            country_code_list.append(country_code)

        logger.info({
            "status": "success",
            "message": f"Country codes for {countries} are {country_code_list}",
            "country_codes": country_code_list
        })
        return {
            "status": "success",
            "message": f"Country codes for {countries} are {country_code_list}",
            "country_codes": country_code_list
        }
    
    else:
        error_logger.error({
            "status": "error",
            "message": f"Invalid input type for country name. Expected string but got {type(country)}",
            "error": "Invalid input type"
        })
        return {
            "status": "error",
            "message": f"Invalid input type for country name. Expected string but got {type(country)}",
            "error": "Invalid input type"
        }
             

# country_codes = get_country_code(COUNTRY_NAMES)['country_codes']
# print("country_codes", country_codes)

@task()
def get_current_weather(country_codes: Union[list, str], 
                        city_names: List[str],
                        fields: list) -> dict:
    """
    This function is used to get the current weather of cities in a country
    by using the country code and city names in an API call

    Args:
        country_code(str): Country code of the country
        city_names(list): List of city names to get the weather for
        fields(list): List of fields to be retrieved from the API
    Returns:
        weather(dict): Dictionary containing the weather information,
        for the specified cities, fields and country code
    
    """

    if isinstance(city_names, List):
        city_names = [city.capitalize() for city in city_names]
        if isinstance(country_codes, str):
            country_code = country_codes
            response_list = []
            for city_name in city_names:
                weather_dict = get_data_from_country_code(country_code, city_name, fields)['weather_data']
                response_list.append(weather_dict)

            logger.info({
                "status": "success",
                "message": f"Current weather information for {city_names} has been retrieved from the API",
                "weather_records": response_list
            })

            return {
                "status": "success",
                "message": f"Current weather information for {city_names} has been retrieved from the API",
                "weather_records": response_list
            }
        elif isinstance(country_codes, list):
            response_list = []
            for country_code in country_codes:
                for city_name in city_names:
                    weather_dict = get_data_from_country_code(country_code, city_name, fields, API_KEY)['weather_data']
                    if weather_dict:
                        response_list.append(weather_dict)
                
            logger.info({
                "status": "success",
                "message": f"Current weather information for {city_names} has been retrieved from the API",
                "weather_records": response_list
            })

            return {
                "status": "success",
                "message": f"Current weather information for {city_names} has been retrieved from the API",
                "weather_records": response_list
            }

    else:
        error_logger.error({
            "status": "error",
            "message": f"Invalid input type for city names. Expected list but got {type(city_names)}",
            "error": "Invalid input type"
        })
        return {
            "status": "error",
            "message": f"Invalid input type for city names. Expected list but got {type(city_names)}",
            "error": "Invalid input type"
        }

# weather_response = get_current_weather(country_codes, CITY_NAMES, FIELDS)
# weather_records = weather_response['weather_records']
# print("weather_records", weather_records)

@task()
def retrieve_weather_fields(weather_records: List[dict]) -> List[tuple]:
    """
    This function is used to retrieve the fields of the weather records;
    longitude and latitude of the cities
    city name, country and state of the cities

    Args:
        weather_records(List[dict]): List of dictionaries containing the weather records
    Returns:
        lon_lat(List[tuple]): List of tuples containing the longitude and latitude of the cities
    """
    if isinstance(weather_records, List):
        print("weather_records", weather_records)
        for record in weather_records:
            if list(record.keys()) != ['name', 'lat', 'lon', 'country', 'state']:
                error_logger.error({
                    "status": "error",
                    "message": f"Invalid keys in the weather records. Expected keys are ['name', 'lat', 'lon', 'country', 'state']",
                    "error": "Invalid keys"
                })
                return {
                    "status": "error",
                    "message": f"Invalid keys in the weather records. Expected keys are ['name', 'lat', 'lon', 'country', 'state']",
                    "error": f"Invalid keys: {list(record.keys())}"
                }
        
        lon_lat = [(round(record['lon'], 2), round(record['lat'], 2)) for record in weather_records]
        
        weather_fields = []
        for record in weather_records:
            print("record", record)
            fields_dict = {}
            fields_dict['city'] = record['name']
            fields_dict['country'] = record['country']
            fields_dict['state'] = record['state']
            weather_fields.append(fields_dict)
        
        weather_fields_dict = {
            "weather_fields": weather_fields,
            "lon_lat": lon_lat
        }

        logger.info({
            "status": "success",
            "message": f"Fields of the weather records have been retrieved",
            "weather_fields": weather_fields_dict
        })
        return {
            "status": "success",
            "message": f"Fields of the weather records have been retrieved",
            "weather_fields": weather_fields_dict
        }


    else:
        error_logger.error({
            "status": "error",
            "message": f"Invalid input type for weather records. Expected list but got {type(weather_records)}",
            "error": "Invalid input type"
        })
        return {
            "status": "error",
            "message": f"Invalid input type for weather records. Expected list but got {type(weather_records)}",
            "error": "Invalid input type"
        }

# weather_fields = retrieve_weather_fields(weather_records)
# weather_fields_dict = weather_fields['weather_fields']
# lon_lat = weather_fields_dict['lon_lat']
# weather_fields = weather_fields_dict['weather_fields']
# print("weather_fields", weather_fields)

@task()
def get_current_weather(
    lon_lat: List[tuple],
    fields: List[str],
    weather_fields: List[dict]
) -> List[dict]:
    
    """
    This function is used to get the current weather of cities in a country
    by using the longitude and latitude of the cities in an API call. 
    It also joins the weather information from the API with the country 
    and state information retrieved earlier

    Args:
        lon_lat(List[tuple]): List of tuples containing the longitude and latitude of the cities
        fields(list): List of fields to be retrieved from the API
        other_list(List[dict]): List of dictionaries containing names of country, state about the cities
    Returns:
        weather(dict): Dictionary containing the weather information,
        for the specified cities, fields and country code
    
    """
    if isinstance(lon_lat, List) and isinstance(fields, List):
        response_list = []
        record_counter = 0
        for lon, lat in lon_lat:
            try:
                url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={WEATHER_FIELDS_EXCLUDE}&appid={API_KEY}"
                response = requests.get(url)
                data = response.json()

                #print("data", data)

                weather_dict = {}
                for key in data.keys():
                    weather_dict[key] = data[key] 
                
                weather_dict['city'] = weather_fields[record_counter]['city']
                weather_dict['country'] = weather_fields[record_counter]['country']
                weather_dict['state'] = weather_fields[record_counter]['state']
                record_counter += 1
                
                response_list.append(weather_dict)
            
            except Exception as e:
                error_logger.error({
                    "status": "error",
                    "message": f"Unable to get weather information for {lon_lat} from the API",
                    "error": str(e)
                })
                return {
                    "status": "error",
                    "message": f"Unable to get weather information for {lon_lat} from the API",
                    "error": str(e)
                }
        
        logger.info({
            "status": "success",
            "message": f"Current weather information for {lon_lat} has been retrieved from the API",
            "weather_records": response_list
        })
        return {
            "status": "success",
            "message": f"Current weather information for {lon_lat} has been retrieved from the API",
            "weather_records": response_list
        }
    else:
        error_logger.error({
            "status": "error",
            "message": f"Invalid input type for lon_lat. Expected list but got {type(lon_lat)}",
            "error": "Invalid input type"
        })
        return {
            "status": "error",
            "message": f"Invalid input type for lon_lat. Expected list but got {type(lon_lat)}",
            "error": "Invalid input type"
        }

# weather_response = get_current_weather(lon_lat, FIELDS, weather_fields)
# weather_records = weather_response['weather_records']
# #print("weather_records", weather_records)


@task()
def transform_weather_records(weather_records: List[dict]) -> List[dict]:
    """
    This function is used to transform the weather records into a 
    a more structured list of dictionaries, with only the necessary fields.
    Each dictionary in the list contains the weather information for each city specified in the API call.
    The datetime field is also converted from a unix timestamp to a datetime object.

    Args:
        weather_records(List[dict]): List of dictionaries containing the weather records
    Returns:
        List[dict]: List of dictionaries containing the transformed weather records
    """
    transformed_weather_records = []
    for record in weather_records:
        transformed_record = {}
        transformed_record['city'] = record['city']
        transformed_record['country'] = record['country']
        transformed_record['state'] = record['state']
        transformed_record['latitude'] = record['lat']
        transformed_record['longitude'] = record['lon']
        transformed_record['timezone'] = record['timezone']
        transformed_record['timezone_offset'] = record['timezone_offset']
        date_time = record['current']['dt']
        date_time = datetime.fromtimestamp(date_time).strftime('%Y-%m-%d %H:%M:%S')
        transformed_record['date_time'] = date_time
        transformed_record['sunrise'] = record['current']['sunrise']
        transformed_record['sunset'] = record['current']['sunset']
        transformed_record['temp'] = record['current']['temp']
        transformed_record['feels_like'] = record['current']['feels_like']
        transformed_record['pressure'] = record['current']['pressure']
        transformed_record['humidity'] = record['current']['humidity']
        transformed_record['dew_point'] = record['current']['dew_point']
        transformed_record['ultraviolet_index'] = record['current']['uvi']
        transformed_record['clouds'] = record['current']['clouds']
        transformed_record['visibility'] = record['current']['visibility']
        transformed_record['wind_speed'] = record['current']['wind_speed']
        transformed_record['wind_deg'] = record['current']['wind_deg']
        transformed_record['weather'] = record['current']['weather'][0]['main']
        transformed_record['description'] = record['current']['weather'][0]['description']
        transformed_weather_records.append(transformed_record)
    
    return transformed_weather_records

# transformed_weather_records = transform_weather_records(weather_records)
# print("transformed_weather_records", transformed_weather_records)


@task()
def load_records_to_database(weather_data: List[dict]):
    """
    This function is used to load the transformed weather records into the postgres database

    Args:
        data(List[dict]): List of dictionaries containing the transformed weather records
    """

    try:
        with get_db() as db:
            data = query_existing_data(Weather, weather_data, db)
            existing_ids = data['existing_ids']
            record_list = data['record_list']
            print("record_list", record_list)

            no_data = 0
            for record in record_list:
                if record['id'] not in existing_ids:
                    weather = Weather(
                        city=record['city'],
                        country=record['country'],
                        state=record['state'],
                        latitude=record['latitude'],
                        longitude=record['longitude'],
                        timezone=record['timezone'],
                        timezone_offset=record['timezone_offset'],
                        date_time=record['date_time'],
                        sunrise=record['sunrise'],
                        sunset=record['sunset'],
                        temperature=record['temp'],
                        feels_like=record['feels_like'],
                        pressure=record['pressure'],
                        humidity=record['humidity'],
                        dew_point=record['dew_point'],
                        ultraviolet_index=record['ultraviolet_index'],
                        clouds=record['clouds'],
                        visibility=record['visibility'],
                        wind_speed=record['wind_speed'],
                        wind_deg=record['wind_deg'],
                        weather=record['weather'],
                        description=record['description']
                    )
                    db.add(weather)
                    no_data += 1
            db.commit()
            print(f"{no_data} weather records have been loaded to the database")
            logger.info({
                "status": "success",
                "message": f"{no_data} weather records have been loaded to the database",
                "weather_records": record_list
            })
            return {
                "status": "success",
                "message": f"{no_data} weather records have been loaded to the database",
                "weather_records": record_list
            }
    except Exception as e:
        error_logger.error({
            "status": "error",
            "message": "Unable to load weather records to the database",
            "error": str(e)
        })
        return {
            "status": "error",
            "message": "Unable to load weather records to the database",
            "error": str(e)
        }

# print(load_records_to_database(transformed_weather_records))

if __name__ == "main":
    # country_codes = get_country_code(COUNTRY_NAMES)['country_codes']
    country_codes = get_country_code()['country_codes']
    weather_response = get_current_weather(country_codes, CITY_NAMES, FIELDS)
    weather_records = weather_response['weather_records']
    weather_fields = retrieve_weather_fields(weather_records)
    weather_fields_dict = weather_fields['weather_fields']
    lon_lat = weather_fields_dict['lon_lat']
    weather_fields = weather_fields_dict['weather_fields']
    weather_response = get_current_weather(lon_lat, FIELDS, weather_fields)
    weather_records = weather_response['weather_records']
    transformed_weather_records = transform_weather_records(weather_records)
    load_records_to_database(transformed_weather_records)

