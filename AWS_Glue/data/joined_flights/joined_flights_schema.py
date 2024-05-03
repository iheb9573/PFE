from typing import Tuple

from pyspark.sql.types import StringType, StructField, StructType, TimestampType, DateType, IntegerType, DoubleType, FloatType



# definition des champs du dataframe
AIRCRAFT: str = "aircraft"
AIRPORT: str = "airport"
ROUNDED_HOUR: str = "rounded_hour"
DATE: str = "date"
FROM: str = "from"
TO: str = "to"
FLIGHT: str = "flight"
FLIGHT_TIME: str = "flight_time"
SCHEDULED_TIME_DEPARTURE: str = "scheduled_time_departure"
ACTUAL_TIME_DEPARTURE: str = "actual_time_departure"
SCHEDULED_TIME_ARRIVAL: str = "scheduled_time_arrival"
STATUS: str = "status"
ACTUAL_TIME_ARRIVAL: str = "actual_time_arrival"
FROM_CITY: str = "from_city"
TO_CITY: str = "to_city" 
DELAY_TIME: str = "delay_time"
TEMPERATURE: str = "temperature"
DEW_POINT: str = "dew_point" 
HUMIDITY: str = "humidity" 
WIND_SPEED: str = "wind_speed"
WIND_GUST: str = "wind_gust" 
PRESSURE: str = "pressure" 
PRECIP: str = "precip" 
MY_FLIGHTRADAR24_RATING: str = "my_flightradar24_rating"
ARRIVAL_DELAY_INDEX: str = "arrival_delay_index"
DEPARTURE_DELAY_INDEX: str = "departure_delay_index" 
MSN: str = "msn"
TYPE: str = "type" 
AIRLINE: str = "airline" 
AGE: str = "age" 

# definition du prefie du chemin des vols 
PREFIX_PATH_FLIGHTS: str = (
    ""
)

'''
PREFIX_PATH_FLIGHTS: str = (
    "CORPORATE/COMPTOIR_CBSFINANCEMENT/MOM/V2/FIAG_FIPL_ATTACHED/eventdate="
)
'''

# definition du shema du dataframe
JOINED_FLIGHTS_SCHEMA: StructType = StructType(
    [
        StructField(AIRCRAFT, StringType()),
        StructField(AIRPORT, StringType()),
        StructField(ROUNDED_HOUR, TimestampType()),
        StructField(DATE, DateType()),
        StructField(FROM, StringType()),
        StructField(TO, StringType()),
        StructField(FLIGHT, StringType()),
        StructField(FLIGHT_TIME, TimestampType()),
        StructField(SCHEDULED_TIME_DEPARTURE, TimestampType()),
        StructField(ACTUAL_TIME_DEPARTURE, TimestampType()),
        StructField(SCHEDULED_TIME_ARRIVAL, TimestampType()),
        StructField(STATUS, StringType()),
        StructField(ACTUAL_TIME_ARRIVAL, TimestampType()),
        StructField(FROM_CITY, StringType()),
        StructField(TO_CITY, StringType()),
        StructField(DELAY_TIME, DoubleType()),
        StructField(TEMPERATURE, DoubleType()),
        StructField(DEW_POINT, DoubleType()),
        StructField(HUMIDITY, DoubleType()),
        StructField(WIND_SPEED, DoubleType()),
        StructField(WIND_GUST, DoubleType()),
        StructField(PRESSURE, DoubleType()),
        StructField(PRECIP, DoubleType()),
        StructField(MY_FLIGHTRADAR24_RATING, IntegerType()),
        StructField(ARRIVAL_DELAY_INDEX, FloatType()),
        StructField(DEPARTURE_DELAY_INDEX, FloatType()),
        StructField(MSN, StringType()),
        StructField(TYPE, StringType()),
        StructField(AIRLINE, StringType()),
        StructField(AGE, IntegerType())
    ]
)

