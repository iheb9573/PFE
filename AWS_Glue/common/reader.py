#import awswrangler as wr # type: ignore
from typing import List, Set
import boto3 # type: ignore
from pyspark.sql import DataFrame # type: ignore
import pandas as pd # type: ignore
#import awswrangler as wr # type: ignore
import os
import boto3
import pandas as pd
from io import StringIO
from pyspark.sql import SparkSession
 
from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.types import StructType # type: ignore
from pyspark.sql import SparkSession # type: ignore
# from ..config.config import app_config
 
def read_from_csv_with_header(s3_bucket: str, s3_key: str):
    """Read a csv file from S3 and ignore the header."""
    # Create a client for S3    
    s3 = boto3.client('s3')
       
    # Get the CSV file object from S3    
    csv_obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)    
 
    # Read the CSV file object    
    csv_string = csv_obj['Body'].read().decode('utf-8')
   
    # Create a pandas DataFrame from the CSV string
    pd_df = pd.read_csv(StringIO(csv_string))
   
    # Create a Spark DataFrame from the pandas DataFrame
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(pd_df)
   
    return df
 
'''
df = read_from_csv_with_header('s3a://flights-delay-prediction-jihed/data/history/flights_csv/flights.csv')
 
df.show(10)
'''
 
read_from_csv_with_header('flights-delay-prediction-pfe-iheb' , 'data_flightRadar24/weather_csv_file/weather.csv').show(50)