"""
This is the Python module containing the functions for the data pipeline
used for the nyc_taxi project

Author: Abdurrahman Shiddiq Thaha
Date: 12-07-2023
"""

# Importing libraries
import os
import contextlib
import requests
import pandas as pd
import aco_lib
from prefect import task
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window, types
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import lit, udf, col, size, min as spark_min


@contextlib.contextmanager
def get_spark_session(conf: SparkConf):
    """
    Function that is wrapped by context manager

    Args:
      - conf(SparkConf): It is the configuration for the Spark session
    """

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    try:
        yield spark
    finally:
        spark.stop()


@task(name="Loading yellow taxi data to local")
def load_yellow_taxi(spark: SparkSession, dir_path: str, year: int, month: int):
    """
    Function to load the yellow taxi data

    Args:
      - spark (SparkSession): The spark session
      - dir_path (string): It is the path for the main.py
      - year (int): The year of data taken
      - month (int): The month of data taken
    """

    taxi_type = 'yellow'
    fmonth = f'{month:02d}'

    _ = aco_lib.download_file(f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{year}-{fmonth}.csv.gz', f'{dir_path}/data/raw/{taxi_type}/{year}')


@task(name="Loading green taxi data to local")
def load_green_taxi(dir_path: str, year: int, month: int):
    """
    Function to load the green taxi data

    Args:
      - spark (SparkSession): The spark session
      - dir_path (string): It is the path for the main.py
      - year (int): The year of data taken
      - month (int): The month of data taken
    """

    taxi_type = 'green'
    fmonth = f'{month:02d}'

    _ = aco_lib.download_file(f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{year}-{fmonth}.csv.gz', f'{dir_path}/data/raw/{taxi_type}/{year}')


@task(name="Loading lookup data to local")
def load_lookup(dir_path: str):
    """
    Function to load the lookup data

    Args:
      - dir_path (string): It is the path for the main.py
    """

    _ = aco_lib.download_file('https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv', f'{dir_path}/data/lookup')


@task(name="Reading local CSV files")
def read_local_csv(spark: SparkSession, dir_path: str, year: int, month: int, taxi_type: str, schema: types.StructType) -> SparkDataFrame:
    
    """
    Function to read local csv

    Args:
      - spark (SparkSession): The spark session
      - dir_path (string): It is the path for the main.py
      - year (int): The year of data taken
      - month (int): The month of data taken
      - taxi_type (str): The type of taxi
    """

    df = spark.read \
          .option('header', 'true') \
          .schema(schema) \
          .csv(f'{dir_path}/data/raw/{taxi_type}/{year}/{taxi_type}_tripdata_{year}-{month}.csv.gz')

    return df


@task(name="Saving raw Parquet locally")
def save_raw_parquet_local(spark: SparkSession, dir_path: str, dataframe: SparkDataFrame, year: int, month: int, taxi_type: str, n_partition: int = 24):
    """
    Function to read local csv

    Args:
      - spark (SparkSession): The spark session
      - dir_path (string): It is the path for the main.py
      - dataframe (SparkDataFrame): The dataframe that will be saved as parquet locally
      - n_partition (int): number of partition of parquet
      - year (int): The year of data saved
      - month (int): The month of data saved
      - taxi_type (str): The type of taxi
    """

    df = dataframe.repartition(n_partition)
    fmonth = f'{month:02d}'

    _ = df.write.parquet(f'{dir_path}/data/parquet/{taxi_type}/{year}/{fmonth}/', mode='overwrite')


@task(name="Joining yellow and green taxi")
def join_yellow_green(spark: SparkSession, yellow: SparkDataFrame, green: SparkDataFrame) -> SparkDataFrame:
    """
    Function to read local csv

    Args:
      - spark (SparkSession): The spark session
      - yellow (SparkDataFrame): the spark dataframe for yellow taxi data
      - green (SparkDataFrame): the spark dataframe for green taxi data
    """


    df_yellow = yellow \
          .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
          .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

    df_green = green \
          .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
          .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')


    common_columns = ['VendorID',
                      'pickup_datetime',
                      'dropoff_datetime',
                      'store_and_fwd_flag',
                      'RatecodeID',
                      'PULocationID',
                      'DOLocationID',
                      'passenger_count',
                      'trip_distance',
                      'fare_amount',
                      'extra',
                      'mta_tax',
                      'tip_amount',
                      'tolls_amount',
                      'improvement_surcharge',
                      'total_amount',
                      'payment_type',
                      'congestion_surcharge']
    
    df_green_sel = df_green \
          .select(common_columns) \
          .withColumn('service_type', lit('green'))

    df_yellow_sel = df_yellow \
          .select(common_columns) \
          .withColumn('service_type', lit('yellow'))
    
    df_trips_data = df_green_sel.unionAll(df_yellow_sel)

    return df_trips_data


@task(name="Data Wrangling")
def data_wrangling(spark: SparkSession, dataframe: SparkDataFrame) -> SparkDataFrame:
    """
    Function to read local csv

    Args:
      - spark (SparkSession): The spark session
      - dataframe (SparkDataFrame): The dataframe that will be wrangled
    """


    dataframe.createOrReplaceTempView('trips_data')

    df_wrangled = spark.sql("""
    SELECT 
        -- Reveneue grouping 
        PULocationID AS revenue_zone,
        date_trunc('month', pickup_datetime) AS revenue_month, 
        service_type, 

        -- Revenue calculation 
        SUM(fare_amount) AS revenue_monthly_fare,
        SUM(extra) AS revenue_monthly_extra,
        SUM(mta_tax) AS revenue_monthly_mta_tax,
        SUM(tip_amount) AS revenue_monthly_tip_amount,
        SUM(tolls_amount) AS revenue_monthly_tolls_amount,
        SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
        SUM(total_amount) AS revenue_monthly_total_amount,
        SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

        -- Additional calculations
        AVG(passenger_count) AS avg_monthly_passenger_count,
        AVG(trip_distance) AS avg_monthly_trip_distance
    FROM
        trips_data
    GROUP BY
        1, 2, 3
    """)

    return df_wrangled


@task(name="Joining with lookup data")
def data_wrangling(spark: SparkSession, dir_path: str, dataframe: SparkDataFrame, lookup: SparkDataFrame) -> SparkDataFrame:
    """
    Function to read local csv

    Args:
      - spark (SparkSession): The spark session
      - dir_path (string): It is the path for the main.py
      - dataframe (SparkDataFrame): The main dataframe
      - lookup (SparkDataFrame): The lookup dataframe
    """

    df_result_joined = dataframe.join(lookup, dataframe['revenue_zone'] == lookup['LocationID'])

    
    _ = df_result_joined.drop('LocationID').write.parquet(f'{dir_path}/data/report/revenue_zones/')

@task(name="Writing to postgres")
def write_to_postgres(spark: SparkSession, dir_path: str, dataframe: SparkDataFrame) -> SparkDataFrame:
    """
    Function to read local csv

    Args:
      - spark (SparkSession): The spark session
      - dir_path (string): It is the path for the main.py
      - dataframe (SparkDataFrame): The dataframe that will be written to postgres
    """