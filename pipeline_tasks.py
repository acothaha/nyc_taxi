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
from google.cloud import bigquery
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

    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{year}-{fmonth}.csv.gz'
    destination = f'{dir_path}/data/raw/{taxi_type}/{year}'

    if os.path.exists(destination + f'/{taxi_type}_tripdata_{year}-{fmonth}.csv.gz'):
        pass
    else:
      _ = aco_lib.download_file(url, destination)


@task(name="Loading green taxi data to local")
def load_green_taxi(spark: SparkSession, dir_path: str, year: int, month: int):
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

    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{year}-{fmonth}.csv.gz'
    destination = f'{dir_path}/data/raw/{taxi_type}/{year}'

    if os.path.exists(destination + f'/{taxi_type}_tripdata_{year}-{fmonth}.csv.gz'):
        pass
    else:
      _ = aco_lib.download_file(url, destination)


@task(name="Loading lookup data to local")
def load_lookup(spark: SparkSession, dir_path: str):
    """
    Function to load the lookup data

    Args:
      - spark (SparkSession): The spark session
      - dir_path (string): It is the path for the main.py
    """

    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    destination = f'{dir_path}/data/lookup'

    _ = aco_lib.download_file('https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv', f'{dir_path}/data/lookup')


@task(name="Reading taxi local CSV files")
def read_taxi_local_csv(spark: SparkSession, dir_path: str, year: int, month: int, n_month: int, taxi_type: str, schema: types.StructType) -> SparkDataFrame:
    
    """
    Function to read local csv

    Args:
      - spark (SparkSession): The spark session
      - dir_path (string): It is the path for the main.py
      - year (int): The year of data taken
      - month (int): The month of data taken
      - months_n (int): The number of months back of data retrieved
      - taxi_type (str): The type of taxi
      - schema (types.StructType): The schema used for the data
    """

    date_lst = aco_lib.get_last_months(n_month, year, month)
    path_lst = [f'{dir_path}/data/raw/{taxi_type}/{date_year}/{taxi_type}_tripdata_{date_year}-{date_month:02d}.csv.gz' for date_year, date_month in date_lst]
  
    df = spark.read \
          .option('header', 'true') \
          .schema(schema) \
          .csv(path_lst)

    return df


@task(name="Reading lookup local CSV files")
def read_lookup_local_csv(spark: SparkSession, dir_path: str) -> SparkDataFrame:
    
    """
    Function to read local csv

    Args:
      - spark (SparkSession): The spark session
      - dir_path (string): It is the path for the main.py
    """

    path = f'{dir_path}/data/lookup/taxi_zone_lookup.csv'
  
    df = spark.read \
          .option('header', 'true') \
          .csv(path)

    return df


@task(name="Saving raw Parquet locally")
def save_raw_parquet_local(spark: SparkSession, dir_path: str, dataframe: SparkDataFrame, year: int, month: int, taxi_type: str, n_partition: int = 12):
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

    destination = f'{dir_path}/data/parquet/{taxi_type}/{year}/{fmonth}/'

    if os.path.exists(destination):
        pass
    else:
        _ = df.write.parquet(destination, compression='gzip', mode='overwrite')


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

@task(name="Joining with lookup data")
def join_lookup(spark: SparkSession, dir_path: str, dataframe: SparkDataFrame, lookup: SparkDataFrame) -> SparkDataFrame:
    """
    Function to read local csv

    Args:
      - spark (SparkSession): The spark session
      - dir_path (string): It is the path for the main.py
      - dataframe (SparkDataFrame): The main dataframe
      - lookup (SparkDataFrame): The lookup dataframe
    """

    df_joined_PU = dataframe.join(lookup, dataframe['PULocationID'] == lookup['LocationID'])

    for i in ['Borough', 'Zone', 'service_zone']:
      df_joined_PU = df_joined_PU.withColumnRenamed(i, 'pickup_' + i)

    df_joined_fin = df_joined_PU.drop('LocationID').join(lookup, df_joined_PU['DOLocationID'] == lookup['LocationID'])

    for i in ['Borough', 'Zone', 'service_zone']:
      df_joined_fin = df_joined_fin.withColumnRenamed(i, 'dropoff_' + i)
    
    _ = df_joined_fin.drop('DOLocationID') \
                     .drop('PULocationID') \
                     .drop('LocationID') \
                     .coalesce(1) \
                     .write.parquet(f'{dir_path}/data/report/fact_trip/', mode='overwrite')

    return df_joined_fin

@task(name="Create revenue zone table")
def revenue_zones(spark: SparkSession, dir_path: str, dataframe: SparkDataFrame) -> SparkDataFrame:
    """
    Function to create a revenue zones table

    Args:
      - spark (SparkSession): The spark session
      - dir_path (string): It is the path for the main.py
      - dataframe (SparkDataFrame): The dataframe that will be wrangled
    """


    dataframe.createOrReplaceTempView('trips_data')

    df_wrangled = spark.sql("""
    SELECT 
        -- Reveneue grouping 
        pickup_Zone AS revenue_zone,
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

    _ = df_wrangled.write.parquet(f'{dir_path}/data/report/revenue_zones/', mode='overwrite')


@task(name="Writing to postgres")
def write_to_postgres(spark: SparkSession, dir_path: str, dataframe: SparkDataFrame) -> SparkDataFrame:
    """
    Function to read local csv

    Args:
      - spark (SparkSession): The spark session
      - dir_path (string): It is the path for the main.py
      - dataframe (SparkDataFrame): The dataframe that will be written to postgres
    """

@task(name="Writing to BigQuery")
def write_to_bigquery(spark: SparkSession, dir_path: str, bq_client, bq_table_id: str):
    """
    Function to read local csv

    Args:
      - spark (SparkSession): The spark session
      - dir_path (string): It is the path for the main.py
      - bq_client: BigQuery client with credentials
      - bq_table_id: BigQuery table id
    """

    path = f'{dir_path}/data/report/revenue_zones/'
    
    for file in os.listdir(path):
      if file.endswith(".parquet"):
        file_path = (os.path.join(path, file))

    
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
    
    with open(file_path, "rb") as file:
      load_job = bq_client.load_table_from_file(
          file, 
          bq_table_id,
          job_config=job_config)
      
    load_job.result()

    destination_table = bq_client.get_table(bq_table_id)

    #Print message
    print(f'loaded {destination_table.num_rows} rows')


