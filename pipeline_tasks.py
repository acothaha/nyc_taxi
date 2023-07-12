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
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col, size, min as spark_min


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

@task(name="Loading lookup data to local")
def load_lookup(dir_path: str):
    """
    Function to load the lookup data

    Args:
      - dir_path (string): It is the path for the main.py
    """

@task(name="Reading local CSV files")
def read_local_csv(spark: SparkSession, dir_path: str) -> SparkDataFrame:
    """
    Function to read local csv

    Args:
      - spark (SparkSession): The spark session
      - dir_path (string): It is the path for the main.py
    """

@task(name="Saving raw Parquet locally")
def save_parquet_local(spark: SparkSession, dir_path: str, dataframe: SparkDataFrame, n_partition: int):
    """
    Function to read local csv

    Args:
      - spark (SparkSession): The spark session
      - dir_path (string): It is the path for the main.py
      - dataframe (SparkDataFrame): The dataframe that will be saved as parquet locally
      - n_partition (int): number of partition of parquet
    """

@task(name="Joining yellow and green taxi")

@task(name="Conducting Data Wrangling")

@task(name="Joining with lookup data")

@task(name="Writing to postgres")
