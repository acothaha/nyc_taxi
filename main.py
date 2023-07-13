"""
This is the main module that is used for orchestrating the data pipeline
for the nyc_taxi project

Author: Abdurrahman Shiddiq Thaha
Date: 13-07-2023
"""

# Importing libraries
import os
import argparse
import datetime
from multiprocessing import cpu_count
from prefect import flow
from pyspark import SparkConf
from pyspark.sql import types
from aco_lib import download_file, get_last_months
from pipeline_tasks import (get_spark_session,
                            load_yellow_taxi,
                            load_green_taxi,
                            load_lookup,
                            read_taxi_local_csv,
                            read_lookup_local_csv,
                            save_raw_parquet_local,
                            join_yellow_green,
                            data_wrangling,
                            join_lookup,
                            write_to_postgres,
                            write_to_bigquery)

# Current directory
dir_path = os.path.dirname(os.path.realpath(__file__))


@flow(name="NYC_Taxi Data Pipeline Flow")
def data_pipeline(n_month, year, month, schema_yellow, schema_green):
    '''
    Function wrapped to be used as the Prefect main flow
    '''

    # CPU numbers
    n_cpus = cpu_count()
    n_executors = n_cpus - 1
    n_cores = 4
    n_max_cores = n_executors * n_cores

    # Add additional spark configurations
    conf = SparkConf().setMaster(f'local[{n_cpus}]').setAppName("Medium post")
    conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    conf.set("parquet.enable.summary-metadata", "false")
    conf.set("spark.sql.broadcastTimeout",  "3600")
    conf.set("spark.sql.autoBroadcastJoinThreshold",  "1073741824")
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.sql.debug.maxToStringFields", "100")
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.driver.memory", "10g")
    conf.set("spark.executor.cores", str(n_cores))
    conf.set("spark.cores.max", str(n_max_cores))
    conf.set("spark.storage.memoryFraction", "0")
    conf.set("spark.driver.maxResultSize", "8g")
    conf.set("spark.files.overwrite","true")

    # Setting up the Spark cluster
    with get_spark_session(conf=conf) as spark_session:


        for y, m in get_last_months(n_month, year, month):
            _ = load_yellow_taxi(spark=spark_session,
                                 dir_path=dir_path,
                                 year=y,
                                 month=m)
            
            _ = load_green_taxi(spark=spark_session,
                                dir_path=dir_path,
                                year=y,
                                month=m)
            

        _ = load_lookup(spark=spark_session,
                        dir_path=dir_path)
        
        
        df_yellow = read_taxi_local_csv(spark=spark_session,
                                        dir_path=dir_path,
                                        year=year,
                                        month=month,
                                        n_month=n_month,
                                        taxi_type='yellow',
                                        schema=schema_yellow)
        

        df_green = read_taxi_local_csv(spark=spark_session,
                                       dir_path=dir_path,
                                       year=year,
                                       month=month,
                                       n_month=n_month,
                                       taxi_type='green',
                                       schema=schema_green)
        

        for taxi_type, df in zip(['yellow', 'green'], [df_yellow, df_green]):
            for y, m in get_last_months(n_month, year, month):
                _ = save_raw_parquet_local(spark=spark_session,
                                           dir_path=dir_path,
                                           dataframe=df,
                                           year=y,
                                           month=m,
                                           taxi_type=taxi_type,
                                           )
        

        df_trips_data = join_yellow_green(spark=spark_session,
                                          yellow=df_yellow,
                                          green=df_green,
                                          )
                

        df_wrangled = data_wrangling(spark=spark_session,
                                     dataframe=df_trips_data)
        

        
        df_lookup = read_lookup_local_csv(spark=spark_session,
                                          dir_path=dir_path)


        df_joined = join_lookup(spark=spark_session,
                                dir_path=dir_path,
                                dataframe=df_wrangled,
                                lookup=df_lookup
                                )

        
if __name__ == '__main__':

    # Input argument to change the flow parameter
    parser = argparse.ArgumentParser()
    parser.add_argument('--n_month', '-nm',
                        help='Number of month(s) of data retrieved backward',
                        type=int,
                        required=True,
                        default=6)
    parser.add_argument('--year', '-y',
                        help='Year of latest data retrieved',
                        type=int,
                        required=True,
                        default=int(datetime.date.today().strftime("%Y")))
    parser.add_argument('--month', '-t',
                        help='Month of latest data retrieved',
                        type=int,
                        required=True,
                        default=int(datetime.date.today().strftime("%m")))
    args = parser.parse_args()

    schema_yellow = types.StructType([
                    types.StructField('VendorID', types.IntegerType(), True), 
                    types.StructField('tpep_pickup_datetime', types.TimestampType(), True), 
                    types.StructField('tpep_dropoff_datetime', types.TimestampType(), True), 
                    types.StructField('passenger_count', types.IntegerType(), True), 
                    types.StructField('trip_distance', types.DoubleType(), True), 
                    types.StructField('RatecodeID', types.IntegerType(), True), 
                    types.StructField('store_and_fwd_flag', types.StringType(), True), 
                    types.StructField('PULocationID', types.IntegerType(), True), 
                    types.StructField('DOLocationID', types.IntegerType(), True), 
                    types.StructField('payment_type', types.IntegerType(), True), 
                    types.StructField('fare_amount', types.DoubleType(), True), 
                    types.StructField('extra', types.DoubleType(), True), 
                    types.StructField('mta_tax', types.DoubleType(), True), 
                    types.StructField('tip_amount', types.DoubleType(), True), 
                    types.StructField('tolls_amount', types.DoubleType(), True), 
                    types.StructField('improvement_surcharge', types.DoubleType(), True), 
                    types.StructField('total_amount', types.DoubleType(), True), 
                    types.StructField('congestion_surcharge', types.DoubleType(), True)])
    
    schema_green = types.StructType([
                    types.StructField('VendorID', types.IntegerType(), True), 
                    types.StructField('lpep_pickup_datetime', types.TimestampType(), True), 
                    types.StructField('lpep_dropoff_datetime', types.TimestampType(), True), 
                    types.StructField('store_and_fwd_flag', types.StringType(), True), 
                    types.StructField('RatecodeID', types.IntegerType(), True), 
                    types.StructField('PULocationID', types.IntegerType(), True), 
                    types.StructField('DOLocationID', types.IntegerType(), True), 
                    types.StructField('passenger_count', types.IntegerType(), True), 
                    types.StructField('trip_distance', types.DoubleType(), True), 
                    types.StructField('fare_amount', types.DoubleType(), True), 
                    types.StructField('extra', types.DoubleType(), True), 
                    types.StructField('mta_tax', types.DoubleType(), True), 
                    types.StructField('tip_amount', types.DoubleType(), True), 
                    types.StructField('tolls_amount', types.DoubleType(), True), 
                    types.StructField('ehail_fee', types.DoubleType(), True), 
                    types.StructField('improvement_surcharge', types.DoubleType(), True), 
                    types.StructField('total_amount', types.DoubleType(), True), 
                    types.StructField('payment_type', types.IntegerType(), True), 
                    types.StructField('trip_type', types.IntegerType(), True), 
                    types.StructField('congestion_surcharge', types.DoubleType(), True)])




    # Running the main flow
    data_pipeline(n_month=args.n_month,
                  year=args.year,
                  month=args.month,
                  schema_yellow=schema_yellow,
                  schema_green=schema_green)
