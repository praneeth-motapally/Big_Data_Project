import requests
import pandas as pd
from io import StringIO
import yaml
import warnings
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utils.logger import get_logger

def extract3(config):
    logger = get_logger("Data Extraction 3")
    logger.info("Data extraction started")
    
    spark = SparkSession.builder.appName("ExtractData3").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    output_path = config['data']['raw_path']
    job_status_df = spark.read.parquet(f"{output_path}/Job_Status")
    owners_df = spark.read.parquet(f"{output_path}/Owners")
    boroughs_df = spark.read.parquet(f"{output_path}/Boroughs")
    
    logger.info("Data extraction completed successfully")
    return job_status_df, owners_df, boroughs_df
