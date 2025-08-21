import yaml
import warnings
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utils.logger import get_logger

logger = get_logger("extract2")

def extract2(config):
    logger.info("Initializing SparkSession for extraction")
    spark = SparkSession.builder.appName("ExtractData").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    output_path = config['data']['raw_path']
    logger.info(f"Reading raw data from {output_path}")

    job_applications_df = spark.read.parquet(f"{output_path}/Job_Applications")
    applicants_df = spark.read.parquet(f"{output_path}/Applicants")
    properties_df = spark.read.parquet(f"{output_path}/Properties")

    logger.info("Data extraction successful for Job_Applications, Applicants, and Properties")

    return job_applications_df, applicants_df, properties_df
