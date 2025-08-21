from pyspark.sql import SparkSession
from src.utils.logger import get_logger

def extract4(config):
    logger = get_logger("Data Extraction 4")
    logger.info("Data extraction started")
    
    spark = SparkSession.builder.appName("ExtractData4").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    output_path = config['data']['processed_path']
    job_status_df = spark.read.parquet(f"{output_path}/Job_Status")
    owners_df = spark.read.parquet(f"{output_path}/Owners")
    
    logger.info("Data extraction completed successfully")
    return job_status_df, owners_df
