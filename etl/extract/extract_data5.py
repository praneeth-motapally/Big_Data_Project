from pyspark.sql import SparkSession
from src.utils.logger import get_logger

def extract5(config):
    logger = get_logger("Data Extraction 5")
    logger.info("Starting data extraction for Pipeline 5")
    
    spark = SparkSession.builder.appName("ExtractData5").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    output_path = config['data']['processed_path']
    job_status_df = spark.read.parquet(f"{output_path}/New_Job_Status")
    
    logger.info("Data extraction completed successfully for Pipeline 5")
    return job_status_df
