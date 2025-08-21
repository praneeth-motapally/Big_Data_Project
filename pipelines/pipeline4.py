import yaml
from etl.extract import *
from etl.transform import *
from etl.load import *
from src.utils.logger import get_logger

def run_pipeline():
    logger = get_logger("Pipeline 4")
    logger.info("ETL Pipeline 4 started")
    
    with open('D:\\Big_data_project\\config\\config.yaml') as f:
        config = yaml.safe_load(f)
    
    job_status_df, owners_df = extract4(config)
    job_status_df, owners_df = transform4(job_status_df, owners_df)
    load4(job_status_df, owners_df, config)
    
    logger.info("ETL Pipeline 4 ended successfully")

if __name__ == "__main__":
    run_pipeline()
