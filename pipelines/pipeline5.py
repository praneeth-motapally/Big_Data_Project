import yaml
from etl.extract import *
from etl.transform import *
from etl.load import *
from src.utils.logger import get_logger

def run_pipeline():
    logger = get_logger("Pipeline 5")
    logger.info("ETL Pipeline 5 started")
    
    with open('D:\\Big_data_project\\config\\config.yaml') as f:
        config = yaml.safe_load(f)
    
    job_status_df = extract5(config)
    job_status_df = transform5(job_status_df, config)
    load5(job_status_df, config)
    
    logger.info("ETL Pipeline 5 ended successfully")

if __name__ == "__main__":
    run_pipeline()
