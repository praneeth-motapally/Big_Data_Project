import yaml
from etl.extract import *
from etl.transform import *
from etl.load import *
from src.utils.logger import get_logger

def run_pipeline():
    logger = get_logger("Pipeline1")
    logger.info("ETL Pipeline 1 started")

    with open('D:\\Big_data_project\\config\\config.yaml') as f:
        config = yaml.safe_load(f)
    logger.info("Configuration loaded successfully")

    raw_df = extract1(config)
    transformed_df = transform1(raw_df)
    load1(transformed_df, config)

    logger.info("ETL Pipeline 1 ended")

if __name__ == "__main__":
    run_pipeline()
