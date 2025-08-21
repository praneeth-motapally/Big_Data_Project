import yaml
from etl.extract import *
from etl.transform import *
from etl.load import *
from src.utils.logger import get_logger

logger = get_logger("pipeline2")

def run_pipeline():
    logger.info("Pipeline 2 execution started")
    with open('D:\\Big_data_project\\config\\config.yaml') as f:
        config = yaml.safe_load(f)

    logger.info("Starting extraction step")
    job_applications_df, applicants_df, properties_df = extract2(config)

    logger.info("Starting transformation step")
    job_applications_df, applicants_df, properties_df = transform2(job_applications_df, applicants_df, properties_df)

    logger.info("Starting load step")
    load2(job_applications_df, applicants_df, properties_df, config)

    logger.info("Pipeline 2 execution completed successfully")

if __name__ == "__main__":
    run_pipeline()
