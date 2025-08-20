import yaml
from etl.extract import *
from etl.transform import *
from etl.load import *
from src.utils.logger import get_logger

def run_pipeline():
    logger = get_logger("checking")
    logger.info("ETL pipeline started")
    a=10
    b=10
    c=a+b
    logger.info("ETL pipeline ended")
    logger = get_logger("check2")
    logger.info("check 2 ETL pipeline started")

if __name__ == "__main__":
    run_pipeline()

