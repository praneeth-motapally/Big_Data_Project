import requests
import pandas as pd
from io import StringIO
import warnings
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utils.logger import get_logger

def transform1(df):
    logger = get_logger("Transform")
    logger.info("Transformation started")

    df = df.withColumnRenamed("ExistingNo. of Stories", "ExistingNo_of_Stories")
    df = df.withColumnRenamed("Proposed No. of Stories", "ProposedNo_of_Stories")
    df = df.withColumnRenamed("Total Est. Fee", "Total_Est_Fee")
    logger.info("Standardized key column names")

    unwanted_col=['Boiler', 'Fuel Burning', 'Fuel Storage', 'Standpipe', 'Fire Alarm', 'Fire Suppression', 'Curb Cut', 'Horizontal Enlrgmt', 'Vertical Enlrgmt', 'Zoning Dist3', 'Special District 2', "Owner's House Number", "Owner'sHouse Street Name", 'City ', 'State', 'Zip']
    col_removed=df.select([x for x in df.columns if x not in unwanted_col])
    logger.info("Removed unwanted columns")

    logger.info("Transformation completed")
    return col_removed
