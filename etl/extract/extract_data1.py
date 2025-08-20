import requests
import pandas as pd
from io import StringIO
import yaml
import warnings
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utils.logger import get_logger

# Below code is for data extraction from URL
'''
def data_extraction_url:
    warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)

    BASE_URL = "https://data.cityofnewyork.us/resource/ic3t-wcy2.csv"
    LIMIT = 50000
    TOTAL_ROWS = 2710000

    dfs = []
    for offset in range(0, TOTAL_ROWS, LIMIT):
        url = f"{BASE_URL}?$limit={LIMIT}&$offset={offset}"
        csv = requests.get(url).text
        df = pd.read_csv(StringIO(csv))
        dfs.append(df)
    full_df = pd.concat(dfs)
    full_df.to_csv("D:\\Big_data_project\\data\\raw\\nyc_data.csv", index=False)

    dtype_mapping = {
    'Block': str,
    # Add other columns from the DtypeWarning here if needed
    #'Column_Name_From_Warning': str
    }

    df = pd.read_csv("D:\\Big_data_project\\data\\raw\\nyc_data.csv", dtype=dtype_mapping, low_memory=False)
    df.to_parquet("D:\\Big_data_project\\data\\Big_data\\Job_Application_Filings_rawp.parquet")
'''
def change_col_name(df):
    source_columns=['Job #', 'Doc #', 'Borough', 'House #', 'Street Name', 'Block', 'Lot', 'Bin #', 'Job Type', 'Job Status', 'Job Status Descrp', 'Latest Action Date', 'Building Type', 'Community - Board', 'Cluster', 'Landmarked', 'Adult Estab', 'Loft Board', 'City Owned', 'Little e', 'PC Filed', 'eFiling Filed', 'Plumbing', 'Mechanical', 'Boiler', 'Fuel Burning', 'Fuel Storage', 'Standpipe', 'Sprinkler', 'Fire Alarm', 'Equipment', 'Fire Suppression', 'Curb Cut', 'Other', 'Other Description', "Applicant's First Name", "Applicant's Last Name", 'Applicant Professional Title', 'Applicant License #', 'Professional Cert', 'Pre- Filing Date', 'Paid', 'Fully Paid', 'Assigned', 'Approved', 'Fully Permitted', 'Initial Cost', 'Total Est. Fee', 'Fee Status', 'Existing Zoning Sqft', 'Proposed Zoning Sqft', 'Horizontal Enlrgmt', 'Vertical Enlrgmt', 'Enlargement SQ Footage', 'Street Frontage', 'ExistingNo. of Stories', 'Proposed No. of Stories', 'Existing Height', 'Proposed Height', 'Existing Dwelling Units', 'Proposed Dwelling Units', 'Existing Occupancy', 'Proposed Occupancy', 'Site Fill', 'Zoning Dist1', 'Zoning Dist2', 'Zoning Dist3', 'Special District 1', 'Special District 2', 'Owner Type', 'Non-Profit', "Owner's First Name", "Owner's Last Name", "Owner's Business Name", "Owner's House Number", "Owner'sHouse Street Name", 'City ', 'State', 'Zip', "Owner'sPhone #", 'Job Description', 'DOBRunDate', 'JOB_S1_NO', 'TOTAL_CONSTRUCTION_FLOOR_AREA', 'WITHDRAWAL_FLAG', 'SIGNOFF_DATE', 'SPECIAL_ACTION_STATUS', 'SPECIAL_ACTION_DATE', 'BUILDING_CLASS', 'JOB_NO_GOOD_COUNT', 'GIS_LATITUDE', 'GIS_LONGITUDE', 'GIS_COUNCIL_DISTRICT', 'GIS_CENSUS_TRACT', 'GIS_NTA_NAME', 'GIS_BIN']
    target_columns = df.columns                   # list of target column names

    for old_name, new_name in zip(target_columns, source_columns):
        df = df.withColumnRenamed(old_name, new_name)
    return df

def extract1(config):
    logger = get_logger("Extract")
    logger.info("Data extraction started")

    spark = SparkSession.builder.appName("ExtractData").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Spark session ready")
    #logger.info("Data Extraction from URL started")
    #data_extraction_url()
    #logger.info("Data Extraction from URL completed")

    df = spark.read.parquet("D:\\checking\\BIG_DATA_PROJECT_DATA\\Job_Application_Filings_rawp.parquet")
    logger.info("Raw parquet file loaded")

    df = change_col_name(df)
    logger.info("Column names standardized")

    logger.info("Data extraction completed")
    return df
