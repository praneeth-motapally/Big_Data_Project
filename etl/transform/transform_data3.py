from pyspark.sql import functions as F
from src.utils.logger import get_logger

def job_status_transform(job_status_df):
    logger = get_logger("Job Status Transform 3")
    logger.info("Transforming job_status_df columns")
    for i in job_status_df.columns:
        job_status_df = job_status_df.withColumnRenamed(i, i.replace(" ", "_"))
    logger.info("job_status_df column transformation completed")
    return job_status_df

def owners_transform(owners_df):
    logger = get_logger("Owners Transform 3")
    logger.info("Transforming owners_df columns and handling nulls")
    
    owners_df = owners_df.withColumnRenamed("Owner Type", "Owner_Type") \
                         .withColumnRenamed("Non-Profit", "Non_Profit")
    
    owners_df = owners_df.fillna({"Owner_Type": "unknown", "Owner_Business_Name": "unknown", "Non_Profit": "Not_specified"})
    logger.info("owners_df transformation completed")
    return owners_df

def boroughs_transform(boroughs_df):
    logger = get_logger("Boroughs Transform 3")
    logger.info("Transforming boroughs_df and mapping codes")
    
    borough_map = {
        "MANHATTAN": 1, "BROOKLYN": 2, "QUEENS": 3, "BRONX": 4, "STATEN ISLAND": 5
    }
    mapping_expr = F.create_map([F.lit(x) for pair in borough_map.items() for x in pair])
    
    boroughs_df = boroughs_df.withColumn("Borough", F.upper(F.trim(F.col("Borough")))) \
                             .withColumn("Borough_Code", mapping_expr[F.col("Borough")])
    
    logger.info("boroughs_df transformation completed")
    return boroughs_df

def transform3(job_status_df, owners_df, boroughs_df):
    logger = get_logger("Pipeline 3 Transformation")
    logger.info("Starting transformation for Pipeline 3")
    
    job_status_df = job_status_transform(job_status_df)
    owners_df = owners_transform(owners_df)
    boroughs_df = boroughs_transform(boroughs_df)
    
    logger.info("All transformations for Pipeline 3 completed successfully")
    return job_status_df, owners_df, boroughs_df
