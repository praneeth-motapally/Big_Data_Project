from pyspark.sql.functions import col, when, initcap, trim, concat_ws
from src.utils.logger import get_logger

def job_status_transform(job_status_df):
    logger = get_logger("Job Status Transform 4")
    logger.info("Mapping job_status_df status codes")
    
    job_status_df = job_status_df.withColumn(
        "Job_Status_Mapped",
        when(col("Job_Status") == "A", "Pre Filed")
        .when(col("Job_Status") == "I", "Sign Off")
        .when(col("Job_Status") == "P", "Approved")
        .when(col("Job_Status") == "R", "Permit Entire")
        .otherwise("UNKNOWN")
    )
    
    logger.info("job_status_df mapping completed")
    return job_status_df

def owners_transform(owners_df):
    logger = get_logger("Owners Transform 4")
    logger.info("Transforming owners_df names and creating full_name")
    
    owners_df = owners_df.withColumn("Owner_First_Name", initcap(trim(col("Owner_First_Name")))) \
                         .withColumn("Owner_Last_Name", initcap(trim(col("Owner_Last_Name")))) \
                         .withColumn("Full_Name", concat_ws(" ", "Owner_First_Name", "Owner_Last_Name"))
    
    logger.info("owners_df transformation completed")
    return owners_df

def transform4(job_status_df, owners_df):
    logger = get_logger("Pipeline 4 Transformation")
    logger.info("Starting transformations for Pipeline 4")
    
    job_status_df = job_status_transform(job_status_df)
    owners_df = owners_transform(owners_df)
    
    logger.info("All transformations for Pipeline 4 completed successfully")
    return job_status_df, owners_df
