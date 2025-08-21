from src.utils.logger import get_logger

logger = get_logger("load2")

def load2(job_applications_df, applicants_df, properties_df, config):
    logger.info("Loading transformed data to processed path")
    output_path = config['data']['processed_path']
    
    job_applications_df \
        .coalesce(5) \
        .write.mode("overwrite") \
        .parquet(f"{output_path}/Job_Applications")
    logger.info("Job_Applications table written successfully")

    properties_df \
        .coalesce(1) \
        .write.mode("overwrite") \
        .parquet(f"{output_path}/Properties")
    logger.info("Properties table written successfully")

    applicants_df \
        .coalesce(1) \
        .write.mode("overwrite") \
        .parquet(f"{output_path}/Applicants")
    logger.info("Applicants table written successfully")
    
    logger.info("All 3 tables processed and loaded successfully")
