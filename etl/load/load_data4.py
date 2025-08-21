from src.utils.logger import get_logger

def load4(job_status_df, owners_df, config):
    logger = get_logger("Data Loading 4")
    logger.info("Writing Pipeline 4 tables to processed path")
    
    output_path = config['data']['processed_path']
    
    job_status_df.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/New_Job_Status")
    owners_df.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/New_Owners")
    
    logger.info("Pipeline 4 tables written successfully")
