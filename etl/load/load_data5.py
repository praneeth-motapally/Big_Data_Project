from src.utils.logger import get_logger

def load5(job_status_df, config):
    logger = get_logger("Data Loading 5")
    logger.info("Writing Pipeline 5 job_status_df to processed path")
    
    output_path = config['data']['processed_path']
    
    job_status_df.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/Job_Status_With_Role")
    
    logger.info("Pipeline 5 table written successfully")
