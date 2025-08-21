from src.utils.logger import get_logger

def load3(job_status_df, owners_df, boroughs_df, config):
    logger = get_logger("Data Loading 3")
    logger.info("Starting to write Pipeline 3 tables to processed path")
    
    output_path = config['data']['processed_path']
    
    job_status_df.coalesce(3).write.mode("overwrite").parquet(f"{output_path}/Job_Status")
    owners_df.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/Owners")
    boroughs_df.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/Boroughs")
    
    logger.info("Pipeline 3 tables written successfully")
