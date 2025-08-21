from pyspark.sql import SparkSession
from src.utils.logger import get_logger
from groq import Groq
import os
from dotenv import load_dotenv

# Load local .env only if running outside Jenkins
if not os.getenv("JENKINS_HOME"):  
    load_dotenv()

def job_status_transform(job_status_df, config):
    logger = get_logger("Job Status Transform 5")
    logger.info("Starting role extraction for job_status_df using Groq API")

    spark = SparkSession.builder.appName("TransformData5").getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    job_descs = job_status_df.select("Job_Status_Descrp").toPandas()

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is not set! Check .env or Jenkins credentials.")
    
    client = Groq(api_key=api_key)

    model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

    def get_job_role(description):
        if not description:
            return "Unknown"
        prompt = f"""
        Analyze the following job description and identify the single most relevant job role. 
        Respond with only the job role and nothing else.

        Job Description: "{description}"
        """
        try:
            chat_completion = client.chat.completions.create(
                messages=[
                    {"role": "system", "content": "You are an expert at identifying job roles from descriptions."},
                    {"role": "user", "content": prompt}
                ],
                model=model,
                temperature=0.0
            )
            role = chat_completion.choices[0].message.content.strip()
            return role.strip('"')
        except Exception as e:
            logger.error(f"Groq API error: {e}")
            return "Error"

    job_descs["Role"] = job_descs["Job_Status_Descrp"].apply(get_job_role)
    job_descs_spark = spark.createDataFrame(job_descs)

    job_status_with_roles = job_status_df.join(
        job_descs_spark,
        on="Job_Status_Descrp",
        how="left"
    )

    logger.info("Role extraction completed successfully")
    return job_status_with_roles

def transform5(job_status_df, config):
    logger = get_logger("Pipeline 5 Transformation")
    logger.info("Starting transformation for Pipeline 5")

    job_status_df = job_status_transform(job_status_df, config)

    logger.info("Pipeline 5 transformation completed successfully")
    return job_status_df
