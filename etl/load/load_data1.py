from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utils.logger import get_logger

def creating_tables(col_removed):
    applicants_df = col_removed.select(
        "Applicant_First_Name", 
        "Applicant_Last_Name", 
        "Applicant_Professional_Title", 
        "Applicant_License_Number"
    ).distinct().withColumn("Applicant_ID", monotonically_increasing_id())

    owners_df = col_removed.select(
        "Owner Type",
        "Non-Profit",
        "Owner_First_Name",
        "Owner_Last_Name",
        "Owner_Business_Name"
    ).distinct().withColumn("Owner_ID", monotonically_increasing_id())

    properties_df = col_removed.select(
        "Bin_Number", 
        "House_Number", 
        "Street Name", 
        "Block", 
        "Lot"
    ).distinct()

    job_status_df = col_removed.select(
        "Job Status", 
        "Job Status Descrp"
    ).distinct()

    boroughs_df = col_removed.select(
        "Borough"
    ).distinct()

    jobs_with_applicants = col_removed.join(
        applicants_df,
        on=["Applicant_First_Name", "Applicant_Last_Name", "Applicant_Professional_Title", "Applicant_License_Number"],
        how="left"
    ).drop("Applicant_First_Name", "Applicant_Last_Name", "Applicant_Professional_Title", "Applicant_License_Number")

    jobs_with_owners = jobs_with_applicants.join(
        owners_df,
        on=["Owner_First_Name", "Owner_Last_Name", "Owner_Business_Name", "Owner Type", "Non-Profit"],
        how="left"
    ).drop("Owner_First_Name", "Owner_Last_Name", "Owner_Business_Name", "Owner Type", "Non-Profit")

    job_applications_df = jobs_with_owners.select(
        "Job_Number",
        "Doc #",
        "Job Type",
        "Latest Action Date",
        "Borough",
        "Job Status",
        "Bin_Number",
        "Applicant_ID",
        "Owner_ID",
        "Building Type", "Community - Board", "Cluster", "Landmarked", "Adult Estab",
        "Loft Board", "City Owned", "Little e", "PC Filed", "eFiling Filed",
        "Plumbing", "Mechanical", "Equipment", "Other", "Other Description",
        "Professional Cert", "Pre- Filing Date", "Paid", "Fully Paid", "Assigned",
        "Approved", "Fully Permitted", "Initial Cost", "Total_Est_Fee", "Fee Status",
        "Existing Zoning Sqft", "Proposed Zoning Sqft", "Enlargement SQ Footage",
        "Street Frontage", "ExistingNo_of_Stories", "ProposedNo_of_Stories",
        "Existing Height", "Proposed Height", "Existing Dwelling Units",
        "Proposed Dwelling Units", "Existing Occupancy", "Proposed Occupancy",
        "Site Fill", "Zoning Dist1", "Zoning Dist2",
        "GIS_COUNCIL_DISTRICT", "GIS_NTA_NAME", "GIS_BIN"
    )

    return (job_applications_df, properties_df, applicants_df, 
                owners_df, job_status_df, boroughs_df)

def load1(col_removed, config):
    logger = get_logger("Load")
    logger.info("Data loading started")

    col_removed = col_removed.withColumnRenamed("Job #", "Job_Number") \
        .withColumnRenamed("Bin #", "Bin_Number") \
        .withColumnRenamed("House #", "House_Number") \
        .withColumnRenamed("Applicant's First Name", "Applicant_First_Name") \
        .withColumnRenamed("Applicant's Last Name", "Applicant_Last_Name") \
        .withColumnRenamed("Applicant Professional Title", "Applicant_Professional_Title") \
        .withColumnRenamed("Applicant License #", "Applicant_License_Number") \
        .withColumnRenamed("Owner's First Name", "Owner_First_Name") \
        .withColumnRenamed("Owner's Last Name", "Owner_Last_Name") \
        .withColumnRenamed("Owner's Business Name", "Owner_Business_Name")
    logger.info("Standardized column names for loading")

    logger.info("Creating tables (fact + dimensions)")
    (job_applications_df, properties_df, applicants_df, 
     owners_df, job_status_df, boroughs_df) = creating_tables(col_removed)
    logger.info("Tables created successfully")

    output_path = config['data']['raw_path']
    logger.info(f"Writing tables to {output_path}")

    job_applications_df.coalesce(5).write.mode("overwrite").parquet(f"{output_path}/Job_Applications")
    properties_df.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/Properties")
    applicants_df.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/Applicants")
    owners_df.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/Owners")
    job_status_df.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/Job_Status")
    boroughs_df.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/Boroughs")

    logger.info("Data loading completed")
