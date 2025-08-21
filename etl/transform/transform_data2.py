import warnings
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utils.logger import get_logger

logger = get_logger("transform2")

def job_applications_transform(job_applications_df):
    logger.info("Starting job_applications_df transformation")

    columns_to_exclude = ["City Owned", "GIS_LATITUDE", "GIS_LONGITUDE","Zoning Dist2"]
    job_applications_df = job_applications_df.drop(*columns_to_exclude)

    # changing column names     
    job_applications_df=job_applications_df.withColumnRenamed("Pre- Filing Date","Pre_Filing_Date")
    job_applications_df=job_applications_df.withColumnRenamed("Doc #","Doc")
    job_applications_df=job_applications_df.withColumnRenamed("Community_-_Board","Community_Board")

    columns=job_applications_df.columns
    temp_dict={i: i.replace(" ","_") for i in columns}

    for old, new in temp_dict.items():
        job_applications_df=job_applications_df.withColumnRenamed(old, new)

    # handling null and irrelavant values
    nullc=["Plumbing","Mechanical","Equipment"]
    for i in nullc:
        job_applications_df=job_applications_df.withColumn(i,when(col(i) == "X",1).otherwise(0))

    job_applications_df = job_applications_df.withColumn("Initial_Cost", regexp_replace(col("Initial_Cost"),"[$]",""))
    job_applications_df = job_applications_df.withColumn("Total_Est_Fee", regexp_replace(col("Total_Est_Fee"),"[$]",""))

    dic=["Initial_Cost","Total_Est_Fee","Existing_Dwelling_Units","Proposed_Dwelling_Units"]
    for i in dic:
        job_applications_df = job_applications_df.withColumn(
            i,
            when(col(i).rlike("^[0-9]+\\.?[0-9]*$"), col(i).cast("double"))
            .otherwise(0)
        )

    # Fill null values dynamically
    fill_values = {}
    for field in job_applications_df.schema.fields:
        column_name = field.name
        data_type = field.dataType
        if isinstance(data_type, StringType):
            fill_values[column_name] = "unknown"
        elif isinstance(data_type, (LongType, IntegerType, DoubleType, FloatType)):
            fill_values[column_name] = 0
        elif isinstance(data_type, (DateType, TimestampType)):
            fill_values[column_name] = "1900-01-01"

    job_applications_df = job_applications_df.fillna(fill_values)

    # changing column datatypes
    cols = {
        "Latest_Action_Date": "MM/dd/yyyy",
        "Pre_Filing_Date": "MM/dd/yyyy",
        "Paid": "MM/dd/yyyy",
        "Fully_Paid": "MM/dd/yyyy",
        "Assigned": "MM/dd/yyyy",
        "Approved": "MM/dd/yyyy",
        "Fully_Permitted": "MM/dd/yyyy"
    }

    date_pattern = r"^(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])/(\d{4})$"

    for i, j in cols.items():
        parts = split(regexp_replace(trim(col(i)), "-", "/"), "/")
        job_applications_df = job_applications_df.withColumn(
            i,
            when(
                (size(parts) == 3) & (trim(col(i)).rlike(date_pattern)),
                to_date(
                    concat_ws(
                        "/",
                        lpad(parts.getItem(0), 2, "0"),
                        lpad(parts.getItem(1), 2, "0"),
                        parts.getItem(2)
                    ),
                    j
                )
            ).otherwise(None)
        )

    logger.info("job_applications_df transformation completed")
    return job_applications_df

def applicants_transform(applicants_df):
    logger.info("Starting applicants_df transformation")

    applicants_df = applicants_df.withColumn(
        "Applicant_License_Number",
        when(col("Applicant_License_Number").rlike("[^0-9]"), "0")
        .otherwise(col("Applicant_License_Number"))
    ).withColumn("Applicant_License_Number", col("Applicant_License_Number").cast("long"))

    logger.info("applicants_df transformation completed")
    return applicants_df

def properties_transform(properties_df):
    logger.info("Starting properties_df transformation")

    properties_df = properties_df.withColumnRenamed("Street Name", "Street_Name")
    properties_df=properties_df.withColumn("Lot",when(col("Lot") == "23,24","0").otherwise(col("Lot")))
    properties_df=properties_df.withColumn("Lot",when(col("Lot") == "OOO58","0").otherwise(col("Lot")))
    properties_df=properties_df.withColumn("Lot",when(col("Lot") == "...28","0").otherwise(col("Lot")))
    properties_df=properties_df.withColumn("Lot",when(col("Lot") == "000 4","0").otherwise(col("Lot")))

    sec = trim(regexp_extract(col("House_Number"), r"^\d+[- ]*(.*)$", 1))

    properties_df = (
        properties_df
            .withColumn("House_Number_Part1", regexp_extract(col("House_Number"), r"^(\d+)", 1))
            .withColumn("House_Number_Part2_Number", when(sec.rlike(r"^\d+$"), sec).otherwise(regexp_extract(sec, r"^\d+", 0)))
            .withColumn("House_Number_Part2_Letter", trim(regexp_replace(sec, r"[^a-zA-Z]", "")))
            .drop("House_Number")
    )

    properties_df = properties_df.withColumn("Block", regexp_replace(col("Block"), r"^[^\d]", "0"))

    for c in ["House_Number_Part1", "House_Number_Part2_Number", "Block", "Lot"]:
        properties_df = properties_df.withColumn(
            c,
            when(col(c) != "", col(c).cast("double")).otherwise(0)
        )

    logger.info("properties_df transformation completed")
    return properties_df

def transform2(job_applications_df, applicants_df, properties_df):
    logger.info("Transformation phase started")

    job_applications_df = job_applications_transform(job_applications_df)
    applicants_df = applicants_transform(applicants_df)
    properties_df = properties_transform(properties_df)

    logger.info("All transformations completed successfully")
    return job_applications_df, applicants_df, properties_df
