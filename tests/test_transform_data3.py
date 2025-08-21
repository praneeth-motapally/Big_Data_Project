# tests/test_transform_data3.py
from pyspark.sql.types import StringType, IntegerType, LongType, StructType,StructField

from etl.transform.transform_data3 import (
    job_status_transform,
    owners_transform,
    boroughs_transform,
    transform3,
)

def test_job_status_transform_schema_only(spark):
    # No actions: only schema/columns assertions
    data = [("A", "B", "C")]
    cols = ["Job Status", "Job Type", "Latest Action Date"]
    df = spark.createDataFrame(data, cols)

    out = job_status_transform(df)

    # Renamed columns should exist
    assert "Job_Status" in out.columns
    assert "Job_Type" in out.columns
    assert "Latest_Action_Date" in out.columns
    # No spaces in column names
    assert all(" " not in c for c in out.columns)

def test_owners_transform_schema_only(spark):
    # Explicit schema to avoid inference errors with None values
    schema = StructType([
        StructField("Owner Type", StringType(), True),
        StructField("Owner_Business_Name", StringType(), True),
        StructField("Non-Profit", StringType(), True),
    ])
    data = [(None, None, None)]
    df = spark.createDataFrame(data, schema=schema)

    out = owners_transform(df)

    # Column presence
    assert "Owner_Type" in out.columns
    assert "Owner_Business_Name" in out.columns
    assert "Non_Profit" in out.columns

    # Dtype checks via schema only
    dtypes = {f.name: f.dataType for f in out.schema.fields}
    assert "Owner_Type" in dtypes and isinstance(dtypes["Owner_Type"], StringType)
    assert "Owner_Business_Name" in dtypes and isinstance(dtypes["Owner_Business_Name"], StringType)
    assert "Non_Profit" in dtypes and isinstance(dtypes["Non_Profit"], StringType)

def test_boroughs_transform_schema_only(spark):
    data = [("Manhattan",), ("brooklyn",), ("QUEENS",)]
    cols = ["Borough"]
    df = spark.createDataFrame(data, cols)

    out = boroughs_transform(df)

    # Expected columns after transform
    assert "Borough" in out.columns
    assert "Borough_Code" in out.columns

    # Schema-only dtype checks (no actions)
    dtypes = {f.name: f.dataType for f in out.schema.fields}
    assert "Borough" in dtypes and isinstance(dtypes["Borough"], StringType)
    # Accept integer/long for code
    assert "Borough_Code" in dtypes and isinstance(dtypes["Borough_Code"], (IntegerType, LongType))

def test_transform3_schema_only(spark):
    job_df = spark.createDataFrame([("A", "B")], ["Job Status", "Job Type"])

    owners_schema = StructType([
        StructField("Owner Type", StringType(), True),
        StructField("Owner_Business_Name", StringType(), True),
        StructField("Non-Profit", StringType(), True),
    ])
    owners_df = spark.createDataFrame([(None, None, None)], schema=owners_schema)

    boroughs_df = spark.createDataFrame([("Manhattan",)], ["Borough"])

    j_out, o_out, b_out = transform3(job_df, owners_df, boroughs_df)

    assert "Job_Status" in j_out.columns
    assert "Job_Type" in j_out.columns
    assert "Owner_Type" in o_out.columns
    assert "Owner_Business_Name" in o_out.columns
    assert "Non_Profit" in o_out.columns
    assert "Borough" in b_out.columns and "Borough_Code" in b_out.columns
