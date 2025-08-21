from pyspark.sql.types import StringType

from etl.transform.transform_data4 import (
    job_status_transform,
    owners_transform,
    transform4,
)

def test_job_status_transform_schema_only(spark):
    # Input DataFrame
    data = [("A",), ("I",), ("P",), ("R",), ("X",)]
    cols = ["Job_Status"]
    df = spark.createDataFrame(data, cols)

    out = job_status_transform(df)

    # Schema-only assertions
    assert "Job_Status" in out.columns
    assert "Job_Status_Mapped" in out.columns

    # Dtype checks (no actions)
    dtypes = {f.name: f.dataType for f in out.schema.fields}
    assert isinstance(dtypes["Job_Status"], StringType)
    assert isinstance(dtypes["Job_Status_Mapped"], StringType)

def test_owners_transform_schema_only(spark):
    data = [("john ", "doe"), ("MARY", " smith")]
    cols = ["Owner_First_Name", "Owner_Last_Name"]
    df = spark.createDataFrame(data, cols)

    out = owners_transform(df)

    # Columns expected after transform
    for c in ["Owner_First_Name", "Owner_Last_Name", "Full_Name"]:
        assert c in out.columns

    # Dtype checks (schema-only)
    dtypes = {f.name: f.dataType for f in out.schema.fields}
    assert isinstance(dtypes["Owner_First_Name"], StringType)
    assert isinstance(dtypes["Owner_Last_Name"], StringType)
    assert isinstance(dtypes["Full_Name"], StringType)

def test_transform4_schema_only(spark):
    job_df = spark.createDataFrame([("A",)], ["Job_Status"])
    owners_df = spark.createDataFrame([("john", "doe")], ["Owner_First_Name", "Owner_Last_Name"])

    j_out, o_out = transform4(job_df, owners_df)

    # Basic schema presence checks (no actions)
    assert "Job_Status" in j_out.columns
    assert "Job_Status_Mapped" in j_out.columns

    assert "Owner_First_Name" in o_out.columns
    assert "Owner_Last_Name" in o_out.columns
    assert "Full_Name" in o_out.columns
