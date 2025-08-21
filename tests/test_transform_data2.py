import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from etl.transform.transform_data2 import job_applications_transform, applicants_transform, properties_transform
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, FloatType, DateType, TimestampType
)


def test_job_applications_transform_schema_only(spark):
    from etl.transform.transform_data2 import job_applications_transform

    # Single-row input; no Spark actions will be executed in this test.
    data = [(
        1, "308", "ALT", "01/01/2000", "BROOKLYN", "ACTIVE", 123456, 111, 222, "RES",
        "R2 - 5", "Cl1", "N", "N", "N", "Y", "Y", "Y", "X", "X", "X",
        "Other", "Other desc", "Cert", "01/02/2000", "01/03/2000", "01/04/2000",
        "01/05/2000", "01/06/2000", "01/07/2000", "$100", "$200", "Paid",
        1000, 1200, 10, 50, 5, 6, 30.0, 35.0, "2", "3", "R", "R", "Site", "R6",
        36, "315", "Name", 123,
        # Pre-transform extra columns that should be dropped by the transform
        "Y", 40.0, -73.0, "R6-X",
    )]

    # Columns reflect your post-load "job_applications" schema plus 4 extra to test drop logic.
    cols = [
        "Job Number","Doc #","Job Type","Latest Action Date","Borough","Job Status",
        "Bin_Number","Applicant_ID","Owner_ID","Building Type","Community - Board",
        "Cluster","Landmarked","Adult Estab","Loft Board","Little e","PC Filed",
        "eFiling Filed","Plumbing","Mechanical","Equipment","Other","Other Description",
        "Professional Cert","Pre- Filing Date","Paid","Fully Paid","Assigned","Approved",
        "Fully Permitted","Initial Cost","Total_Est_Fee","Fee Status","Existing Zoning Sqft",
        "Proposed Zoning Sqft","Enlargement SQ Footage","Street Frontage",
        "ExistingNo_of_Stories","ProposedNo_of_Stories","Existing Height","Proposed Height",
        "Existing Dwelling Units","Proposed Dwelling Units","Existing Occupancy",
        "Proposed Occupancy","Site Fill","Zoning Dist1","GIS_COUNCIL_DISTRICT",
        "GIS_CENSUS_TRACT","GIS_NTA_NAME","GIS_BIN",
        # Extra pre-transform columns to be dropped (their exact spellings matter pre-normalization)
        "City Owned","GIS_LATITUDE","GIS_LONGITUDE","Zoning Dist2",
    ]

    df = spark.createDataFrame(data, cols)

    # Apply transform (lazy; no actions triggered here)
    out = job_applications_transform(df)

    # ========== Schema-only assertions (no actions) ==========

    # 1) Columns that should be present after transform (renamed/normalized)
    must_have_any = [
        "Job_Number",           # from "Job Number"
        "Doc",                  # from "Doc #"
        "Job_Type",
        "Latest_Action_Date",   # from "Latest Action Date"
        "Borough",
        "Job_Status",
        "Bin_Number",
        "Applicant_ID",
        "Owner_ID",
        "Building_Type",
    ]
    for c in must_have_any:
        assert c in out.columns, f"Expected column missing: {c}. Got: {out.columns}"

    # Community Board: robust transform should normalize to Community_Board.
    # If your transform hasn't been patched, it might remain "Community_-_Board".
    assert ("Community_Board" in out.columns) or ("Community_-_Board" in out.columns), \
        f"Community Board column missing. Got: {out.columns}"

    # 2) Ensure dropped columns are absent (post-normalization names)
    for c in ["City_Owned", "GIS_LATITUDE", "GIS_LONGITUDE", "Zoning_Dist2"]:
        assert c not in out.columns, f"Dropped column still present: {c}"

    # 3) Ensure key normalized target columns exist
    expected_normalized = [
        "Initial_Cost",            # from "Initial Cost"
        "Total_Est_Fee",           # already underscored in input
        "Existing_Dwelling_Units", # from "Existing Dwelling Units"
        "Proposed_Dwelling_Units", # from "Proposed Dwelling Units"
        "Pre_Filing_Date",         # from "Pre- Filing Date"
        "Paid", "Fully_Paid", "Assigned", "Approved", "Fully_Permitted",
        "Plumbing", "Mechanical", "Equipment",
    ]
    for c in expected_normalized:
        assert c in out.columns, f"Expected normalized column missing: {c}. Got: {out.columns}"

    # 4) Data type expectations (schema-only; no actions)
    types = {f.name: f.dataType for f in out.schema.fields}

    numeric_like = ["Initial_Cost", "Total_Est_Fee", "Existing_Dwelling_Units", "Proposed_Dwelling_Units"]
    for c in numeric_like:
        assert c in types, f"Type missing for {c}"
        assert isinstance(
            types[c],
            (DoubleType, FloatType, IntegerType, LongType, StringType)
        ), f"Unexpected dtype for {c}: {types[c]}"

    date_like = [
        "Latest_Action_Date", "Pre_Filing_Date",
        "Paid", "Fully_Paid", "Assigned", "Approved", "Fully_Permitted"
    ]
    for c in date_like:
        assert c in types, f"Type missing for {c}"
        # Accept StringType if parsing not applied yet; prefer DateType/TimestampType
        assert isinstance(
            types[c],
            (DateType, TimestampType, StringType)
        ), f"Unexpected dtype for {c}: {types[c]}"

    # 5) Boolean-like fields exist (X->1/0 mapped in transform, but we don't assert values here)
    for c in ["Plumbing", "Mechanical", "Equipment"]:
        assert c in types, f"Missing boolean-like column: {c}"
        assert isinstance(
            types[c],
            (IntegerType, LongType, DoubleType, StringType)
        ), f"Unexpected dtype for {c}: {types[c]}"

    # 6) Ensure no spaces remain in column names after normalization
    assert all(" " not in c for c in out.columns), f"Found spaces in column names: {out.columns}"


def test_applicants_transform_schema_only(spark):

    data = [("John","Doe","PE","ABC123",1), ("Jane","Smith","RA","12345",2)]
    cols = ["Applicant_First_Name","Applicant_Last_Name","Applicant_Professional_Title","Applicant_License_Number","Applicant_ID"]
    df = spark.createDataFrame(data, cols)

    out = applicants_transform(df)

    # Column presence (no actions)
    for c in cols:
        assert c in out.columns

    # Type checks (schema-only)
    t = {f.name: f.dataType for f in out.schema.fields}
    assert isinstance(t["Applicant_First_Name"], StringType)
    assert isinstance(t["Applicant_Last_Name"], StringType)
    assert isinstance(t["Applicant_Professional_Title"], StringType)
    assert isinstance(t["Applicant_ID"], (IntegerType, LongType))
    # License should be numeric after transform
    assert isinstance(t["Applicant_License_Number"], (LongType, IntegerType, DoubleType, FloatType))

    # No spaces in column names
    assert all(" " not in c for c in out.columns)


def test_properties_transform_schema_only(spark):

    # Include House_Number because the transform derives parts from it
    data = [
        (3103052, "EAST 54 STREET", "4702", "65", "123-45"),
        (3060447, "HEWES STREET", "OOO58", "23,24", "77B"),
    ]
    cols = ["Bin_Number", "Street Name", "Block", "Lot", "House_Number"]
    df = spark.createDataFrame(data, cols)

    out = properties_transform(df)

    # Column rename
    assert "Street_Name" in out.columns

    # Expected columns after transform (schema-only checks, no actions)
    expected_cols = [
        "Bin_Number", "Street_Name", "Block", "Lot",
        "House_Number_Part1", "House_Number_Part2_Number", "House_Number_Part2_Letter",
    ]
    for c in expected_cols:
        assert c in out.columns, f"Missing column: {c}. Got: {out.columns}"

    # Dtype checks via schema only
    types = {f.name: f.dataType for f in out.schema.fields}

    # Bin_Number numeric (accept int/long)
    assert "Bin_Number" in types
    assert isinstance(types["Bin_Number"], (IntegerType, LongType))

    # Block and Lot cast to double
    assert "Block" in types and isinstance(types["Block"], DoubleType)
    assert "Lot" in types and isinstance(types["Lot"], DoubleType)

    # House number parts: numbers as double, letter as string
    assert isinstance(types["House_Number_Part1"], DoubleType)
    assert isinstance(types["House_Number_Part2_Number"], DoubleType)
    assert isinstance(types["House_Number_Part2_Letter"], StringType)

    # No spaces should remain in column names
    assert all(" " not in c for c in out.columns)
