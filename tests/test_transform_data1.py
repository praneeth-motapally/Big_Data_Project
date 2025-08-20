import pytest

def create_sample_df_full(spark):
    # Input uses ORIGINAL names (with spaces/punctuation) as transform1 expects to rename them.
    data = [
        ("A1", 5, 6, 100.0, "boiler_val", "fuel_burn_val", "fuel_store_val", "standpipe_val",
         "fire_alarm_val", "fire_supp_val", "curb_cut_val", "h_enlarge_val", "v_enlarge_val",
         "Z3", "SD2", "123", "Main St", "NYC", "NY", "10001", "keep1_val", 42),
        ("A2", 1, 2, 50.5, "boiler_val2", "fuel_burn_val2", "fuel_store_val2", "standpipe_val2",
         "fire_alarm_val2", "fire_supp_val2", "curb_cut_val2", "h_enlarge_val2", "v_enlarge_val2",
         "Z3b", "SD2b", "456", "Broadway", "LA", "CA", "90001", "keep2_val", 99),
    ]
    cols = [
        "Lot",
        "ExistingNo. of Stories",
        "Proposed No. of Stories",
        "Total Est. Fee",
        "Boiler",
        "Fuel Burning",
        "Fuel Storage",
        "Standpipe",
        "Fire Alarm",
        "Fire Suppression",
        "Curb Cut",
        "Horizontal Enlrgmt",
        "Vertical Enlrgmt",
        "Zoning Dist3",
        "Special District 2",
        "Owner's House Number",
        "Owner'sHouse Street Name",
        "City ",
        "State",
        "Zip",
        "keep_col_str",
        "keep_col_int",
    ]
    return spark.createDataFrame(data, cols)

def create_sample_df_partial(spark):
    # Missing some unwanted columns to ensure the function still works
    data = [
        ("A3", 3, 4, 75.0, "keep3", 7),
    ]
    cols = [
        "Lot",
        "ExistingNo. of Stories",
        "Proposed No. of Stories",
        "Total Est. Fee",
        "keep_col_str",
        "keep_col_int",
    ]
    return spark.createDataFrame(data, cols)

def create_sample_raw_df(spark):
    data = [
        ("A4", 10, 11, 500.0, "keep4", 123),
    ]
    cols = [
        "Lot",
        "ExistingNo. of Stories",  # Original, non-standardized names
        "Proposed No. of Stories",
        "Total Est. Fee",
        "keep_col_str",
        "keep_col_int",
    ]
    return spark.createDataFrame(data, cols)

def test_transform_renames_and_drops_unwanted_columns(spark):
    from etl.transform.transform_data1 import transform1

    df = create_sample_df_full(spark)
    out = transform1(df)

    # Assert unwanted columns removed (names must match exactly the function list)
    unwanted_cols = [
        'Boiler', 'Fuel Burning', 'Fuel Storage', 'Standpipe', 'Fire Alarm',
        'Fire Suppression', 'Curb Cut', 'Horizontal Enlrgmt', 'Vertical Enlrgmt',
        'Zoning Dist3', 'Special District 2', "Owner's House Number",
        "Owner'sHouse Street Name", 'City ', 'State', 'Zip'
    ]
    for c in unwanted_cols:
        assert c not in out.columns, f"Unwanted column {c} should be removed"

    # Assert renamed columns present
    assert "ExistingNo_of_Stories" in out.columns
    assert "ProposedNo_of_Stories" in out.columns
    assert "Total_Est_Fee" in out.columns

    # Assert original names no longer present
    assert "ExistingNo. of Stories" not in out.columns
    assert "Proposed No. of Stories" not in out.columns
    assert "Total Est. Fee" not in out.columns

def test_transform_handles_missing_unwanted_columns_gracefully(spark):
    from etl.transform.transform_data1 import transform1

    df = create_sample_df_partial(spark)
    out = transform1(df)

    # Renamed columns should be present
    assert "ExistingNo_of_Stories" in out.columns
    assert "ProposedNo_of_Stories" in out.columns
    assert "Total_Est_Fee" in out.columns

    # Input had no unwanted columns; still should succeed and keep expected columns
    assert "keep_col_str" in out.columns and "keep_col_int" in out.columns

def test_transform1_renaming(spark):
    from etl.transform.transform_data1 import transform1
    df = create_sample_raw_df(spark)
    out = transform1(df)

    # Check for the new, standardized names
    assert "ExistingNo_of_Stories" in out.columns
    assert "ProposedNo_of_Stories" in out.columns
    assert "Total_Est_Fee" in out.columns

    # Ensure the old names are gone
    assert "ExistingNo. of Stories" not in out.columns

