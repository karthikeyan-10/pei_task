import pytest
import pandas as pd
from pyspark.sql import SparkSession, Row
from unittest.mock import patch, MagicMock
from src.utils.data_utils import raw_data_to_df, clean_column_names, write_df_to_table


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()
    yield spark
    spark.stop()


def test_invalid_source_location(spark):
    with pytest.raises(ValueError, match="Invalid source_location"):
        raw_data_to_df("", "csv")

def test_invalid_file_format(spark):
    with pytest.raises(ValueError, match="Unsupported file_format."):
        raw_data_to_df("data.txt", "txt")

def test_invalid_header_type(spark):
    with pytest.raises(TypeError, match="'header' must be a boolean"):
        raw_data_to_df("data.csv", "csv", header="yes")

def test_invalid_delimiter(spark):
    with pytest.raises(ValueError, match="'delimiter' must be a single character string."):
        raw_data_to_df("data.csv", "csv", delimiter="::")

def test_invalid_mode(spark):
    with pytest.raises(ValueError, match="'mode' must be one of: PERMISSIVE, DROPMALFORMED, FAILFAST."):
        raw_data_to_df("data.csv", "csv", mode="INVALID")


def test_csv_load(spark, tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id,name\n1,Karthik\n2,Mohan\n")

    df = raw_data_to_df(str(path), "csv", header=True, infer_schema=True)
    rows = [tuple(r) for r in df.collect()]
    assert rows == [(1, "Karthik"), (2, "Mohan")]

def test_json_load(spark, tmp_path):
    path = tmp_path / "data.json"
    path.write_text('{"id":1,"name":"Karthik"}\n{"id":2,"name":"Mohan"}\n')

    df = raw_data_to_df(str(path), "json")
    rows = [tuple(r) for r in df.collect()]
    assert rows == [(1, "Karthik"), (2, "Mohan")]




def test_clean_column_names_basic(spark):
    data = [Row(**{"Customer ID": 1, "Order-Date": "2025-08-23", "Price($)": 100})]
    df = spark.createDataFrame(data)

    cleaned_df = clean_column_names(df)
    expected_cols = ["customer_id", "order_date", "price___"]

    assert cleaned_df.columns == expected_cols

def test_clean_column_names_special_chars(spark):
    data = [Row(**{"A,x;C{}()= \n\t": 123})]
    df = spark.createDataFrame(data)

    cleaned_df = clean_column_names(df)
    assert cleaned_df.columns == ["a_x_c_________"]

def test_clean_column_names_no_change(spark):
    data = [Row(**{"name": "Karthik", "age": 20})]
    df = spark.createDataFrame(data)

    cleaned_df = clean_column_names(df)
    assert cleaned_df.columns == ["name", "age"]




def sample_df(spark):
    data = [Row(id=1, name="Karthik")]
    return spark.createDataFrame(data)

def test_write_df_invalid_catalog(sample_df):
    with pytest.raises(ValueError, match="'catalog_name' must be a non-empty string."):
        write_df_to_table(sample_df, "", "schema", "table")

def test_write_df_invalid_schema(sample_df):
    with pytest.raises(ValueError, match="'schema_name' must be a non-empty string."):
        write_df_to_table(sample_df, "catalog", "   ", "table")

def test_write_df_invalid_table(sample_df):
    with pytest.raises(ValueError, match="'table_name' must be a non-empty string."):
        write_df_to_table(sample_df, "catalog", "schema", None)

