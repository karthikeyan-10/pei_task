from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import pandas as pd
import re

def raw_data_to_df(
    source_location: str,
    file_format: str,
    header: bool = True,
    delimiter: str = ",",
    infer_schema: bool = True,
    mode: str = "FAILFAST",
    **options
) -> DataFrame:
    """
    Read raw data from source location and return a Spark DataFrame.
    Supports: CSV, JSON, Parquet, Excel+.

    Parameters
    ----------
    source_location : str
        Path to the input file
    file_format : str
        Format of the file.
    header : bool
        Whether first row is header.
    delimiter : str
        Field delimiter for CSV.
    infer_schema : bool
        Whether to infer schema automatically.
    mode : str
        To handle corrupted records: 'PERMISSIVE', 'DROPMALFORMED', 'FAILFAST'.
    **options :
        Other Spark options.
    """

    # input validation
    supported_formats = {"csv", "json", "parquet", "excel"}
    if not source_location or not isinstance(source_location, str):
        raise ValueError("Invalid source_location. It must be a non-empty string.")

    if file_format.lower() not in supported_formats:
        raise ValueError(
            f"Unsupported file_format: {file_format}. "
            f"Supported formats: {supported_formats}"
        )

    if not isinstance(header, bool):
        raise TypeError("'header' must be a boolean.")

    if not isinstance(delimiter, str) or len(delimiter) != 1:
        raise ValueError("'delimiter' must be a single character string.")

    if mode.upper() not in {"PERMISSIVE", "DROPMALFORMED", "FAILFAST"}:
        raise ValueError("'mode' must be one of: PERMISSIVE, DROPMALFORMED, FAILFAST.")

    fmt = file_format.lower()

    try:
        
        if fmt in ["csv", "json", "parquet"]:
            reader = spark.read.format(fmt)

            if fmt == "csv":
                reader = (
                    reader.option("header", str(header).lower())
                          .option("delimiter", delimiter)
                          .option("inferSchema", str(infer_schema).lower())
            )
            reader = reader.option("mode", mode)

            # add extra options
            for k, v in options.items():
                reader = reader.option(k, v)

            return reader.load(source_location)

        elif fmt == "excel":
            # Try spark-excel first
            try:
                _ = spark._sc._jvm.com.crealytics.spark.excel.DefaultSource
                has_spark_excel = True
            except Exception:
                has_spark_excel = False

            if has_spark_excel:
                reader = spark.read.format("com.crealytics.spark.excel")
                for k, v in options.items():
                    reader = reader.option(k, v)
                return reader.load(source_location)

            else:
                # Fallback to Pandas
                sheet_name = options.get("sheet_name", 0)
                pdf = pd.read_excel(f"{source_location}", sheet_name=sheet_name)
                # Fix object columns
                for col in pdf.select_dtypes(include="object").columns:
                    pdf[col] = pdf[col].fillna("").astype(str)
                return spark.createDataFrame(pdf)

    except Exception as e:
        raise RuntimeError(
            f"Failed to read {file_format.upper()} from {source_location}: {str(e)}"
        )


def clean_column_names(df: DataFrame) -> DataFrame:
    """
    Clean DataFrame column names by replacing invalid characters with underscores and to lower case 
    """
    cleaned_cols = []
    for col in df.columns:
        # Replace invalid characters with underscore
        clean_col = re.sub(r"[ ,-;{}()\n\t=]", "_", col).lower()
        cleaned_cols.append(clean_col)
    
    return df.toDF(*cleaned_cols)


def write_df_to_table(
    df: DataFrame,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    mode: str = "overwrite"
) -> None:
    """
    Write a Spark DataFrame to a table in the specified catalog and schema using Delta format.
    
    Parameters
    ----------
    df : DataFrame
        Spark DataFrame to write.
    catalog_name : str
        Target catalog name
    schema_name : str
        Target schema name.
    table_name : str
        Target table name.
    mode : str
        Save mode: 'overwrite', 'append', (default to 'overwrite').
    
    Raises
    ------
    ValueError
        If inputs are invalid.
    RuntimeError
        If writing the table fails.
    """
    
    for arg_name, arg_value in [("catalog_name", catalog_name), ("schema_name", schema_name), ("table_name", table_name)]:
        if not isinstance(arg_value, str) or not arg_value.strip():
            raise ValueError(f"'{arg_name}' must be a non-empty string.")
    
    valid_modes = {"overwrite", "append"}
    if mode.lower() not in valid_modes:
        raise ValueError(f"Invalid write mode '{mode}'. Supported modes: {valid_modes}")

    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

    # Add audit columns
    df = df.withColumn("inserted_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())

    # Write DataFrame
    try:
        df.write.format("delta").mode(mode.lower()).saveAsTable(full_table_name)
        print(f"DataFrame successfully written to {full_table_name} with mode '{mode}'.")
    except Exception as e:
        raise RuntimeError(f"Failed to write DataFrame to table {full_table_name}: {str(e)}")


if __name__ == "__main__":
    pass
