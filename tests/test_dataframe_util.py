import sys
import os
import shutil
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType
from pyspark.sql.functions import col

from delta import *

from utils.dataframe_util import *

@pytest.fixture(scope="module")
def spark():
    """Fixture for creating a SparkSession."""
    builder = (
        SparkSession.builder.appName("FlightRadar24 Test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")\
        .config("spark.sql.debug.maxToStringFields", "1000")\
        .config("spark.sql.parquet.columnarReaderBatchSize", "2048") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.sources.bucketing.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def cleanup(request):
    yield
    test_parquet_paths = ["test_save_df.parquet", "test_read_files.parquet"]
    for path in test_parquet_paths:
        if os.path.exists(path):
            shutil.rmtree(path)

    test_delta_paths = ["test_save_df.delta", "test_read_files.delta", "test_table_exists.delta", "test_upsert_table.delta"]
    for path in test_delta_paths:
        if os.path.exists(path):
            shutil.rmtree(path)

def test_save_dataframe_to_parquet(spark):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True)
    ])
    test_data = [
        ("1", "Alice", "1996", "1996_5", "1996_5_12"),
        ("2", "Bob", "1995", "1995_5", "1995_5_12"),
        ("3", "Charlie", "1994", "1994_5", "1994_5_12")
        ]

    test_df = spark.createDataFrame(test_data, schema)

    partition_col = ["year", "month", "day"]
    test_parquet_path = "test_save_df.parquet"
    save_dataframe_to_parquet(test_df, test_parquet_path, partition_columns=partition_col)
    
    assert os.path.exists(test_parquet_path)
    assert os.path.exists(test_parquet_path+"/year=1996/month=1996_5/day=1996_5_12")
    assert os.path.exists(test_parquet_path+"/year=1995/month=1995_5/day=1995_5_12")
    assert os.path.exists(test_parquet_path+"/year=1994/month=1994_5/day=1994_5_12")
    

def test_read_parquet_files(spark):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True)
    ])
    test_data = [
        ("1", "Alice", "1996", "1996_5", "1996_5_12"),
        ("2", "Bob", "1995", "1995_5", "1995_5_12"),
        ("3", "Charlie", "1994", "1994_5", "1994_5_12")
        ]

    test_df = spark.createDataFrame(test_data, schema)

    test_parquet_path = "test_read_files.parquet"
    test_df.write.parquet(test_parquet_path)
    read_df = read_parquet_files(spark, test_parquet_path)
    
    read_df = read_df.orderBy([col(c) for c in read_df.columns])
    test_df = test_df.orderBy([col(c) for c in test_df.columns])
    
    assert read_df is not None
    assert read_df.schema == test_df.schema
    assert read_df.collect() == test_df.collect()


def test_save_dataframe_to_delta(spark):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True)
    ])
    test_data = [
        ("1", "Alice", "1996", "1996_5", "1996_5_12"),
        ("2", "Bob", "1995", "1995_5", "1995_5_12"),
        ("3", "Charlie", "1994", "1994_5", "1994_5_12")
        ]
    
    test_df = spark.createDataFrame(test_data, schema)
    test_delta_path = "test_save_df.delta"
    partition_col = ["year", "month", "day"]
    save_dataframe_to_delta(test_df, test_delta_path, partition_columns=partition_col)
    
    assert os.path.exists(test_delta_path)
    assert os.path.exists(test_delta_path+"/year=1996/month=1996_5/day=1996_5_12")
    assert os.path.exists(test_delta_path+"/year=1995/month=1995_5/day=1995_5_12")
    assert os.path.exists(test_delta_path+"/year=1994/month=1994_5/day=1994_5_12")

def test_read_delta_table(spark):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True)
    ])
    test_data = [
        ("1", "Alice", "1996", "1996_5", "1996_5_12"),
        ("2", "Bob", "1995", "1995_5", "1995_5_12"),
        ("3", "Charlie", "1994", "1994_5", "1994_5_12")
        ]
    test_df = spark.createDataFrame(test_data, schema)

    test_delta_path = "test_read_files.delta"
    test_df.write.format("delta").mode("append").save(test_delta_path)
    read_df = read_delta_table(spark, test_delta_path)
    
    read_df = read_df.orderBy([col(c) for c in read_df.columns])
    test_df = test_df.orderBy([col(c) for c in test_df.columns])
    
    assert read_df is not None
    assert read_df.schema == test_df.schema
    assert read_df.collect() == test_df.collect()

    
def test_table_exists(spark):
    table_path = "test_table_exists.delta"
    
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True)
    ])
    test_data = [
        ("1", "Alice", "1996", "1996_5", "1996_5_12"),
        ("2", "Bob", "1995", "1995_5", "1995_5_12"),
        ("3", "Charlie", "1994", "1994_5", "1994_5_12")
        ]

    test_df = spark.createDataFrame(test_data, schema)
    
    test_df.write.format("delta").mode("overwrite").save(table_path)
    
    delta_table = table_exists(spark, table_path)
    assert delta_table is not None, "The table should exist, but table_exists returned None."
    
    # Clean up the Delta table
    shutil.rmtree(table_path)
    
    # Test if the table does not exist
    delta_table = table_exists(spark, table_path)
    assert delta_table is None, "The table should not exist, but table_exists did not return None."

    
def test_upsert_delta_table(spark):
    ## CREATE TABLE + INSERT
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("sexe", StringType(), True),        
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True)   
    ])
    test_data = [
        ("1", "Alice", "F", "1996", "1996_5", "1996_5_12"),
        ("2", "Bob", "M", "1995", "1995_5", "1995_5_12"),
        ("3", "Charlie", "M", "1994", "1994_5", "1994_5_12")
        ]
    test_df = spark.createDataFrame(data=test_data, schema=schema)
    
    test_delta_path = "test_upsert_table.delta"
    pkey = "id"
    upsert_delta_table(spark, test_delta_path, test_df, pkey)
    read_df = read_delta_table(spark, test_delta_path).drop("effective_time")
    
    theoric_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("sexe", StringType(), True),        
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True),
        StructField("is_current", BooleanType(), True),
        StructField("end_time", TimestampType(), True),        
    ])
    theoric_data = [
        ("1", "Alice", "F", "1996", "1996_5", "1996_5_12", True, None),
        ("2", "Bob", "M", "1995", "1995_5", "1995_5_12", True, None),
        ("3", "Charlie", "M", "1994", "1994_5", "1994_5_12", True, None)
        ]
    theoric_df = spark.createDataFrame(theoric_data, theoric_schema)
    
    read_df = read_df.orderBy([col(c) for c in read_df.columns])
    theoric_df = theoric_df.orderBy([col(c) for c in theoric_df.columns])
    
    assert DeltaTable.forPath(spark, test_delta_path) is not None
    assert read_df.collect() == theoric_df.collect()
    
    ## Upsert
    test_updates_data = [
        ("1", "Axel", "M", "1996", "1996_5", "1996_5_12"),
        ("2", "Bob", "F", "1995", "1995_5", "1995_5_12"),
        ("3", "Charlie", "F", "1994", "1994_5", "1994_5_12")
        ]
    test_update_df = spark.createDataFrame(test_updates_data, schema)
    pkey = "id"
    assert DeltaTable.forPath(spark, test_delta_path) is not None
    upsert_delta_table(spark, test_delta_path, test_update_df, pkey)

    theoric_data = [
        ("1", "Axel", "M", "1996", "1996_5", "1996_5_12", True),
        ("2", "Bob", "F", "1995", "1995_5", "1995_5_12", True),
        ("3", "Charlie", "F", "1994", "1994_5", "1994_5_12", True),
        ("1", "Alice", "F", "1996", "1996_5", "1996_5_12", False),
        ("2", "Bob", "M", "1995", "1995_5", "1995_5_12", False),
        ("3", "Charlie", "M", "1994", "1994_5", "1994_5_12", False)
        ]
    theoric_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("sexe", StringType(), True),        
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True),
        StructField("is_current", BooleanType(), True),
    ])
    theoric_df = spark.createDataFrame(theoric_data, theoric_schema)
    read_df = read_delta_table(spark, test_delta_path).drop("effective_time", "end_time")
    
    read_df = read_df.orderBy([col(c) for c in read_df.columns])
    theoric_df = theoric_df.orderBy([col(c) for c in theoric_df.columns])
    
    assert read_df is not None
    assert read_df.collect() == theoric_df.collect()
    
if __name__ == "__main__":
    pytest.main([__file__])
