import pytest
import sys
import os
from datetime import datetime, timedelta
import pytz
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql.types import StructField, IntegerType, StringType, StructType, BooleanType, TimestampType
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from utils.etls_util import get_current_flight, haversine_udf

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("ETLS_UTILS_TEST") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_get_current_flight(spark):
    current_time = datetime.now(pytz.utc) + timedelta(hours=3)
    current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
    
    data = [
        Row(id=0, scheduled_time_arrival="2024-06-10 12:00:00", estimated_arrival="2024-06-10 11:00:00", is_current=True, end_time=None),
        Row(id=1, scheduled_time_arrival="2024-06-10 12:00:00", estimated_arrival="2024-06-10 11:00:00", is_current=True, end_time=None),
        Row(id=2, scheduled_time_arrival="2024-06-10 12:00:00", estimated_arrival=None, is_current=True, end_time=None),
        Row(id=3, scheduled_time_arrival=None, estimated_arrival="2024-06-10 11:00:00", is_current=True, end_time=None),
        Row(id=4, scheduled_time_arrival=current_time_str, estimated_arrival=current_time_str, is_current=True, end_time=None),
        Row(id=5, scheduled_time_arrival=None, estimated_arrival=current_time_str, is_current=True, end_time=None),
        Row(id=6, scheduled_time_arrival=current_time_str, estimated_arrival=None, is_current=True, end_time=None),
    ]
    
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("scheduled_time_arrival", StringType(), True),
        StructField("estimated_arrival", StringType(), True),
        StructField("is_current", BooleanType(), True),
        StructField("end_time", TimestampType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    result_df = get_current_flight(df)
    result_data = result_df.collect()
    
    expected_ids = {4, 5, 6}
    actual_ids = {row.id for row in result_data}    
    
    assert len(result_data) == 3
    assert actual_ids == expected_ids

def test_haversine_udf(spark):
    data = [
        Row(id=0, lat1=48.8566, lon1=2.3522, lat2=51.5074, lon2=-0.1278),  
        Row(id=1, lat1=40.7128, lon1=-74.0060, lat2=34.0522, lon2=-118.2437),
        Row(id=2, lat1=None, lon1=-74.0060, lat2=34.0522, lon2=-118.2437),
        Row(id=3, lat1=40.7128, lon1=None, lat2=34.0522, lon2=-118.2437),
        Row(id=4, lat1=40.7128, lon1=-74.0060, lat2=None, lon2=-118.2437),
        Row(id=5, lat1=40.7128, lon1=-74.0060, lat2=34.0522, lon2=None),
    ]
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("lat1", DoubleType(), True),
        StructField("lon1", DoubleType(), True),
        StructField("lat2", DoubleType(), True),
        StructField("lon2", DoubleType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    result_df = df.withColumn("distance", haversine_udf(df["lat1"], df["lon1"], df["lat2"], df["lon2"]))
    
    result_data = result_df.collect()
    
    # Vérification des distances calculées
    id0 = result_data[0]["distance"]
    id1 = result_data[1]["distance"]
    
    assert id0 == pytest.approx(343.556, rel=1e-2)
    assert id1 == pytest.approx(3940.069, rel=1e-2)
    assert result_data[2]["distance"] == None
    assert result_data[3]["distance"] == None
    assert result_data[4]["distance"] == None
    assert result_data[5]["distance"] == None

#pytest tests/test_etls_util.py
if __name__ == "__main__":
    pytest.main([__file__])
