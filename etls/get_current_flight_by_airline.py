import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import HOST
from utils.constants import CLEANNED_PATH, NB_PER_AIRLINE_PATH
from utils.logger_util import get_module_logger
from utils.dataframe_util import read_delta_table, save_dataframe_to_delta
from utils.etls_util import get_current_flight

from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col, current_date, current_timestamp, date_format, unix_timestamp, count

from delta import *

logger = get_module_logger("Get current flight")

        
if __name__ == "__main__":

    s_conn = None

    try:
        builder = (
            SparkSession.builder.appName("FlightRadar24-CurrentFlightData")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.hadoop.fs.defaultFS", f"hdfs://{HOST}:9000") \
            .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")\
            .config("spark.sql.shuffle.partitions", "2")\
            .config("spark.sql.debug.maxToStringFields", "1000")\
            .config("spark.sql.parquet.columnarReaderBatchSize", "2048") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .config("spark.sql.sources.bucketing.enabled", "false")
        )
        s_conn = configure_spark_with_delta_pip(builder).getOrCreate()
        logger.info("Spark connection created successfully!")
    except Exception as e:
        logger.error(f"Couldn't create the spark session due to exception {e}")
        
    logger.info("Reading cleaning table")
    
    filter = "is_current = true"
    flight_df = read_delta_table(s_conn, CLEANNED_PATH, filter)
    flight_df = get_current_flight(flight_df)
    
    # flight_df = s_conn.read.format("delta").load(CLEANNED_PATH).filter("is_current = true")
    
    logger.info("Get the company with the most current flight")
    current_flight = flight_df.filter((col("on_ground") == 0) & (~col("status_text").contains("Landed")))

    airline_current_flight = current_flight.groupBy("airline_name", "airline_iata") \
        .agg(count("*").alias("nb_current_flight")) \
        .sort("nb_current_flight", ascending = False)
        
        
    airline_current_flight = airline_current_flight.withColumn("extract_year", date_format(current_date(), "yyyy")) \
        .withColumn("extract_month", date_format(current_date(), "yyyy_MM")) \
        .withColumn("extract_day", date_format(current_date(), "yyyy_MM_dd")) \
        .withColumn("extract_time", unix_timestamp(current_timestamp().cast(TimestampType())))
    
    airline_current_flight.show(truncate=False)


    save_dataframe_to_delta(airline_current_flight, NB_PER_AIRLINE_PATH, partition_columns=["extract_year", "extract_month", "extract_day", "extract_time"])

    # upsert_delta_table(s_conn, NB_PER_AIRLINE_PATH, airline_current_flight, "airline_iata")
    
    logger.info("Done.")
    s_conn.stop()