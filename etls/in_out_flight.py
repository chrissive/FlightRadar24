import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import HOST
from utils.constants import CLEANNED_PATH, IN_OUT_FLIGHT_PATH
from utils.logger_util import get_module_logger
from utils.dataframe_util import read_delta_table, save_dataframe_to_delta
from utils.etls_util import get_current_flight

from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col, count, current_date, current_timestamp, date_format, unix_timestamp, abs

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
    
    flight_df = read_delta_table(s_conn, CLEANNED_PATH)
    flight_df = get_current_flight(flight_df)

    # flight_df = s_conn.read.format("delta").load(CLEANNED_PATH).filter("is_current = true")
    
    logger.info("Get airline with the most in out flight")
    
    outbound_flights = flight_df.groupBy("origin_airport_iata", "origin_airport_name").agg(count("*").alias("out_flight_count"))
    inbound_flights = flight_df.groupBy("destination_airport_iata", "destination_airport_name").agg(count("*").alias("in_flight_count"))

    inbound_flights = inbound_flights.withColumnRenamed("destination_airport_iata", "airport_iata")
    inbound_flights = inbound_flights.withColumnRenamed("destination_airport_name", "airport_name")
    outbound_flights = outbound_flights.withColumnRenamed("origin_airport_iata", "airport_iata")
    outbound_flights = outbound_flights.withColumnRenamed("origin_airport_name", "airport_name")

    airport_flight_counts = outbound_flights.join(inbound_flights, ["airport_iata", "airport_name"], "outer")
    airport_flight_counts = airport_flight_counts.na.fill(0)

    airport_flight_counts = airport_flight_counts\
        .withColumn("difference", abs(col("out_flight_count") - col("in_flight_count"))) \
        .orderBy(col("difference").desc())

    max_diff_airport = airport_flight_counts.first()
    print(f"Aéroport avec la plus grande différence: {max_diff_airport['airport_iata']} avec une différence de {max_diff_airport['difference']} vols.")

    
    airport_flight_counts = airport_flight_counts.withColumn("extract_year", date_format(current_date(), "yyyy")) \
        .withColumn("extract_month", date_format(current_date(), "yyyy_MM")) \
        .withColumn("extract_day", date_format(current_date(), "yyyy_MM_dd")) \
        .withColumn("extract_time", unix_timestamp(current_timestamp().cast(TimestampType())))
    
    airport_flight_counts.show(truncate=False)


    logger.info("Saving result.")

    save_dataframe_to_delta(airport_flight_counts, IN_OUT_FLIGHT_PATH, partition_columns=["extract_year", "extract_month", "extract_day", "extract_time"])
    
    logger.info("Done.")
    s_conn.stop()