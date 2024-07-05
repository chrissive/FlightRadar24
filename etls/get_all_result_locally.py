import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import HOST
from utils.constants import (
                             AIRCRAFT_MODELE_PATH,
                             AIRCRAFT_COMPANY_FLIGHTS_PATH,
                             NB_PER_AIRLINE_PATH, REGIONNAL_FLIGHT_PATH,
                             LONGGEST_FLIGHT_BY_TIME_PATH,
                             LONGGEST_FLIGHT_BY_DISTANCE_PATH,
                             AVG_DISTANCE_FLIGHT_BY_CONTINENT_PATH,
                             IN_OUT_FLIGHT_PATH
                             )
from utils.logger_util import get_module_logger
from utils.dataframe_util import read_delta_table, save_dataframe_to_delta

from pyspark.sql import SparkSession
from delta import *

logger = get_module_logger("Get current flight")


if __name__ == "__main__":

    s_conn = None

    try:
        builder = (
            SparkSession.builder.appName("FlightRadar24-Results")
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
        
        
    logger.info("Get all results")
    filter = False
    aircraft_model = read_delta_table(s_conn, f"hdfs://{HOST}:9000{AIRCRAFT_MODELE_PATH}")
    aircraft_company_flight = read_delta_table(s_conn, f"hdfs://{HOST}:9000{AIRCRAFT_COMPANY_FLIGHTS_PATH}")
    nb_per_airline = read_delta_table(s_conn, f"hdfs://{HOST}:9000{NB_PER_AIRLINE_PATH}")
    regionnal_flight = read_delta_table(s_conn, f"hdfs://{HOST}:9000{REGIONNAL_FLIGHT_PATH}")
    longgest_flighttime = read_delta_table(s_conn, f"hdfs://{HOST}:9000{LONGGEST_FLIGHT_BY_TIME_PATH}")
    longgest_flight_distance = read_delta_table(s_conn, f"hdfs://{HOST}:9000{LONGGEST_FLIGHT_BY_DISTANCE_PATH}")
    avg_distance_by_continent = read_delta_table(s_conn, f"hdfs://{HOST}:9000{AVG_DISTANCE_FLIGHT_BY_CONTINENT_PATH}")
    in_out_flight = read_delta_table(s_conn, f"hdfs://{HOST}:9000{IN_OUT_FLIGHT_PATH}")

    logger.info("1. Current flight for each airline company")
    first_row = nb_per_airline.first()
    nb_per_airline.show()
    
    logger.info("2. Current regionnal flight")
    regionnal_flight.show()

    logger.info("3.1 Current flight order by loggest flighttime")
    first_row = longgest_flighttime.first()
    longgest_flighttime.show()
    
    logger.info("3.2 Current flight order by loggest flight distance")
    first_row = longgest_flight_distance.first()
    longgest_flight_distance.show()
    
    logger.info("4. Current flight order by average_flight_length (km)")
    avg_distance_by_continent.show()

    logger.info("5. Current flight for each aircraft manufacturer")
    first_row = aircraft_company_flight.first()
    aircraft_company_flight.show()
    
    logger.info("6. Top 3 aircraft model")
    first_three_rows = aircraft_model.head(3)
    # first_three_rows_df = s_conn.createDataFrame(first_three_rows, aircraft_company_flight.schema)
    aircraft_model.show()
    
    logger.info("Bonus Get airline with the most in out flight")
    in_out_flight.show()
    


    
    logger.info("Done.")
    s_conn.stop()