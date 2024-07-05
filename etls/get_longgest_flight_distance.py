import sys
import os
import math

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import HOST, CLEANNED_PATH, LONGGEST_FLIGHT_BY_DISTANCE_PATH, AVG_DISTANCE_FLIGHT_BY_CONTINENT_PATH
from utils.logger_util import get_module_logger
from utils.dataframe_util import read_delta_table, save_dataframe_to_delta
from utils.etls_util import get_current_flight

from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, DoubleType
from pyspark.sql.functions import udf, col, current_date, current_timestamp, date_format, unix_timestamp, desc, avg

from delta import *

logger = get_module_logger("Get current flight")

## Probleme d'importation 
def haversine(lat1, lon1, lat2, lon2):
    """Formula of the distance by haversine

    Args:
        lat1 (float): latitude 1
        lon1 (float): longitude 1
        lat2 (float): latitude 2
        lon2 (float): longitude 2

    Returns:
        float: distance
    """
    R = 6371.0  # Rayon de la Terre en kilomètres
    # Convertir les degrés en radians
    
    if None in (lat1, lon1, lat2, lon2):
        logger.info(f"One or more inputs are None: lat1={lat1}, lon1={lon1}, lat2={lat2}, lon2={lon2}")
        return None
    else : 
        lat1 = math.radians(lat1)
        lon1 = math.radians(lon1)
        lat2 = math.radians(lat2)
        lon2 = math.radians(lon2)
    
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        try : 
            a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
            
            distance = R * c
        except Exception as e:
            logger.error(f"There is an error {e}")
            return None

    return distance

@udf(returnType=DoubleType())
def haversine_udf(lat1, lon1, lat2, lon2):
    return haversine(lat1, lon1, lat2, lon2)



if __name__ == "__main__":

    s_conn = None

    try:
        builder = (
            SparkSession.builder.appName("FlightRadar24-LonggestFlightDistance")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.hadoop.fs.defaultFS", f"hdfs://{HOST}:9000") \
            .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
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
    # flight_df = s_conn.read.format("delta").load(CLEANNED_PATH).filter("is_current = true")
    flight_df = get_current_flight(flight_df)
    flight_df = flight_df.drop("origin_airport_website", "destination_airport_website")
    
    logger.info("Get longgest flight distance in current flight")
    current_flight = flight_df.filter((col("on_ground") == 0) & (~col("status_text").contains("Landed")))

    distance_airport_to_airport = current_flight \
        .withColumn("origin_airport_latitude", current_flight["origin_airport_latitude"].cast(DoubleType()))\
        .withColumn("origin_airport_longitude", current_flight["origin_airport_longitude"].cast(DoubleType()))\
        .withColumn("destination_airport_latitude", current_flight["destination_airport_latitude"].cast(DoubleType()))\
        .withColumn("destination_airport_longitude", current_flight["destination_airport_longitude"].cast(DoubleType()))


    distance_airport_to_airport = distance_airport_to_airport\
        .withColumn(
            "distance_km",
            haversine_udf(
                distance_airport_to_airport["origin_airport_latitude"],
                distance_airport_to_airport["origin_airport_longitude"],
                distance_airport_to_airport["destination_airport_latitude"],
                distance_airport_to_airport["destination_airport_longitude"]
                )
            )\
        .orderBy(desc("distance_km"))
        
    distance_airport_to_airport.show(truncate=False)
    
    distance_airport_to_airport = distance_airport_to_airport.withColumn("extract_year", date_format(current_date(), "yyyy")) \
        .withColumn("extract_month", date_format(current_date(), "yyyy_MM")) \
        .withColumn("extract_day", date_format(current_date(), "yyyy_MM_dd")) \
        .withColumn("extract_time", unix_timestamp(current_timestamp().cast(TimestampType())))
    
    distance_airport_to_airport.show(truncate=False)


    logger.info("Saving result.")
    save_dataframe_to_delta(distance_airport_to_airport, LONGGEST_FLIGHT_BY_DISTANCE_PATH, partition_columns=["extract_year", "extract_month", "extract_day", "extract_time"])
    logger.info("Done.")

    
    logger.info("Get average distance flight grouped by continent")
    average_distance_df = distance_airport_to_airport \
        .withColumnRenamed("origin_airport_country_name", "country") \
        .withColumnRenamed("origin_airport_country_code", "country_code") \
        .groupBy("country", "country_code").agg(avg("distance_km").alias("average_flight_length_km"))

    average_distance_df = average_distance_df.withColumn("extract_year", date_format(current_date(), "yyyy")) \
        .withColumn("extract_month", date_format(current_date(), "yyyy_MM")) \
        .withColumn("extract_day", date_format(current_date(), "yyyy_MM_dd")) \
        .withColumn("extract_time", unix_timestamp(current_timestamp().cast(TimestampType())))

    average_distance_df.show(truncate=False)

    logger.info("Saving result.")
    save_dataframe_to_delta(average_distance_df, AVG_DISTANCE_FLIGHT_BY_CONTINENT_PATH, partition_columns=["extract_year", "extract_month", "extract_day", "extract_time"])

    logger.info("Done.")
    s_conn.stop()