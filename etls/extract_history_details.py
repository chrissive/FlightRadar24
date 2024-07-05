import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger_util import get_module_logger
from utils.constants import FLIGHT_HISTORY_EXTRACTION_PATH, FLIGHT_EXTRACTION_PATH
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

from delta import *

extract_logger = get_module_logger("Extraction")

if __name__ == "__main__":

    s_conn = None
    try:
        builder = (
            SparkSession.builder.appName("FlightRadar24 FlightHistoryDetailsData")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.debug.maxToStringFields", "1000")

        )
        s_conn = configure_spark_with_delta_pip(builder).getOrCreate()
        extract_logger.info("Spark connection created successfully!")
    except Exception as e:
        extract_logger.error(f"Couldn't create the spark session due to exception {e}")
        
    extract_logger.info("Extraction is being started...")
    extract_logger.info("Flights details extraction :")
    
    flight_df = s_conn.read.format("delta").load(FLIGHT_EXTRACTION_PATH)

    aircraft_table = flight_df.selectExpr(
        "id as current_id", 
        "aircraft_code",
        "aircraft_country_id",
        "aircraft_model",
        "aircraft_history",
        "extract_year",
        "extract_month",
        "extract_day",
        "extract_time"
    )
    
    aircraft_table_exploded = aircraft_table.withColumn("aircraft_history", explode(aircraft_table["aircraft_history"]))
    
    aircraft_table_exploded_flat = aircraft_table_exploded.selectExpr(
        "*",
        "aircraft_history['identification']['number']['default'] as aircraft_default",
        "aircraft_history['identification']['id'] as aircraft_identification",
        "aircraft_history['time']['real']['departure'] as aircraft_departure_time",
        "aircraft_history['airport']['destination']['website'] as aircraft_destination_website",
        "aircraft_history['airport']['destination']['visible'] as aircraft_destination_visible",
        "aircraft_history['airport']['destination']['code']['iata'] as aircraft_destination_iata",
        "aircraft_history['airport']['destination']['code']['icao'] as aircraft_destination_icao",
        "aircraft_history['airport']['destination']['timezone']['offset'] as aircraft_destination_timezone_offset",
        "aircraft_history['airport']['destination']['timezone']['offsetHours'] as aircraft_destination_timezone_offsetHours",
        "aircraft_history['airport']['destination']['timezone']['isDst'] as aircraft_destination_timezone_isDst",
        "aircraft_history['airport']['destination']['timezone']['abbrName'] as aircraft_destination_timezone_abbrName",
        "aircraft_history['airport']['destination']['timezone']['name'] as aircraft_destination_timezone_name",
        "aircraft_history['airport']['destination']['timezone']['abbr'] as aircraft_destination_timezone_abbr",
        "aircraft_history['airport']['destination']['name'] as aircraft_destination_name",
        "aircraft_history['airport']['destination']['position']['country']['name'] as aircraft_destination_country",
        "aircraft_history['airport']['destination']['position']['country']['codeLong'] as aircraft_destination_codeLong",
        "aircraft_history['airport']['destination']['position']['country']['code'] as aircraft_destination_code",
        "aircraft_history['airport']['destination']['position']['country']['id'] as aircraft_destination_country_id",
        "aircraft_history['airport']['destination']['position']['altitude'] as aircraft_destination_position_altitude",
        "aircraft_history['airport']['destination']['position']['region']['city'] as aircraft_destination_position_region_city",
        "aircraft_history['airport']['destination']['position']['latitude'] as aircraft_destination_position_latitude",
        "aircraft_history['airport']['destination']['position']['longitude'] as aircraft_destination_position_longitude",
        "aircraft_history['airport']['origin']['website'] as aircraft_origin_website",
        "aircraft_history['airport']['origin']['visible'] as aircraft_origin_visible",
        "aircraft_history['airport']['origin']['code']['iata'] as aircraft_origin_iata",
        "aircraft_history['airport']['origin']['code']['icao'] as aircraft_origin_icao",
        "aircraft_history['airport']['origin']['timezone']['offset'] as aircraft_origin_timezone_offset",
        "aircraft_history['airport']['origin']['timezone']['offsetHours'] as aircraft_origin_timezone_offsetHours",
        "aircraft_history['airport']['origin']['timezone']['isDst'] as aircraft_origin_timezone_isDst",
        "aircraft_history['airport']['origin']['timezone']['abbrName'] as aircraft_origin_timezone_abbrName",
        "aircraft_history['airport']['origin']['timezone']['name'] as aircraft_origin_timezone_name",
        "aircraft_history['airport']['origin']['timezone']['abbr'] as aircraft_origin_timezone_abbr",
        "aircraft_history['airport']['origin']['name'] as aircraft_origin_name",
        "aircraft_history['airport']['origin']['position']['country']['name'] as aircraft_origin_country",
        "aircraft_history['airport']['origin']['position']['country']['codeLong'] as aircraft_origin_codeLong",
        "aircraft_history['airport']['origin']['position']['country']['code'] as aircraft_origin_country_code",
        "aircraft_history['airport']['origin']['position']['country']['id'] as aircraft_origin_country_id",
        "aircraft_history['airport']['origin']['position']['altitude'] as aircraft_origin_position_altitude",
        "aircraft_history['airport']['origin']['position']['region']['city'] as aircraft_origin_position_region_city",
        "aircraft_history['airport']['origin']['position']['latitude'] as aircraft_origin_position_latitude",
        "aircraft_history['airport']['origin']['position']['longitude'] as aircraft_origin_position_longitude" 
    ).drop("aircraft_history")

    extract_logger.info("Saving the extraction")
    partition_col = ["extract_year", "extract_month", "extract_day", "extract_time"]
    aircraft_table_exploded_flat.write.format("delta").partitionBy(*partition_col).mode("append").save(FLIGHT_HISTORY_EXTRACTION_PATH)
    extract_logger.info("Extraction is done.")
    
    s_conn.stop()