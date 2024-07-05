import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger_util import get_module_logger
from utils.constants import HOST

from utils.extraction_util import Extraction

from pyspark.sql import SparkSession
from delta import *

extract_logger = get_module_logger("Extraction")

if __name__ == "__main__":

    s_conn = None

    try:
        builder = (
            SparkSession.builder.appName("FlightRadar24 FlightDetailsData")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.hadoop.fs.defaultFS", f"hdfs://{HOST}:9000") \
            .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.debug.maxToStringFields", "1000")
        )
        s_conn = configure_spark_with_delta_pip(builder).getOrCreate()
        extract_logger.info("Spark connection created successfully!")
    except Exception as e:
        extract_logger.error(f"Couldn't create the spark session due to exception {e}")
        
    extract_logger.info("Extraction is being started...")
    extract_logger.info("Flights details extraction :")
    
    schema = "etls/schema/flight_detail_data.json"
    extraction = Extraction(s_conn, schema_path=schema)
    flight_detail_df = extraction.extract_flight_details()
    
    flight_detail_df.show()
    
    extract_logger.info("Saving the extraction")
    if flight_detail_df:
        extraction.save_flight_detail(flight_detail_df)
        
    extract_logger.info("Extraction is done.")
    
    s_conn.stop()