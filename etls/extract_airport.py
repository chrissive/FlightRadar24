import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger_util import get_module_logger
from utils.extraction_util import Extraction

from pyspark.sql import SparkSession


extract_logger = get_module_logger("Extraction")

if __name__ == "__main__":

    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataExtraction airport') \
            .getOrCreate()
        extract_logger.info("Spark connection created successfully!")
    except Exception as e:
        extract_logger.error(f"Couldn't create the spark session due to exception {e}")
        
            
    extract_logger.info("Extraction is being started...")
    extract_logger.info("Airports extraction :")
    
    extraction = Extraction(s_conn)    
    airport_df = extraction.extract_airports()
    
    extract_logger.info("Saving the extraction")
    if airport_df:
        extraction.save_airport(airport_df)
        
    extract_logger.info("Extraction is done.")

    s_conn.stop()