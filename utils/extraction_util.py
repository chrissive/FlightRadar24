from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_format, current_timestamp, unix_timestamp
from pyspark.sql.types import TimestampType

from FlightRadar24 import FlightRadar24API
from utils.logger_util import get_module_logger
from utils.dataframe_util import save_dataframe_to_parquet, save_dataframe_to_delta
from utils.file_util import read_json_struct_schema
from utils.constants import (
    FLIGHT_EXTRACTION_PATH,
    AIRLINE_EXTRACTION_PATH,
    AIRPORT_EXTRACTION_PATH,
    FLIGHT_DETAIL_EXTRACTION_PATH,
    FLIGHT_HISTORY_EXTRACTION_PATH
    )

logger = get_module_logger("EXTRACTION_UTILS")



class Extraction:
    def __init__(self, spark: SparkSession, schema_path: str, fr_api=FlightRadar24API()):
        self.spark_session = spark
        self.fr_api = fr_api
        self.schema_path = schema_path

    def extract_data(self, data):
        try:
            schema = read_json_struct_schema(self.schema_path)
            if data:
                data_rdd = self.spark_session.sparkContext.parallelize(data)
                data_df = self.spark_session.createDataFrame(data_rdd, schema=schema)
                data_df = data_df.withColumn("extract_year", date_format(current_date(), "yyyy")) \
                    .withColumn("extract_month", date_format(current_date(), "yyyy_MM")) \
                    .withColumn("extract_day", date_format(current_date(), "yyyy_MM_dd")) \
                    .withColumn("extract_time", unix_timestamp(current_timestamp().cast(TimestampType())))

                return data_df
            else:
                logger.error(f"There is no data.")
                return None

        except Exception as e:
            logger.error(f"Error occurred during data extraction: {str(e)}")
            return None

    def extract_flight_details(self):
        flights = self.fr_api.get_flights()
        flights_list = []
        for flight in flights:
            try:
                flight_details = self.fr_api.get_flight_details(flight)
                flight.set_flight_details(flight_details)
                flights_list.append(flight)
            except:
                continue # continue because we have free version
        return self.extract_data(flights_list)
    
    def extract_flight(self):
        data = self.fr_api.get_flights()
        return self.extract_data(data)
    
    def extract_airlines(self):
        data = self.fr_api.get_airlines()
        return self.extract_data(data)

    def extract_airports(self):
        data = self.fr_api.get_airports()
        return self.extract_data(data)
    
    def save_flight(self, df):
        save_dataframe_to_parquet(df, FLIGHT_EXTRACTION_PATH, partition_columns=["extract_year", "extract_month", "extract_day"])

    def save_airline(self, df):
        save_dataframe_to_parquet(df, AIRLINE_EXTRACTION_PATH, partition_columns=["extract_year", "extract_month", "extract_day"])
        
    def save_airport(self, df):
        save_dataframe_to_parquet(df, AIRPORT_EXTRACTION_PATH, partition_columns=["extract_year", "extract_month", "extract_day"])
        
    def save_flight_detail(self, df):
        save_dataframe_to_delta(df, FLIGHT_DETAIL_EXTRACTION_PATH, partition_columns=["extract_year", "extract_month", "extract_day", "extract_time"])










# #############################################################@
# class Extraction:
#     def __init__(self, spark : SparkSession):
#         self.spark_session = spark

#     def extract_data(self, schema_path, data):
#         try:
#             schema = read_json_struct_schema(schema_path)
#             if data:
#                 data_rdd = self.spark_session.sparkContext.parallelize(data)
#                 data_df = self.spark_session.createDataFrame(data_rdd, schema=schema)
#                 data_df = data_df.withColumn("extract_year", date_format(current_date(), "yyyy")) \
#                     .withColumn("extract_month", date_format(current_date(), "yyyy_MM")) \
#                     .withColumn("extract_day", date_format(current_date(), "yyyy_MM_dd")) \
#                     .withColumn("extract_time", unix_timestamp(current_timestamp().cast(TimestampType()))) \

#                 return data_df
#             else:
#                 logger.error(f"There is no data.")
#                 return None

#         except Exception as e:
#             logger.error(f"Error occurred during data extraction: {str(e)}")
#             return None

#     def extract_flight(self):
#         fr_api = FlightRadar24API()
#         data = fr_api.get_flights()
#         schema_path = "etls/schema/flight_data.json"
#         return self.extract_data(schema_path, data)
    
#     def extract_flight_details(self):
#         fr_api = FlightRadar24API()
#         flights = fr_api.get_flights()
#         flights_list = []
#         for flight in flights:
#             try :
#                 flight_details = fr_api.get_flight_details(flight)
#                 flight.set_flight_details(flight_details)
#                 flights_list.append(flight)
#             except : continue # continue because we have free version
#         schema = "etls/schema/flight_detail_data.json"
#         return self.extract_data(schema, flights_list)

#     def extract_airlines(self):
#         fr_api = FlightRadar24API()
#         data = fr_api.get_airlines()
#         schema_path = "etls/schema/airline_data.json"
#         return self.extract_data(schema_path, data)  

#     def extract_airports(self):
#         fr_api = FlightRadar24API()
#         data = fr_api.get_airports()
#         schema_path = "etls/schema/airport_data.json"
#         return self.extract_data(schema_path, data)
    
#     def save_flight(self, df):
#         save_dataframe_to_parquet(df, FLIGHT_EXTRACTION_PATH, partition_columns=["extract_year", "extract_month", "extract_day"])

#     def save_airline(self, df):
#         save_dataframe_to_parquet(df, AIRLINE_EXTRACTION_PATH, partition_columns=["extract_year", "extract_month", "extract_day"])
        
#     def save_airport(self, df):
#         save_dataframe_to_parquet(df, AIRPORT_EXTRACTION_PATH, partition_columns=["extract_year", "extract_month", "extract_day"])
        
#     def save_flight_detail(self, df):
#         save_dataframe_to_delta(df, FLIGHT_DETAIL_EXTRACTION_PATH, partition_columns=["extract_year", "extract_month", "extract_day", "extract_time"])
    

