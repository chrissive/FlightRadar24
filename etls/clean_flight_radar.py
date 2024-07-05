import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import HOST
from utils.constants import FLIGHT_DETAIL_EXTRACTION_PATH, CLEANNED_PATH
from utils.logger_util import get_module_logger
from utils.dataframe_util import upsert_delta_table

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_unixtime, when, isnan, count

from delta import *

# from FlightRadar24 import FlightRadar24API

cleaning_logger = get_module_logger("Cleanning")

def delete_missing_value(df: DataFrame, seuil=0.7)-> DataFrame:
    """Delete columns with more than 70% of missing value

    Args:
        df (DataFrame): dataframe to be treated
        seuil (float, optional): seuil. Defaults to 0.7.

    Returns:
        DataFrame: new dataframe cleanned
    """
    
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            when(
                (col(col_name) == "") | col(col_name).isNull() | isnan(col(col_name)),
                None
                ).otherwise(col(col_name))
            )
    
    nb_total = df.count()
    missing_proportion = df.select(
        [
            (count(when(col(c).isNull(), c)) / nb_total).alias(c) for c in df.columns
            ]
        )
    del_column = [
        colonne for colonne in missing_proportion.columns if missing_proportion.select(colonne).first()[colonne] > seuil
        ]
    clean_df = df.drop(*del_column)
    return clean_df


# def update_schema(schema: str) :
#     """update the old schema 

#     Args:
#         schema (str): old schema

#     Returns:
#         str: new schema
#     """
#     # Suppression des colonnes du schéma
#     for colonne in schema:
#         schema.fields = [field for field in schema.fields if field.name != colonne]

#     cleaning_logger.info("Saving the new JSON schema")
#     schema_json = schema.json()
#     # Enregistrer le schéma JSON dans un fichier
#     with open('flight_schema.json', 'w') as file:
#         file.write(schema_json)
#     cleaning_logger.info("Schema saved.")
#     return schema


# def check_merge_key(df: DataFrame) -> DataFrame:
#     """Delete line where icao and iata is None

#     Args:
#         df (DataFrame): dataframe to checked

#     Returns:
#         DataFrame: new dataframe cleanned
#     """

#     # Drop rows with empty values in specified columns
#     columns_to_check = ['airline_icao', 'destination_airport_iata', 'origin_airport_iata']
#     clean_df = df.dropna(subset=columns_to_check)

#     return clean_df


        
if __name__ == "__main__":

    s_conn = None

    try:
        builder = (
            SparkSession.builder.appName("FlightRadar24 cleanning")
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
        cleaning_logger.info("Spark connection created successfully!")
    except Exception as e:
        cleaning_logger.error(f"Couldn't create the spark session due to exception {e}")
        
     
    cleaning_logger.info("Cleanning is being started...")
    
    cleaning_logger.info("Reading extraction table")
    
    ################# Flight table #################
    
    flight_df = s_conn.read.format("delta").load(FLIGHT_DETAIL_EXTRACTION_PATH)
    flight_df.show()
    new_flight_table = flight_df.drop(
        "aircraft_history",
        "aircraft_age",
        "destination_airport_baggage",
        "destination_airport_gate", 
        "destination_airport_terminal", 
        "origin_airport_baggage", 
        "origin_airport_gate", 
        "origin_airport_terminal",
        "squawk"
        )
    
    
    columns_to_check = ['airline_icao', 'destination_airport_iata', 'origin_airport_iata']
    new_flight_table = new_flight_table.dropna(subset=columns_to_check)

    new_flight_table = new_flight_table.filter(
        (col('airline_icao') != '') &
        (col('destination_airport_iata') != '') &
        (col('origin_airport_iata') != '') &
        (col('airline_icao') != 'N/A') &
        (col('destination_airport_iata') != 'N/A') &
        (col('origin_airport_iata') != 'N/A')
    )

    new_flight_table = new_flight_table.selectExpr(
        "*",
        "time_details['scheduled']['departure'] as scheduled_time_departure",
        "time_details['real']['departure'] as real_time_departure",
        "time_details['scheduled']['arrival'] as scheduled_time_arrival",
        "time_details['estimated']['arrival'] as estimated_arrival",
        "time_details['historical']['flighttime'] as historical_flighttime",
        "time_details['historical']['delay'] as historical_delay",
        "time_details['other']['updated'] as time_updated",
        "time_details['other']['eta'] as time_eta",
    ).withColumn("estimated_arrival", from_unixtime("estimated_arrival", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("time_updated", from_unixtime("time_updated", "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("time_eta", from_unixtime("time_eta", "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("real_time_departure", from_unixtime("real_time_departure", "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("scheduled_time_arrival", from_unixtime("scheduled_time_arrival", "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("scheduled_time_departure", from_unixtime("scheduled_time_departure", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("time", from_unixtime("time", "yyyy-MM-dd HH:mm:ss"))
        
    new_flight_table = new_flight_table.drop("time_details")

    new_flight_table.show()
    cleaning_logger.info("Done.")


    upsert_delta_table(s_conn, CLEANNED_PATH, new_flight_table, "id")
    
    # old_flight_table = table_exists(s_conn, CLEANNED_PATH)
    
    # if old_flight_table :
    #     cleaning_logger.info("Saving data, upsert with SCD type2")
        
    #     df_recent = new_flight_table \
    #         .withColumn("effective_time", unix_timestamp(current_timestamp()).cast(TimestampType())) \

    #     exclude_columns = ['id', 'effective_time']
    #     col_attr = [col for col in df_recent.columns if col not in exclude_columns]
        
    #     window_spec = Window.partitionBy("id").orderBy(col("effective_time").desc())
    #     df_with_row_num = df_recent.withColumn("row_num", row_number().over(window_spec))
    #     df_recent = df_with_row_num.filter(col("row_num") == 1).drop("row_num")

    #     mack.type_2_scd_upsert(old_flight_table, df_recent, "id", col_attr)

    # else :
    #     cleaning_logger.info("Saving data as a delta table.")
        
    #     df_recent = new_flight_table \
    #         .withColumn("is_current", lit(True).cast(BooleanType())) \
    #         .withColumn("effective_time", unix_timestamp(current_timestamp()).cast(TimestampType())) \
    #         .withColumn("end_time", lit(None).cast(TimestampType()))
        
    #     partition_col = ["extract_year", "extract_month", "extract_day", "extract_time"]
    #     df_recent.write.format("delta").mode("append").save(CLEANNED_PATH)
    cleaning_logger.info("Done.")

    s_conn.stop()