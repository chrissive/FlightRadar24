from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, BooleanType, TimestampType

from pyspark.sql.window import Window
from pyspark.sql.functions import unix_timestamp, col, row_number, current_timestamp, lit

from delta import *
import mack

from utils.logger_util import get_module_logger

logger = get_module_logger("DATAFRAME_UTIL")


def read_parquet_files(spark :SparkSession, parquet_path: str, schema :StructType=None) -> DataFrame:
    """
    Read all parquet files from the specified directory.

    Parameters:
        spark (SparkSession) : Spark session
        parquet_path (str): The path to the directory containing parquet files.
        schema (StructType) : The schema to use for reading the parquet files.
    Returns:
        DataFrame: A DataFrame containing the data from all parquet files.
    """
    if schema is None : 
        try :
            parquet_df = spark.read.parquet(parquet_path)
            return parquet_df

        except Exception as e:
            logger.error(f"Une erreur s'est produite lors de la lecture des fichiers parquet : {str(e)}")
            return None
    else:
        try :
            parquet_df = spark.read.schema(schema).parquet(parquet_path)
            return parquet_df

        except Exception as e:
            logger.error(f"Une erreur s'est produite lors de la lecture des fichiers parquet : {str(e)}")
            return None


def save_dataframe_to_parquet(df :DataFrame, file_path :str, partition_columns=None):
    """
    Save a Spark DataFrame to a parquet file using the write method.

    Parameters:
        df (DataFrame): The Spark DataFrame to save.
        file_path (str): The path to save the parquet file.
        partition_columns (list or str): List of column names or single column name to partition by.
    Returns:
        None
    """
    if partition_columns is None : 
        try:
            df.write.parquet(file_path, mode="append")
        except Exception as e:
            logger.error(f"Error occurred while saving DataFrame to parquet: {str(e)}")
    else : 
        try:
            if isinstance(partition_columns, str):
                partition_columns = [partition_columns]
            df.write.partitionBy(*partition_columns).mode("append").parquet(file_path)
        except Exception as e:
            logger.error(f"Error occurred while saving DataFrame to parquet: {str(e)}")
            
def save_dataframe_to_delta(df: DataFrame, file_path: str, partition_columns=None):
    """
    Save a Spark DataFrame to a Delta table.

    Parameters:
        df (DataFrame): The Spark DataFrame to save.
        file_path (str): The path to save the Delta table.
        partition_columns (list or str): List of column names or single column name to partition by.
    Returns:
        None
    """
    try:
        if partition_columns is None:
            try : 
                df.write.format("delta").mode("append").save(file_path)
            except Exception as e:
                logger.error(f"Error occurred while saving DataFrame to Delta table:: {str(e)}")
            
        else:
            if isinstance(partition_columns, str):
                partition_columns = [partition_columns]
            df.write.format("delta").partitionBy(*partition_columns).mode("append").save(file_path)
    except Exception as e:
        logger.error(f"Error occurred while saving DataFrame to Delta table: {str(e)}")


def table_exists(spark: SparkSession, table_path: str):
    try:
        delta_table = DeltaTable.forPath(spark, table_path)
        return delta_table
    except:
        return None
    
def upsert_delta_table(spark: SparkSession, path: str, new_table, key_table: str):
    """Insert or update the delta table with the SCD type2 

    Args:
        spark (SparkSession): Session 
        path (str): path to update or write
        new_table (_type_): table to add 
        key_table (str): primary key of the table
    """
    old_flight_table = table_exists(spark, path)
    
    if old_flight_table :
        logger.info("Saving data, upsert with SCD type2")
        
        df_recent = new_table \
            .withColumn("effective_time", unix_timestamp(current_timestamp()).cast(TimestampType())) \

        exclude_columns = [key_table, 'effective_time']
        col_attr = [col for col in df_recent.columns if col not in exclude_columns]
        
        window_spec = Window.partitionBy(key_table).orderBy(col("effective_time").desc())
        df_with_row_num = df_recent.withColumn("row_num", row_number().over(window_spec))
        df_recent = df_with_row_num.filter(col("row_num") == 1).drop("row_num")

        mack.type_2_scd_upsert(old_flight_table, df_recent, key_table, col_attr)
        
    else :
        logger.info("Saving data as a delta table.")
        
        df_recent = new_table \
            .withColumn("is_current", lit(True).cast(BooleanType())) \
            .withColumn("effective_time", unix_timestamp(current_timestamp()).cast(TimestampType())) \
            .withColumn("end_time", lit(None).cast(TimestampType()))
        
        # partition_col = ["extract_year", "extract_month", "extract_day", "extract_time"]
        save_dataframe_to_delta(df_recent, path, partition_columns=None)


def read_delta_table(spark: SparkSession, path: str, filter: str=False):
    """
    Read delta table from the specified directory.

    Parameters:
        spark (SparkSession) : Spark session
        path (str): The path to the directory containing delta table.
        filter (str) : The condition to use for reading the delta table.
    Returns:
        DataFrame: A DataFrame containing the data from all parquet files.
    """
    if filter is False : 
        try :
            delta_table = spark.read.format("delta").load(path)
            return delta_table

        except Exception as e:
            logger.error(f"Une erreur s'est produite lors de la lecture des fichiers parquet : {str(e)}")
            return None
    else : 
        try : 
            delta_table = spark.read.format("delta").load(path).filter(filter)
            return delta_table
        except Exception as e:
            logger.error(f"Une erreur s'est produite lors de la lecture des fichiers parquet : {str(e)}")
            return None
        