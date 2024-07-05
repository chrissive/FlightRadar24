from datetime import datetime
import pytz
import math

from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType, DoubleType

from pyspark.sql.functions import when, col, lit
from pyspark.sql.functions import udf

from utils.logger_util import get_module_logger

logger = get_module_logger("ETLS_UTILS")

def get_current_flight(delta_table_df: DataFrame)->DataFrame:
    """update about the date of our data

    Args:
        delta_table_df (DataFrame): delta table to clean and update 

    Returns:
        DataFrame: updated table
    """
    current_time = datetime.now(pytz.utc)
    current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
    
    df = delta_table_df.withColumn(
        "arrival_time",
        when(col("estimated_arrival").isNotNull(), col("estimated_arrival")).otherwise(col("scheduled_time_arrival"))
    ).withColumn(
        "is_current",
        when(col("arrival_time") < current_time_str, lit(False).cast(BooleanType())).otherwise(lit(True).cast(BooleanType()))
    ).withColumn(
        "end_time",
        when(col("arrival_time") < current_time_str, current_time_str).otherwise(lit(None))
    ).drop("arrival_time")
    return df.filter("is_current = true")


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


