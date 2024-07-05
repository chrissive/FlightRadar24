import json
from pyspark.sql.types import StructType

from utils.logger_util import get_module_logger


logger = get_module_logger("FILE_UTILS")


def read_json_struct_schema(file_path: str):
    try : 
        with open(file_path, 'r') as f:
            flight_schema_json = json.load(f)
        flight_schema = StructType.fromJson(flight_schema_json)
        
        return flight_schema
    
    except Exception as e:
        logger.error(f"Couldn't extract the schema due to exception {e}")

        
