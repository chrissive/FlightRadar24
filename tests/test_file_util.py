import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql.types import StructField, StringType, StructType
import pytest
import json
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from utils.file_util import read_json_struct_schema

@pytest.fixture
def temp_json_file(tmp_path):
    # Créer un schéma StructType et le convertir en JSON
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    schema_json = schema.jsonValue()

    # Écrire le JSON dans un fichier temporaire
    file_path = tmp_path / "schema.json"
    with open(file_path, 'w') as f:
        json.dump(schema_json, f)

    yield file_path

    # Le fichier temporaire sera automatiquement supprimé par pytest

def test_read_json_struct_schema(temp_json_file):
    # Appeler la fonction à tester
    schema = read_json_struct_schema(temp_json_file)
    
    # Définir le schéma attendu
    expected_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    
    # Vérifier que le schéma retourné est correct
    assert schema == expected_schema

if __name__ == "__main__":
    pytest.main([__file__])
