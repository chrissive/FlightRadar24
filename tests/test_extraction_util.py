import sys
import os
import json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from unittest.mock import patch, MagicMock

from pyspark.sql import SparkSession
from delta import *

from FlightRadar24 import FlightRadar24API
from FlightRadar24.entities.flight import Flight

from utils.extraction_util import Extraction


@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder.appName("FlightRadar24 Test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.debug.maxToStringFields", "1000")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def mock_fr_api():
    mock_api = MagicMock(spec=FlightRadar24API)
    return mock_api

@pytest.fixture
def extraction(spark, mock_fr_api):
    schema_path = "etls/schema/flight_detail_data.json"
    return Extraction(spark=spark, schema_path=schema_path, fr_api=mock_fr_api)

@pytest.fixture
def mock_flights_data():
    return [
        Flight(flight_id="flight1", info=[None, 52.0, 13.0, 90, 35000, 450, "1234", None, "A320", "D-ABCD", "time", "JFK", "LAX", "FL123", True, 500, "CS123", None, "DLH"]),
    ]
    
@pytest.fixture
def mock_flight_detail_data():
    return {
        "info": [None, 52.0, 13.0, 90, 35000, 450, "1234", None, "A320", "D-ABCD", "time", "JFK", "LAX", "FL123", True, 500, "CS123", None, "DLH"],
        "aircraft": {"age": 5, "countryId": 1, "model": {"text": "A320"}, "images": []},
        "airline": {"name": "Lufthansa", "short": "LH"},
        "airport": {
            "destination": {"code": {"icao": "LAX"}, "info": {"baggage": "B", "gate": "G1", "terminal": "T1"}, "position": {"country": {"code": "US", "name": "United States"}, "latitude": 33.9425, "longitude": -118.4081}, "timezone": {"abbr": "PDT", "abbrName": "Pacific Daylight Time", "name": "America/Los_Angeles", "offset": -7, "offsetHours": "-07:00"}},
            "origin": {"code": {"icao": "JFK"}, "info": {"baggage": "A", "gate": "G2", "terminal": "T2"}, "position": {"country": {"code": "US", "name": "United States"}, "latitude": 40.6413, "longitude": -73.7781}, "timezone": {"abbr": "EDT", "abbrName": "Eastern Daylight Time", "name": "America/New_York", "offset": -4, "offsetHours": "-04:00"}}
        },
        "flightHistory": {"aircraft": []},
        "status": {"icon": "icon.png", "text": "On time"},
        "time": {},
        "trail": []
    }


def test_extract_data(spark, tmp_path):
    schema_path = tmp_path / "schema.json"
    
    schema_json = {
        "fields": [
            {"name": "id", "type": "integer", "nullable": True},
            {"name": "name", "type": "string", "nullable": True},
            {"name": "age", "type": "integer", "nullable": True}
        ],
        "type": "struct"
    }

    with open(schema_path, 'w') as f:
        json.dump(schema_json, f)

    data = [{"id": 1, "name": "Alice", "age": 30}, {"id": 2, "name": "Bob", "age": 25}]
    
    extraction = Extraction(spark, str(schema_path))
    df = extraction.extract_data(data)
    
    assert df is not None
    assert df.count() == 2
    assert "extract_year" in df.columns
    assert "extract_month" in df.columns
    assert "extract_day" in df.columns
    assert "extract_time" in df.columns

def test_extract_flight_details(extraction, mock_fr_api, mock_flights_data, mock_flight_detail_data):
    mock_fr_api.get_flights.return_value = mock_flights_data
    mock_fr_api.get_flight_details.return_value = mock_flight_detail_data

    result_df = extraction.extract_flight_details()
    
    assert result_df is not None
    assert result_df.count() == len(mock_flights_data)
    
    result_row = result_df.collect()[0]
    assert result_row.aircraft_code == "A320"
    assert result_row.airline_name == "Lufthansa"
    assert result_row.destination_airport_icao == "LAX"
    
    