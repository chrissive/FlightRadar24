import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))

HOST = "192.168.1.58" # "192.168.1.58" # "192.168.1.21" #"192.168.0.40"
FLIGHT_EXTRACTION_PATH = parser.get('file_paths', 'flight_extraction_path')
FLIGHT_DETAIL_EXTRACTION_PATH = parser.get('file_paths', 'flight_detail_extraction_path')
FLIGHT_HISTORY_EXTRACTION_PATH = parser.get('file_paths', 'flight_history_extraction_path')
AIRLINE_EXTRACTION_PATH = parser.get('file_paths', 'airline_extraction_path')
AIRPORT_EXTRACTION_PATH = parser.get('file_paths', 'airport_extraction_path')
CLEANNED_PATH = parser.get('file_paths', 'clean_path')
NB_PER_AIRLINE_PATH = parser.get('file_paths', 'flight_by_airline')
FLIGHT_BY_AIRLINE_PATH = parser.get('file_paths', 'flight_by_airline')
REGIONNAL_FLIGHT_PATH = parser.get('file_paths', 'regionnal_flight')
LONGGEST_FLIGHT_BY_TIME_PATH = parser.get('file_paths', 'longgest_flight_by_time')
LONGGEST_FLIGHT_BY_DISTANCE_PATH = parser.get('file_paths', 'longgest_flight_by_distance')
AIRCRAFT_COMPANY_FLIGHTS_PATH = parser.get('file_paths', 'aircraft_company_flights')
AVG_DISTANCE_FLIGHT_BY_CONTINENT_PATH = parser.get('file_paths', 'avg_distance_flight_by_continent')
AIRCRAFT_MODELE_PATH = parser.get('file_paths', 'aircraft_model')
IN_OUT_FLIGHT_PATH = parser.get('file_paths', 'in_out_flight')
