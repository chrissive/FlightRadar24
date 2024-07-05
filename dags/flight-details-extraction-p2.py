
import sys
import os
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import HOST

default_args = {
    'owner': 'Christophe Sive',
    'start_date': datetime(2024, 7, 2, 11, 0),
    'retries': 3,  
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id = "flight-detail-extraction-p2",
    tags=["cicd", "flightradar24"],
    default_args = default_args,
    schedule_interval = "0 * * * *",
    max_active_runs = 1,
    description = "Traitement de l'API FlightRadar24",
)

conf = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.defaultFS": f"hdfs://{HOST}:9000",
    "spark.hadoop.dfs.client.use.datanode.hostname": "true",
}

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

in_out_flight = SparkSubmitOperator(
    task_id="in-out-flight",
    conn_id="spark-conn",
    application="etls/in_out_flight.py",
    conf=conf,
    packages='io.delta:delta-spark_2.12:3.1.0',
    dag=dag
)

aircraft_by_airline_country = SparkSubmitOperator(
    task_id="aircraft-by-airline-country",
    conn_id="spark-conn",
    application="etls/get_aircraft_by_airline_country.py",
    conf=conf,     
    packages='io.delta:delta-spark_2.12:3.1.0',
    dag=dag
)


end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> [in_out_flight, aircraft_by_airline_country] >> end




