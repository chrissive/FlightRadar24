
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
    dag_id = "flight-detail-extraction",
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
    # 'spark.executor.memory': '1g',
    # 'spark.driver.memory': '1g',
    # 'spark.executor.cores': '1', 
    # 'spark.task.cpus': '1',
}

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

extract_flight_job = SparkSubmitOperator(
    task_id="extract-flight-detail",
    conn_id="spark-conn",
    application="etls/extract_flight_details.py",
    conf=conf,
    packages='io.delta:delta-spark_2.12:3.1.0',
    dag=dag
)

# extract_flight_history_job = SparkSubmitOperator(
#     task_id="extract-flight-history",
#     conn_id="spark-conn",
#     application="etls/extract_history_details.py",
#     conf={
#         "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
#         "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
#     },
#     packages='io.delta:delta-spark_2.12:3.1.0',
#     dag=dag
# )

clean_flight = SparkSubmitOperator(
    task_id="clean-flight-radar",
    conn_id="spark-conn",
    application="etls/clean_flight_radar.py",
    conf=conf,    
    packages='io.delta:delta-spark_2.12:3.1.0',
    verbose=True,
    dag=dag
)

current_flight_by_airline = SparkSubmitOperator(
    task_id="current-flight-by-airline",
    conn_id="spark-conn",
    application="etls/get_current_flight_by_airline.py",
    conf=conf,    
    packages='io.delta:delta-spark_2.12:3.1.0',
    dag=dag
)

aircraft_company_flight = SparkSubmitOperator(
    task_id="aircraft-company-flight",
    conn_id="spark-conn",
    application="etls/get_aircraft_company_flight.py",
    conf=conf,
    packages='io.delta:delta-spark_2.12:3.1.0',
    dag=dag
)

regional_flight = SparkSubmitOperator(
    task_id="regional-flight",
    conn_id="spark-conn",
    application="etls/get_regionnal_flight.py",
    conf=conf,     
    packages='io.delta:delta-spark_2.12:3.1.0',
    dag=dag
)

# aircraft_by_airline_country = SparkSubmitOperator(
#     task_id="aircraft-by-airline-country",
#     conn_id="spark-conn",
#     application="etls/get_aircraft_by_airline_country.py",
#     conf=conf,     
#     packages='io.delta:delta-spark_2.12:3.1.0',
#     dag=dag
# )


longgest_flighttime = SparkSubmitOperator(
    task_id="longgest-flighttime",
    conn_id="spark-conn",
    application="etls/get_longgest_flighttime.py",
    conf=conf,
    packages='io.delta:delta-spark_2.12:3.1.0',
    dag=dag
)

longgest_flight_distance = SparkSubmitOperator(
    task_id="longgest-flight-distance",
    conn_id="spark-conn",
    application="etls/get_longgest_flight_distance.py",
    conf=conf,
    packages='io.delta:delta-spark_2.12:3.1.0',
    dag=dag
)


end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> extract_flight_job >> clean_flight >> [current_flight_by_airline, aircraft_company_flight] >> regional_flight >> [longgest_flighttime,longgest_flight_distance] >> end




