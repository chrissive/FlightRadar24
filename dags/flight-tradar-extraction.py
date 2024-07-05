
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


default_args = {
    'owner': 'Christophe Sive',
    'start_date': datetime(2024, 4, 17)
}

dag = DAG(
    dag_id = "flight-tradar-extraction",
    default_args = default_args,
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

extract_flight_job = SparkSubmitOperator(
    task_id="extract-flight-radar",
    conn_id="spark-conn",
    application="etls/extract_flight.py",
    dag=dag
)


end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> [extract_flight_job] >> end








