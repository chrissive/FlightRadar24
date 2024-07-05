import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.dagbag import DagBag
from prometheus_client import start_http_server, Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import HOST
from utils.logger_util import get_module_logger

logger = get_module_logger("prometheus_metrics")
start_http_server(9091)
dag_bag = DagBag()

METRIC_JOB_SUCCESS = Counter('airflow_job_success', 'Number of successful Airflow jobs')
METRIC_JOB_FAILURE = Counter('airflow_job_failure', 'Number of failed Airflow jobs')

def job_success(context):
    METRIC_JOB_SUCCESS.inc()
    logger.info("Job Success Metric Incremented")

def job_failure(context):
    METRIC_JOB_FAILURE.inc()
    logger.info("Job Failure Metric Incremented")

dag_bag = DagBag()

for dag_id, dag in dag_bag.dags.items():
    if isinstance(dag, DAG):
        dag.on_success_callback = job_success
        dag.on_failure_callback = job_failure
        logger.info(f"Callbacks added for DAG: {dag_id}")
        
        
        
default_args = {
    'owner': 'Christophe Sive',
    'start_date': datetime(2024, 6, 26, 11, 0),
    'retries': 3,  
    'retry_delay': timedelta(minutes=1),
    'on_success_callback': job_success,
    'on_failure_callback': job_failure,
}

dag = DAG(
    dag_id = "get-metrics",
    tags=["cicd", "metrics"],
    default_args = default_args,
    schedule_interval = "0 * * * *",
    max_active_runs = 1,
    description='Enregistrement des métriques pour tous les DAGs',
)


start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

airflow_metrics = PythonOperator(
    task_id='airflow-metrics',
    python_callable = lambda: print("Recording Airflow metrics"),
    on_success_callback=job_success,
    on_failure_callback=job_failure,
    dag=dag,
)


end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> airflow_metrics >> end
