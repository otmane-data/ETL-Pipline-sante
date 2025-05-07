from airflow import DAG
from src.utils import *
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from prometheus_client import Counter, Gauge
from statsd import StatsClient
from airflow.configuration import conf


STATSD_HOST = conf.get("metrics", "statsd_host")
STATSD_PORT = conf.get("metrics", "statsd_port")
STATSD_PREFIX = conf.get("metrics", "statsd_prefix")



# # Métriques Prometheus
dag_runs = Counter('sante_dag_runs_total', 'Total number of DAG runs')
task_duration = Gauge('sante_task_duration_seconds', 'Task execution duration')




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': True,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}



# List of dimensions and facts tables to extract
dimensions = [
    'dim_temps', 'dim_patient', 'dim_medecin', 'dim_etablissement',
    'dim_diagnostic', 'dim_medicament'
]

facts = [
    'fact_consultation', 'fact_traitement', 'fact_analyse', 'fact_occupation_etablissement'
]


with DAG(
    'sante_metrics_dag-v1.0.0',
    default_args=default_args,
    schedule_interval='@daily',
    max_active_runs=2,
    tags=['sante', 'data-pipeline'],
    concurrency=5,     
    ) as dag:
    start_task = EmptyOperator(task_id = 'start_task')
    
    # Create extraction tasks for each dimension table
    extraction_tasks = []
    with TaskGroup('extract_dimensions') as extract_dimensions:
        for table in dimensions:
            task = PythonOperator(
                task_id=f'extract_{table}',
                python_callable=extract_table,
                op_kwargs={'table_name': table},
                provide_context=True,
            )
            extraction_tasks.append(task)
    
    # Create extraction tasks for each fact table
    with TaskGroup('extract_facts') as extract_facts:
        for table in facts:
            task = PythonOperator(
                task_id=f'extract_{table}',
                python_callable=extract_table,
                op_kwargs={'table_name': table},
                provide_context=True,
            )
            extraction_tasks.append(task)
    
    # Create cleaning tasks for dimensions
    def create_cleaning_task(table):
        return PythonOperator(
        task_id=f'clean_{table}',
        python_callable=clean_data,
        op_kwargs={'table_name': table},
        provide_context=True,
        )
    
    with TaskGroup('clean_dimensions') as clean_dimensions:
        cleaning_tasks_dim = [
            create_cleaning_task(table) for table in dimensions
        ]
    
    # Create cleaning tasks for facts
    with TaskGroup('clean_facts') as clean_facts:
        cleaning_tasks_facts = [
            create_cleaning_task(table) for table in facts
        ]
    
    with TaskGroup("prepare_data") as prepare_data:
        prepare_dimensions_task = PythonOperator(
            task_id='prepare_dimensions_tables',
            python_callable=dimension_pipeline,
            provide_context=True
        )
        prepare_facts_task = PythonOperator(
            task_id='prepare_fact_tables',
            python_callable=aggregate_daily_data,
            provide_context=True,
            depends_on_past=True
        )
        prepare_dimensions_task >> prepare_facts_task
    
    with TaskGroup("insert_data_in_data_warehouse") as insert_data_in_data_warehouse:
        insert_data_in_dimension_table = PythonOperator(
             task_id='insert_data_in_dimension_table',
             python_callable=insert_data_in_dim_tables,
             provide_context=True
         )
        insert_data_in_fact_table = PythonOperator(
             task_id='insert_data_in_fact_table',
             python_callable=fact_pipeline,
             provide_context=True
         )
        insert_data_in_dimension_table >> insert_data_in_fact_table
        
    end_task = EmptyOperator(task_id ='end_task')
    
    # Define the task dependencies
    start_task >> [extract_dimensions, extract_facts]
    extract_dimensions >> clean_dimensions
    extract_facts >> clean_facts
    [clean_dimensions, clean_facts] >> prepare_data
    prepare_data >> insert_data_in_data_warehouse >> end_task