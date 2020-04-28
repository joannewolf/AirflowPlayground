from datetime import datetime, timedelta
from airflow import DAG


def dag_function():
    dag = DAG(
        dag_id='dag_in_function',
        default_args=default_args,
        schedule_interval='0 0 * * *'
    )
    return dag


default_args = {
    'owner': 'chiao',
    'start_date': datetime(2020, 1, 2),
    'retries': 2,
    'depends_on_past': True
}

# dag created in function must be in the global namespace
dag = dag_function()
globals()['dag_in_function'] = dag_function()