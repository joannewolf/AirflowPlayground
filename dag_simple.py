from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_world(number):
    print "world", number


default_args = {
    'owner': 'chiao',
    'start_date': datetime(2020, 1, 2),
    'end_date': datetime(2020, 1, 5),
    'retries': 2,
    'retry_delay': timedelta(minutes=20),
    'depends_on_past': True
    # 'pool': 'analyst_layer',
    # 'queue': 'technical_analytics',
    # 'concurrency': 200
}

dag = DAG(
    dag_id='dag_simple',
    default_args=default_args,
    # catchup=False,
    # schedule_interval=None,
    # dagrun_timeout=timedelta(hours=1),
    # max_active_runs=5,
    # concurrency=5,
    # is_paused_upon_creation=False
)

with dag:
    print_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo "hello"'
    )
    sleep = BashOperator(
        task_id='sleep',
        bash_command='sleep 5'
    )
    print_world = PythonOperator(
        task_id='print_world',
        python_callable=print_world,
        op_args=[1]
    )
print_hello >> sleep >> print_world
