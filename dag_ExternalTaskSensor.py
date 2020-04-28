from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.utils.dates import days_ago
import datetime

def print_str(input):
    print "string is: {}".format(input)


default_args = {
    'owner': 'chiao',
    'start_date': days_ago(1),
    'retries': 2,
    'depends_on_past': True
}

dag_sensor = DAG(
    dag_id='dag_ExternalTaskSensor_sensor',
    default_args=default_args
)

dag_object = DAG(
    dag_id='dag_ExternalTaskSensor_object',
    default_args=default_args
)

print_object = PythonOperator(
    task_id='print_str',
    python_callable=print_str,
    op_args=['print_str'],
    dag=dag_object
)

# default it's poking the external dag with same execution date
# execution_delta or execution_date_fn can be used to specify the desired execution dates to query
sensor = ExternalTaskSensor(
    task_id='wait',
    external_dag_id='dag_ExternalTaskSensor_object',
    external_task_id='print_str',
    allowed_states=['success', 'failed', 'skipped'],
    # execution_delta=datetime.timedelta(days=1),
    check_existence=True,
    dag=dag_sensor
)

print_sensor = PythonOperator(
    task_id='print_sensor',
    python_callable=print_str,
    op_args=['print_sensor'],
    dag=dag_sensor
)

sensor >> print_sensor
