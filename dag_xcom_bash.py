from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'chiao',
    'start_date': days_ago(2),
    'retries': 2,
    'depends_on_past': True
}

dag = DAG(
    dag_id='dag_xcom_bash',
    default_args=default_args
    # schedule_interval='0 0 * * *',
)


push_bash1 = BashOperator(
    task_id='push_bash1',
    bash_command='echo "{{ ti.xcom_push(key="k1", value="v1") }}" "{{ti.xcom_push(key="k1-2", value="[1, 2, 3]") }}"',
    dag=dag,
    provide_context=True
)

push_bash2 = BashOperator(
    task_id='push_bash2',
    bash_command='date',
    xcom_push=True,
    dag=dag
)

pull_command = """
    echo "{{ ti.xcom_pull(key="k1") }}" "{{ ti.xcom_pull(key="k1-2") }}"
    echo "{{ ti.xcom_pull("push_bash2") }}"
    echo "{{ ti.xcom_pull("not_exist") }}"
"""

# template inside script is NOT working, but it can be passed as argument
pull = BashOperator(
    task_id='puller',
    # bash_command='/usr/local/airflow/scripts/print_xcom.sh {{ ti.xcom_pull(key="k1") }}',
    bash_command=pull_command,
    dag=dag,
    provide_context=True
)

pull << [push_bash1, push_bash2]
