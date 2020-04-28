from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
import random

def print_number(num):
    print "number is: {}".format(num)

def choose_random_branch(**kwargs):
    branches = ['branch_0', 'branch_1', 'branch_2']
    # branches = ['branch_not_exist']
    kwargs['ti'].xcom_push(key='branch', value=random.choice(branches))

def get_branch(**kwargs):
    ti = kwargs['ti']
    branch = ti.xcom_pull(key='branch')
    print "branch chosen: {}".format(branch)
    return branch


default_args = {
    'owner': 'chiao',
    'start_date': days_ago(1),
    'retries': 2,
    'depends_on_past': True
}

dag = DAG(
    dag_id='dag_branch',
    default_args=default_args
)

with dag:
    decide_branch = PythonOperator(
        task_id='decide_branch',
        python_callable=choose_random_branch,
        provide_context=True
    )
    # python_callable needs a task_id value, it'll error out if returned task_id not found
    # other unchoosen branches are skipped
    # if provide_context=True, it can use with XCom to decide which branch to choose
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=get_branch,
        provide_context=True
    )
    decide_branch >> branching

    for i in xrange(3):
        print_number_op = PythonOperator(
            task_id='branch_{}'.format(i),
            python_callable=print_number,
            op_kwargs={'num': i},
            dag=dag
        )
        branching >> print_number_op
