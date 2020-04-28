from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from jinja2 import Template

def print_macro(input, params):
    print "input", input
    print "input", params['param_py']


def print_world(number):
    print "world", number, "{{ ds }}"


default_args = {
    'owner': 'chiao',
    'start_date': days_ago(2),
    'retries': 2,
    'depends_on_past': True
}

dag = DAG(
    dag_id='dag_print_macro',
    default_args=default_args,
    schedule_interval='0 0 * * *',
)

# for BashOperator, macro in command string is working
# but for script file is NOT working
commands = """
    echo "hello {{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo {{ params.param_bash }}
    echo "env $EXECUTION_DATE"
"""
date = "{{ ds }}"

print_hello = BashOperator(
    task_id='print_hello',
    bash_command='/usr/local/airflow/scripts/print_macro.sh ',
    # bash_command=commands,
    params={'param_bash': 'param_bash_val'},
    env={'EXECUTION_DATE': date},
    dag=dag
)
sleep = BashOperator(task_id='sleep', bash_command='sleep 5', dag=dag)
print_hello.set_downstream(sleep)

# for PythonOperator, macro inside args is working
# but macro inside function is NOT working
print_macro_op = PythonOperator(
    task_id='print_macro',
    python_callable=print_macro,
    op_args=["{{ ds }}"],
    params={'param_py': 'param_py_val'},
    dag=dag
)
for i in xrange(3):
    print_world_op = PythonOperator(
        task_id='print_world-' + str(i),
        python_callable=print_world,
        op_args=[i],
        dag=dag
    )
    sleep.set_downstream([print_world_op, print_macro_op])

# macro is not working in bash
print "date", date

# jinja template example
t = Template("Hello {{ something }}!")
print t.render(something="World")
