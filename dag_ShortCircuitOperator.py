from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago

def is_even_number(num):
    # return str(num)
    return (num % 2 == 0)

def print_number(num):
    print "number is: {}".format(num)


default_args = {
    'owner': 'chiao',
    'start_date': days_ago(1),
    'retries': 2,
    'depends_on_past': True
}

dag = DAG(
    dag_id='dag_ShortCircuitOperator',
    default_args=default_args
)

for i in xrange(3):
    # if ShortCircuitOperator function return False, it'll skip all downstream tasks; else it'll continue 
    # if ShortCircuitOperator function return non-Boolean, it'll continue
    check_number_op = ShortCircuitOperator(
        task_id='check_number-' + str(i),
        python_callable=is_even_number,
        op_args=[i],
        dag=dag
    )

    print_number_op = PythonOperator(
        task_id='print_number-' + str(i),
        python_callable=print_number,
        op_kwargs={'num': i},
        dag=dag
    )

    check_number_op >> print_number_op
