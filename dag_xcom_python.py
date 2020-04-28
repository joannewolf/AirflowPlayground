from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def push1(**kwargs):
    # Pushes an XCom without a specific target
    # in order to get kwargs['ti'], operator need to set provide_context=True
    kwargs['ti'].xcom_push(key='key1', value=[1, 2, 3])
    kwargs['ti'].xcom_push(key='key1-2', value=datetime.now())

def push2(**kwargs):
    # Pushes an XCom without a specific target, just by returning it
    return {'key2': 'value2'}

def pull(**kwargs):
    # in order to get kwargs['ti'], operator need to set provide_context=True
    ti = kwargs['ti']
    print "key1 from push1", ti.xcom_pull(key='key1', task_ids='push_python1')
    print "key1-2 from push1", ti.xcom_pull(key='key1-2', task_ids='push_python1')
    print "key2 from push2", ti.xcom_pull(key='key2', task_ids='push_python2') # None
    print ti.xcom_pull(key=None, task_ids=['push_python1', 'push_python2'])

    from_push2 = ti.xcom_pull(key=None, task_ids='push_python2')
    print "key2 from push2 var", from_push2['key2']


default_args = {
    'owner': 'chiao',
    'start_date': days_ago(2),
    'retries': 2,
    'depends_on_past': True
}

dag = DAG(
    dag_id='dag_xcom_python',
    default_args=default_args
    # schedule_interval='0 0 * * *',
)


push_python1 = PythonOperator(
    task_id='push_python1',
    dag=dag,
    python_callable=push1,
    provide_context=True
)

push_python2 = PythonOperator(
    task_id='push_python2',
    dag=dag,
    python_callable=push2
)

pull = PythonOperator(
    task_id='puller',
    dag=dag,
    python_callable=pull,
    provide_context=True
)

pull << [push_python1, push_python2]
