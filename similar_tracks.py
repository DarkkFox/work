from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from datetime import datetime, timedelta
import pprint

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 10),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('similar_tracks_f', default_args=default_args, schedule_interval='10 7 * * *')
conn = PostgresHook(postgres_conn_id='agg2_db')

def query_func(ds, **kwargs):
    print(ds)
    print(query)
    result = ''
    result = conn.run(query)
    return result

sshHook = SSHHook(ssh_conn_id='agg2')
query = \
    """
    SELECT reports.similar_tracks_f(now()::timestamp);
    """

ds = '{{ ds }}'

t1 = PythonOperator(
    task_id='similar_tracks_f',
    provide_context=True,
    python_callable=query_func,
    op_kwargs={'query': query},
    dag=dag)
