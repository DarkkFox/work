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
    'start_date': datetime(2020, 3, 13),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('refresh_sensors', default_args=default_args, schedule_interval='0 * * * *')
conn = PostgresHook(postgres_conn_id='agg2_db')
conn.autocommit = True

def query_func(ds, **kwargs):
    print(ds)
    print(query)
    result = ''
    result = conn.run(query)
    return result

sshHook = SSHHook(ssh_conn_id='agg2')
query = \
    """
    REFRESH MATERIALIZED VIEW charger.estp_sensors;
    """

ds = '{{ ds }}'

t1 = PythonOperator(
    task_id='refresh_sensors',
    provide_context=True,
    python_callable=query_func,
    op_kwargs={'query': query},
    dag=dag)
