from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from datetime import datetime, timedelta
import pprint

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2019, 6, 25),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('zabbix4', default_args=default_args, schedule_interval=timedelta(minutes=1))
conn = PostgresHook(postgres_conn_id='agg2_db_pg')

def query_func(ds, query, **kwargs):
    print(ds)
    print(query)
    result = ''
    for rec in conn.get_records(query):
        result = ''.join(map(str,(rec)))
    return result

sshHook = SSHHook(ssh_conn_id='agg2')

query1 = \
    """
    SELECT write_lag();
    """

query2 = \
    """
    SELECT replay_lag();
    """

query3 = \
    """
    SELECT flush_lag();
    """

query4 = \
    """
    SELECT repl_state();
    """


ds = '{{ ds }}'

command1 = "echo '{0}' > /usr/share/zabgres/write_lag".format(query_func(ds, query1))
command2 = "echo '{0}' > /usr/share/zabgres/replay_lag".format(query_func(ds, query2))
command3 = "echo '{0}' > /usr/share/zabgres/flush_lag".format(query_func(ds, query3))
command4 = "echo '{0}' > /usr/share/zabgres/repl_state".format(query_func(ds, query4))



t1 = SSHOperator(
    task_id="sshtask1",
    command=command1,
    ssh_hook=sshHook,
    remote_host="10.127.33.41",
    dag=dag
 )

t2 = SSHOperator(
    task_id="sshtask2",
    command=command2,
    ssh_hook=sshHook,
    remote_host="10.127.33.41",
    dag=dag
 )

t3 = SSHOperator(
    task_id="sshtask3",
    command=command3,
    ssh_hook=sshHook,
    remote_host="10.127.33.41",
    dag=dag
 )

t4 = SSHOperator(
    task_id="sshtask4",
    command=command4,
    ssh_hook=sshHook,
    remote_host="10.127.33.41",
    dag=dag
 )


t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
