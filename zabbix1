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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('zabbix1', default_args=default_args, schedule_interval=timedelta(minutes=0,hours=8))
conn = PostgresHook(postgres_conn_id='agg2_db')

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
    SELECT
    COUNT(*) FROM
    reports.get_actual_car_info_f(date_trunc('day', now()::timestamp));
    """

query2 = \
    """
    SELECT
    COUNT(*) FROM
    reports.fuel_period_t where report_date = date_trunc('day', now()) + interval '6 hours';
    """

query3 = \
    """
    SELECT
    COUNT(*) FROM
    reports.gps_grid where navigation_date = date_trunc('day', now());
    """

query4 = \
    """
    select
    COUNT(*) FROM
    reports.gps_incidents where report_date = date_trunc('day', now()) + interval '6 hours';
    """
query5 = \
    """
    SELECT
    COUNT(*) FROM
    reports.gps_signal_delay where report_date = date_trunc('day', now()) + interval '6 hours';
    """
query6 = \
    """
    select
    count(*) from
    reports.gps_signal_distribution where report_date = date_trunc('day', now()) + interval '6 hours';
    """

query7 = \
    """
    select
    count(*) from
    reports.no_gps_day where report_date = date_trunc('day', now()) + interval '6 hours';
    """

query8 = \
    """
    select
    count(*) from
    reports.no_gps_stat where report_date = date_trunc('day', now()) + interval '6 hours';
    """

query9 = \
    """
    select
    count(*) from
    reports.similar_tracks where report_date = date_trunc('day', now()) + interval '6 hours';
    """

query10 = \
    """
    select
    count(*) from
    reports.work_equip_t where report_date = date_trunc('day', now()) + interval '6 hours';
    """

ds = '{{ ds }}'

command1 = "echo '{0}' > /usr/share/zabgres/aci".format(query_func(ds, query1))
command2 = "echo '{0}' > /usr/share/zabgres/fp".format(query_func(ds, query2))
command3 = "echo '{0}' > /usr/share/zabgres/gg".format(query_func(ds, query3))
command4 = "echo '{0}' > /usr/share/zabgres/gi".format(query_func(ds, query4))
command5 = "echo '{0}' > /usr/share/zabgres/gsde".format(query_func(ds, query5))
command6 = "echo '{0}' > /usr/share/zabgres/gsdi".format(query_func(ds, query6))
command7 = "echo '{0}' > /usr/share/zabgres/ngd".format(query_func(ds, query7))
command8 = "echo '{0}' > /usr/share/zabgres/ngs".format(query_func(ds, query8))
command9 = "echo '{0}' > /usr/share/zabgres/st".format(query_func(ds, query9))
command10 = "echo '{0}' > /usr/share/zabgres/we".format(query_func(ds, query10))


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

t5 = SSHOperator(
    task_id="sshtask5",
    command=command5,
    ssh_hook=sshHook,
    remote_host="10.127.33.41",
    dag=dag
 )


t6 = SSHOperator(
    task_id="sshtask6",
    command=command6,
    ssh_hook=sshHook,
    remote_host="10.127.33.41",
    dag=dag
 )

t7 = SSHOperator(
    task_id="sshtask7",
    command=command7,
    ssh_hook=sshHook,
    remote_host="10.127.33.41",
    dag=dag
 )

t8 = SSHOperator(
    task_id="sshtask8",
    command=command8,
    ssh_hook=sshHook,
    remote_host="10.127.33.41",
    dag=dag
 )

t9 = SSHOperator(
    task_id="sshtask9",
    command=command9,
    ssh_hook=sshHook,
    remote_host="10.127.33.41",
    dag=dag
 )

t10 = SSHOperator(
    task_id="sshtask10",
    command=command10,
    ssh_hook=sshHook,
    remote_host="10.127.33.41",
    dag=dag
 )

t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
t5.set_upstream(t4)
t6.set_upstream(t5)
t7.set_upstream(t6)
t8.set_upstream(t7)
t9.set_upstream(t8)
t10.set_upstream(t9)
