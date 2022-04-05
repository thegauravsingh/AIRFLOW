from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta


default_args = {    
    'owner': 'phdevu',
    'depends_on_past': False,
    'start_date': days_ago( 0 ),
    'catchup':False,
    'email': ['shi_cts_psft_hr@transformco.com'],
    'email_on_failure': False,
    'email_on_retry': False,    
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

dag = DAG(
    dag_id='poker-job',
    tags=['hr-it-dev'],
    schedule_interval = None,
    default_args=default_args
    )

start = DummyOperator(task_id='start', dag=dag)

trigger_dag = TriggerDagRunOperator(
        task_id='poking-triggered',
        trigger_dag_id = "aft-sensor",
        conf = {"message" : "triggering AFT-Sensor",
                "filepattern" : "inboundfile.dat",
                "sourcedirpath" : "/home/ssh_user/psin/",  
                "localdirpath" : "/tmp/",
                "mode" : "match",
                "targetdirpath" : "/home/ssh_user/psout/",
                "source_conn_id" : "sftp_sshkeyauth_airflow_devcontainer_devserver_1",
                "target_conn_id" : "sftp_airflow_devcontainer_devserver_1"
                },
        wait_for_completion = True,
        dag=dag,
    )
end = DummyOperator(task_id='end', dag=dag)


start >> trigger_dag >> end