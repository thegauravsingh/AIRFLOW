import os, re, logging
from typing import Dict
from datetime import timedelta

from airflow.decorators import dag,task

from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.python import PythonSensor

from airflow.utils.dates import days_ago
from airflow.utils.decorators import apply_defaults

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.hooks.sftp  import SFTPHook
from airflow.providers.ssh.hooks.ssh  import SSHHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.exceptions import AirflowSensorTimeout


def _failure_call(context):
    if isinstance(context['exception'], AirflowSensorTimeout):
        logging.info(context)
        logging.info("File Watcher timed out")  

class AFTSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, sftp_conn_id='sftp_default', *args, **kwargs): # pylint: disable=keyword-arg-before-vararg
        super(AFTSensor, self).__init__(*args, **kwargs) # pylint: disable=super-with-arguments
        self.hook = None
       
    def poke(self, context): # pylint: disable=unused-argument
        print('message ', context['dag_run'].conf['message'])
        print('filepattern ', context['dag_run'].conf['filepattern'])
        print('mode ', context['dag_run'].conf['mode'])
        print('sourcedirpath ', context['dag_run'].conf['sourcedirpath'])
        print('localdirpath ', context['dag_run'].conf['localdirpath'])
        print('targetdirpath ', context['dag_run'].conf['targetdirpath'])
        print('source_conn_id ', context['dag_run'].conf['source_conn_id'])
        print('target_conn_id ', context['dag_run'].conf['target_conn_id'])
        self.path = context['dag_run'].conf['sourcedirpath']
        self.mode = context['dag_run'].conf['mode']
        self.filepattern =  context['dag_run'].conf['filepattern']
        self.sftp_conn_id = context['dag_run'].conf['source_conn_id']

        self.hook = SFTPHook(self.sftp_conn_id)
        self.log.info('Poking for %s', self.path)
        try:
            files_list = self.hook.list_directory(self.path)
            match_pattern = re.compile(self.filepattern)
            selected_files = []
            self.log.info("Number of files in Directory {}".format(len(files_list)))
            print("List of files in Directory: ",files_list)
            print("context: ",context)
            for file in files_list:
                if self.mode == 'match':
                    if re.match(match_pattern,file):
                        print("files matching pattern: ",file)
                        selected_files.append(file)
                #if self.mode == 'latest':
                #    pass        
                else:
                    return False
            print('selected_files', selected_files)
            context['ti'].xcom_push(key = self.filepattern,value = selected_files)    
            return True
        except IOError as e: # pylint: disable=invalid-name
            if e.errno != SFTP_NO_SUCH_FILE:
                raise e
            return False
        finally:
            self.hook.close_conn()

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


@dag(dag_id='aft-sensor',tags=['hr-it-dev'],schedule_interval = None, default_args=default_args)
def taskflow():

    @task
    def start(msg,**kwargs):
        logging.info(kwargs['dag_run'].conf.get('message'))
   
    #   tmpstore_ssh_keys = BashOperator(
    #              task_id='tmpstore-ssh-keys',      
    #               bash_command="aws secretsmanager get-secret-value --secret-id phdevu-airflow-secret-key-working --query 'SecretString' --output text |base64 --decode > /tmp/phdevu_airflow_private.pem",
    #               wait_for_downstream=True)

    wait_for_files = AFTSensor(task_id="wait-for-file",
                    poke_interval=60,
                    timeout=60*3,
                    soft_fail=True,
                    on_failure_callback=_failure_call)
   
    @task
    def download_files(**kwargs):

        filepattern = kwargs['dag_run'].conf.get('filepattern')
        sourcedirpath = kwargs['dag_run'].conf.get('sourcedirpath')  
        localdirpath =  kwargs['dag_run'].conf.get('localdirpath')
        source_conn_id = kwargs['dag_run'].conf.get('source_conn_id')



        files_name = kwargs['ti'].xcom_pull(task_ids="wait-for-file", key = filepattern)
        print('files_name ',files_name)

        for file in files_name:
            remote_filepath = os.path.join(os.path.dirname(sourcedirpath),file)
            local_filepath = os.path.join(os.path.dirname(localdirpath),file)
            print("remote_filepath " ,remote_filepath )
            print("local_filepath ",local_filepath)
       
            download_file = SFTPOperator(
                            task_id="downloading_" + file,
                            ssh_conn_id=source_conn_id,
                            local_filepath=local_filepath,
                            remote_filepath=remote_filepath,
                            operation="get"
                            )
            download_file.execute(dict())
    @task
    def upload_files(**kwargs):
        filepattern = kwargs['dag_run'].conf.get('filepattern')
        targetdirpath = kwargs['dag_run'].conf.get('targetdirpath')  
        localdirpath =  kwargs['dag_run'].conf.get('localdirpath')
        target_conn_id = kwargs['dag_run'].conf.get('target_conn_id')

        files_name = kwargs['ti'].xcom_pull(task_ids="wait-for-file", key = filepattern)
        print('files_name ',files_name)
        for file in files_name:
            remote_filepath = os.path.join(os.path.dirname(targetdirpath),file)
            local_filepath = os.path.join(os.path.dirname(localdirpath),file)
            print("remote_filepath " ,remote_filepath )
            print("local_filepath ",local_filepath)
       
            upload_file = SFTPOperator(
                            task_id="uploading_" + file,
                            ssh_conn_id=target_conn_id,
                            local_filepath=local_filepath,
                            remote_filepath=remote_filepath,
                            operation="put"
                            )
            upload_file.execute(dict())    
    @task
    def end(msg:str):
        logging.info(msg)

    #end("Dag Starts Here") << upload_files() << download_files() << wait_for_files << tmpstore_ssh_keys << start("Dag Starts Here")
    end("Dag Starts Here") << upload_files() << download_files() << wait_for_files << start("Dag Starts Here")

dag = taskflow()