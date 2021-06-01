from datetime import *
from airflow.example_dags.libr import airflow-functions as libr1
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initi
from airflow.models import Variable


default_args = {
    'owner': 'taffazzel',
    'start_date': datetime(2020, 3, 17, 4, 0),
     #'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

#each of the functions are called with the argument clusterId and the region
def setPythonEnvironment():
    libr1.setPython(Variable.get("ClusterId"),'eu-west-1')

def localFolder():
    libr1.localFolderCreate(Variable.get("ClusterId"),'eu-west-1')


def copyApplication():
    libr1.copyApplication(Variable.get("ClusterId"),'eu-west-1')

def setMod():
    libr1.setMod(Variable.get("ClusterId"),'eu-west-1')

def runApplication():
    libr1.runApplication(Variable.get("ClusterId"),'eu-west-1')

dag = DAG(
        'run-on-EMR',
        default_args=default_args,
        description='It creates EMR cluster, runs some steps to prepare then execute the pysaprk application',
        schedule_interval='0 0 * * *', #runs daily at 00.00
    )




setPython_task = PythonOperator(task_id='setPythonEnve',python_callable=setPythonEnvironment,dag=dag)
localFolder_ready_task = PythonOperator(task_id='local_folder_create_cluster',python_callable=localFolder(),dag=dag)
copyApplication_to_local_task = PythonOperator(task_id='CopyApplicationfromS3toLocal',python_callable=copyApplication(),dag=dag)
set_permission_task = PythonOperator(task_id='Create_EMR_runJobs',python_callable=setMod(),dag=dag)
run_application_task = PythonOperator(task_id='Create_EMR_runJobs',python_callable=runApplication(),dag=dag)

setPython_task >> localFolder_ready_task >> copyApplication_to_local_task >> set_permission_task >> run_application_task

