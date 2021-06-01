import boto3, json, pprint, requests, textwrap, time, logging, requests
from datetime import datetime
import os


def setPython(cluster_id,region_name):
    emr = boto3.client('emr', region_name=region_name)
    cluster_id = cluster_id
    steps = [{
        'Name': "TEST Test",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['export', 'PYSPARK_PYTHON=python3']
        }
    }]
    print("SPARK Python3 export")
    action2 = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=steps)


def localFolderCreate(cluster_id,region_name):
    emr = boto3.client('emr', region_name=region_name)
    cluster_id = cluster_id
    steps = [{
        'Name': "Runnung a script on EMR_SPARK_SUBMIT",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['sudo', 'mkdir', '/tmp/Airflow_test']
        }
    }]
    print("Directory Created")
    action4 = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=steps)

def copyApplication(cluster_id,region_name):
    emr = boto3.client('emr', region_name=region_name)
    cluster_id = cluster_id
    steps = [{
        'Name': "Copying a script from s3 to local which needs to be excecuted",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['sudo', 'aws', 's3', 'cp',
                     's3://XXXX/test-spark/Application/application-pyspark-emr.py',
                     '/tmp/Airflow_test/']
        }
    }]
    action5 = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=steps)
    print("Successfully downloaded application.py")

def setMod(cluster_id,region_name):
    emr = boto3.client('emr', region_name=region_name)
    cluster_id = cluster_id
    steps = [{
        'Name': "Changing CHMOD for process-flow.py",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['sudo', 'chmod', '777', '/tmp/Airflow_test/application-pyspark-emr.py']
        }
    }]
    action6 = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=steps)
    print("CHMOD Changed for application-pyspark.py")



def runApplication(cluster_id,region_name):
    emr = boto3.client('emr', region_name=region_name)
    cluster_id = cluster_id
    steps = [{
        'Name': "Runnung a script on EMR_SPARK_SUBMIT",
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['sudo', 'spark-submit', '/tmp/Airflow_test/application-pyspark-emr.py']
        }
    }]
    print("SPARK-Submit Successfully executed")
    action7 = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=steps)
