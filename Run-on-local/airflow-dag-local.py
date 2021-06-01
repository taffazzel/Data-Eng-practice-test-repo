# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from builtins import range
from datetime import *
import datetime
import pendulum
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

local_tz = pendulum.timezone("Europe/London")

default_args = {
    'owner': 'taffazzel',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 10, tzinfo=local_tz),
    'email': ['tafaz_hossain@yahoo.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(dag_id='data-eng-exercise',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 0 * * *")

pyspark_app_home = Variable.get("PYSPARK_APP_HOME")

flight_search_ingestion = SparkSubmitOperator(task_id='data-eng-excercise',
                                              application=f'{pyspark_app_home}/Application/application-pyspark.py',
                                              packages="org.apache.spark:spark-core_2.12:2.4.7",
					                          total_executor_cores=1,
                                              executor_cores=1,
                                              executor_memory='1g',
                                              driver_memory='1g',
                                              execution_timeout=timedelta(minutes=10),
                                              dag=dag
                                              )
