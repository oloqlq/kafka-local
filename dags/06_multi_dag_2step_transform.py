#####################################################
#                     패키지 호출                      #
#####################################################

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 
import json
import random
import pandas as pd
import os

DATA_PATH = '/opt/airflow/dags/data'
os.makedirs(DATA_PATH, exist_ok=True)



#####################################################
#                      TRANSFORM                    #
#####################################################
# _extract에서 추출한 데이터를 XCom을 통해서 획득 
# 파생변수로 화씨 데이터 구성 (temperature_f) = (섭씨 * 9/5) + 32
# 전처리된 내용은 csv로 덤프 (s3로 업로드 고려)
# csv 경로 xcom을 통해서 개시


def _transform(**kwargs):
    dag_run = kwargs['ti']
    json_file_path = dag_run.conf.get('json_path')

    logging.info(f'전달받은 데이터 {json_file_path}')
    df = pd.read_json( json_file_path )
    target_df = df[ df['temperature'] < 100 ].copy()    
    target_df['temperature_f'] = (target_df['temperature'] * 9/5) + 32
    file_path = f'{DATA_PATH}/preprocessing_data_{ kwargs['ds_nodash'] }.csv'
    target_df.to_csv( file_path, index=False ) # 인덱스 제외
    logging.info(f'전처리후 csv 저장 완료 {file_path}')
    return file_path


#####################################################
#                     DAG Define                    #
#####################################################


with DAG(
    dag_id      = "06_multi_dag_2step_trasform", 
    description = "transform 전용 DAG",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['transform', 'etl'],
) as dag:
    task_transform   = PythonOperator(
        task_id = "transform",
        python_callable = _transform
    )
    task_trigger_load_dag_run = TriggerDagRunOperator(
        task_id = "trigger_load",
        trigger_dag_id = "06_multi_dag_3step_load",
        conf ={
            "csv_path":"{{ task_instance.xcom_pull(task_ids='extract') }}"
        },
        reset_dag_run = True,
        wait_for_completion = False
    )

#####################################################
#                      의존성 정의                     #
#####################################################
    task_transform >> task_trigger_load_dag_run