'''
 DAG -> DAG 작동 시키는(오퍼레이터) 트리거 필요함 -> 핵심
'''

#####################################################
#                     패키지 호출                      #
#####################################################

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # 핵심!
import logging
import json
import random
import pandas as pd
import os

DATA_PATH = '/opt/airflow/dags/data'
os.makedirs(DATA_PATH, exist_ok=True)




#####################################################
#                       EXTRACT                     #
#####################################################
# 더미 데이터 고려 구성 -> 1회성으로 10건 구성 -> [ {}, {}, ... ]
# 더미 데이터를 파일로 저장 (로그파일처럼) -> json 형태
# XCOM을 통해서  task_trasform에게 전달 (로그의 경로를 전달, 실 데이터 전달 x(지양))

def _extract(**kwargs):
    data  = [
        { 
            "sensor_id" : f"SENSOR_{i+1}", # 장비 ID
            "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # YYYY-MM-DD hh:mm:ss
            "temperature": round( random.uniform(20.0, 150.0), 2),
            "status" : "on", # "off"
        } for i in range(10)   ]
    file_path = f'{DATA_PATH}/sensor_data_{ kwargs['ds_nodash'] }.json'
    with open(file_path, 'w') as f:
        json.dump(data, f)
    logging.info(f'extract 한 로그 데이터 { file_path } ')
    return file_path


#####################################################
#                     DAG Define                    #
#####################################################


with DAG(
    dag_id      = "06_multi_dag_1step_extract", 
    description = "extract 전용 DAG",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['extract', 'etl'],
) as dag:
    task_extract    = PythonOperator(
        task_id = "extract",
        python_callable = _extract
    )

    # 신규 추가 오퍼레이터
    # 다음 dag을 실행시키는 트리거 발동하는 역할
    task_trigger_transform_dag_run = TriggerDagRunOperator(
        task_id = "trigger_transform",
        trigger_dag_id = "06_multi_dag_2step_transform",
        conf ={
            "json_path":"{{ task_instance.xcom_pull(task_ids='extract') }}"
        },
        reset_dag_run = True,
        wait_for_completion = False # 타 DAG가 수행하라는 명령을 전달하면 대기 없이 바로 본 Task를 종료(비동기처리)
    )
    


#####################################################
#                      의존성 정의                     #
#####################################################

    task_extract >> task_trigger_transform_dag_run