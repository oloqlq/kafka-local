'''
- etl 간단하게 적용, 스마트팩토리상 온도 센서 데이터 처리 (mysql 사용)
'''


# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

from airflow.providers.mysql.operators.mysql import MysqlOperator
# Load 단계에서 SQL에 전처리된 데이터를 밀어넣을 때 사용
from airflow.providers.mysql.hooks.mysql import MySqlHook

# 데이터
import json
import random
import pandas as pd #소량의 데이터(규모 면에서)
import os


# 2. 환경변수
# 프로젝트 내부 폴더를 데이터용으로 (~/dags/data) 지정
# task 진행간 생성되는 파일을 동기화하도록 위치를 지정 -> 향후 s3로 대체될 수 있음

# 도커 컨테이너의 워커 내부 airflow 상의 지정한 데이터 위치
DATA_PATH = '/opt/airflow/dags/data' 
os.mkdir(DATA_PATH, exist_ok=True)

# 3-1. 콜백함수 정의

def _extract(**kwargs):
    pass
def _transform(**kwargs):
    pass
def _load(**kwargs):
    pass

# 2. DAG 정의
with DAG(
    dag_id      = "05_mysql_etl", 
    description = "ETL 수행하여 mysql에 온도 센서 데이터 적재",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['mysql', 'etl'],
) as dag:
    # 3. task 정의
    task_create_table = MysqlOperator(
        #최초는 생성, 존재하면 pass : if not exists
        task_id = "create_table",
        
    )
    t1 = PythonOperator(
        task_id = "extract",
        python_callable = _extract
    )
    t2 = PythonOperator(
        task_id = "transform",
        python_callable = _transform
    )
    t3 = PythonOperator(
        task_id = "load",
        python_callable = _load
    )

    # 4. 의존성 정의 -> 시나리오별 준비 
    task_create_table >> t1 >> t2 >> t3

    pass