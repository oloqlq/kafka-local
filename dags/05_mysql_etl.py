'''
- etl 간단하게 적용, 스마트팩토리상 온도 센서에 대한 ETL 처리, mysql 사용
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
# 추가분
#from airflow.providers.mysql.operators.mysql import MysqlOperator
# Load 처리시 sql에 전처리된 데이터를 밀어 넣을때 사용
from airflow.providers.mysql.hooks.mysql import MySqlHook
# 데이터
import json
import random
import pandas as pd  # 소량의 데이터(데이터 규모)
import os

# 2. 기본설정
# 프로젝트 내부 폴더를 데이터용으로 (~/dags/data)지정
# task 진행간 생성되는 파일을 동기화하도록 위치 지정 -> 향후 s3(데이터 레이크)로 대체 될수 있음

# 도커 내부에 생성된 컨테이너 상 워커내의 airflow 상의 지정한 데이터 위치
DATA_PATH = '/opt/airflow/dags/data'
os.makedirs(DATA_PATH, exist_ok=True)

def _extract(**kwargs):
    # 스마트팩토리에 설치된 오븐 온도 센서에서  데이터가 발생되면 데이터레이크(s3, 어딘가에 존재)에 쌓이고 있다
    # 더미데이터 고려 구성 -> 1회성으로 10건 구성 [{}, {}, {}, ...]
    data = [
        {
            "sensor_id" : f"SENSOR_{i+1}", #장비 ID
            "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
            "temperature" : round( random.uniform(20.0, 150.0), 2),
            "status" : "on", #off
        } for i in range(10)    ]
    
    # 더미 데이터를 파일로 저장 (로그파일처럼) -> json 형태
# /opt/airflow/dags/data/sensor_data_DAG수행날짜.json
# 실습 -> 위의 데이터를 위의 형식으로 저장하시오 ( json.dump(data, f) )

    file_path = f'{DATA_PATH}/sensor_data_{ kwargs['ds_nodash'] }.json'
    with open(file_path, 'w') as f:
        json.dump(data, f)
    pass



def _trasform(**kwargs):
    # _extract에서 추출한 데이터를 XCom을 통해서 획득
    # 이 데이터를 df(pandas 사용, 소량데이터)로 로드 -> 섭씨를 화씨로 일괄 처리(1번에 n개의 센서에서 데이터가 전달)
    # 전처리된 내용은 csv로 덤프 (s3로 업로드 고려)
    pass
def _load(**kwargs):
    # csv => df => mysql 적제
    pass

# 3. DAG 정의
with DAG(
    dag_id      = "05_mysql_etl", 
    description = "etl 수행하여 mysql에 온도 센서 데이터 적제",
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
    # 4. task 정의
    # task_create_table = MysqlOperator(
    #     # 테이블 생성, if not exists를 사용하여 무조건 sql이 일단 수행되게 구성 
    #     # -> 아니라면 fail 발생함(2회차부터)
    #     # 최초는 생성, 존재하면 pass => if not exists
    #     task_id = "create_table",
    #     # 연결정보
    #     mysql_conn_id = "mysql_default", # 대시보드에 admin>connectinos>하위에 사전 등록
    #     # sql
    #     sql = '''
    #         CREATE TABLE IF NOT EXISTS sensor_readings (
    #             id INT AUTO_INCREMENT PRIMARY KEY,
    #             sensor_id VARCHAR(50),
    #             timestamp DATETIME,
    #             temperature_c FLOAT,
    #             temperature_f FLOAT,
    #             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    #         );
    #     '''
    # )
    
    task_extract    = PythonOperator(
        task_id = "extract",
        python_callable = _extract
    )
    task_trasform   = PythonOperator(
        task_id = "trasform",
        python_callable = _trasform
    )
    task_load       = PythonOperator(
        task_id = "load",
        python_callable = _load
    )

    # 5. 의존성 정의 -> 시나리오별 준비 
    # task_create_table >> task_extract >> task_trasform >> task_load
    task_extract >> task_trasform >> task_load