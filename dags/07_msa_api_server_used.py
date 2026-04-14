'''
- API 호출 과정 적용. 데이터 처리에 대한 스케줄 구성
'''

# 1. 패키지 구성
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import json
import requests

# 2. API 서버 주소
API_URL = "http://127.0.0.1:8000/predict"


# 4-4. 콜백함수 정의
def _create_dummy_data(**kwargs):
    pass
def _api_service_call(**kwargs):
    pass
def _load_users_credit(**kwargs):
    pass



# 3. DAG 정의
with DAG(
    dag_id          = "07_msa_api_server_used",
    description     = "msa 상 특정 서비스를 호출하고 수행하는 스케줄링."
    default_args    = {
        'owner'         : 'de_2team_manager',
        'retries'       : 1,
        'retry_delay'   : timedelta(minutes=1)
    }

) as dag:
    # 4. Task 정의
    # 4-1. 더미 데이터 준비
    task_create_dummy_data = PythonOperator(
        task_id = "task_create_dummy_data",
        python_callable = _create_dummy_data
    )

    # 4-2. API 호출(AI서비스 활용)-> 신용평가획득
    task_api_service_call = PythonOperator(
        task_id = "task_api_service_call",
        python_callable = _api_service_call
    )
    # 4-3. 결과 저장 -> 추후 고객정보 업데이트 
    task_load_users_credit = PythonOperator(
        task_id = "task_load_users_credit",
        python_callable = _load_users_credit
    )


# 5. 의존성, 각 task는 xCom 통신으로 데이터 공유

task_create_dummy_data >> task_api_service_call >> task_load_users_credit
