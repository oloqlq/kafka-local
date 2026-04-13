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



# 3. DAG 정의
with DAG(

) as dag:
    # 4. Task 정의
    # 4-1. 더미 데이터 준비
    task_create_dummy_data = PythonOperator()

    # 4-2. API 호출(AI서비스 활용)-> 신용평가획득
    task_api_service_call = PythonOperator()
    # 4-3. 결과 저장 -> 추후 고객정보 업데이트 
    task_load_users_credit = PythonOperator()


# 5. 의존성, 각 task는 xCom 통신으로 데이터 공유

task_create_dummy_data >> task_api_service_call >> task_load_users_credit
