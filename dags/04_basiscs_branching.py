'''
- 함수 내부 연산의 결과에 의해 조건부로 task를 선택하여 진행 (의존성 컨트롤)
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator # task 조건부 선택
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule # 성공, 실패, 최소 단위등등 조건 설정
import logging

# 2. DAG 정의
with DAG() as dag:
    # 3. task 정의
    task_start = EmptyOperator()
    task_brach = BranchPythonOperator()
    task_process = PythonOperator()
    task_skip    = EmptyOperator()
    task_end     = EmptyOperator()

    # 4. 의존성 정의 -> 시나리오별 준비 
    task_start >> task_brach
    task_brach >> task_process >> task_end
    task_brach >> task_skip    >> task_end

    pass

