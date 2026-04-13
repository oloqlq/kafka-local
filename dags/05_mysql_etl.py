'''
- etl 간단하게 적용, 스마트팩토리상 온도 센서에 대한 ETL 처리, mysql 사용
'''

#####################################################
#                     패키지 호출                      #
#####################################################
# from airflow.providers.mysql.operators.mysql import MysqlOperator - 범용 SQL Operator로 대체
# MySQLHook : Load 단계에서 활용

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json
import random
import pandas as pd
import os


#####################################################
#                       EXTRACT                     #
#####################################################
# 더미데이터 생성 - json.dump 저장
# xCom을 통해서 task_trasnform로 로그 경로를 전달. (실제 데이터 전송은 지양할 것)


DATA_PATH = '/opt/airflow/dags/data'
os.makedirs(DATA_PATH, exist_ok=True)

def _extract(**kwargs):
    data = [
        {
            "sensor_id" : f"SENSOR_{i+1}", #장비 ID
            "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
            "temperature" : round( random.uniform(20.0, 150.0), 2),
            "status" : "on", #off
        } for i in range(10)    ]
    
    file_path = f'{DATA_PATH}/sensor_data_{ kwargs['ds_nodash'] }.json'
    with open(file_path, 'w') as f:
        json.dump(data, f)
    logging.info(f'추출된 로그 데이터 { file_path} ')
    return file_path
    pass



#####################################################
#                      TRANSFORM                    #
#####################################################
# 섭씨를 화씨로 일괄 처리 (1번에 n개의 센서에서 데이터가 전달)
# 파생변수로 화씨 데이터 구성 (temperature_f) = (섭씨 * 9/5) + 32
# 100도 미만 데이터만 추출 (필터링) - 100도 이상의 데이터는 이상치로 간주
# XCom을 통해 전달된 데이터를 획득해온다.
# 전처리데이터 csv 덤프 (s3 업로드 고려) - airflow가 aws에서 가동되면 s3로 저장됨.
# CSV경로 XCOM을 통해 개시

def _trasform(**kwargs):
    ti = kwargs['ti']
    json_file_path = ti.xcom_pull(task_ids='extract')
    logging.info(f'전달받은 데이터 {json_file_path}')

    df = pd.read_json(json_file_path)
    target_df = df[ df['temperature'] < 100 ].copy()
    target_df['temperature_f'] = (target_df['temperature'] * 9/5) + 32

    file_path = f'{DATA_PATH}/preprocessing_data_{ kwargs['ds_nodash' ]}.csv'
    target_df.to_csv( file_path, index=False) 

    return file_path
    pass



#####################################################
#                        LOAD                       #
#####################################################
# Workflow : .csv를 DataFrame으로 변환하여 MySQL로 적재
# 1. .csv 경로 획득 -> xcom을 통해 이전 task의 id를 이용하여 추출한다. - ti(task instance)가
# 2. DataFrame 변환 : 소규모 데이터이므로 pandas로도 적절하다. 
# 3. MySQL 연결 : MySqlHook
# 4. 커서 획득
    # 4-1. insert 구문 사용
        #sql = ""
        #params = []
        #cursor.executemany( sql, params )
        # 4-2. 커밋
        #conn.commit()
# 5. 연결 종료
# 7. 전체를 try ~ except로 감싸기(I/O)

def _load(**kwargs):
    ti = kwargs['ti']
    csv_path = ti.xcom_pull(task_ids='transform')
    df = pd.read_csv(csv_path)

    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn       = mysql_hook.get_conn() # 커넥션 획득 - I/O 영향 있음 : 예외처리 필요. with문 .. 
    try:
        with conn.cursor() as cursor:
            sql = '''
                insert into sensor_readings
                (sensor_id, timestamp. temperature_c, temperature_f)
                values (%s, %s, %s, %s)

            '''
            params = [
                ( data['sensor_id'], data['timestamp'],
                  data['temperature_c'], data['temperature_f'] )
                for _, data in df.iterrows()
            ]
            logging.info(f'입력할 데이터(파라미터) {params}')
            cursor.executemany( sql, params )

            conn.commit()
            logging.info('mysql에 적재 완료')
            pass        
    except Exception as e:
        logging.info(f'적재 오류: {e}')
        pass
    finally:
        # 5
        if conn:
            conn.close()
            logging.info(f'mysql 연결 종료')
    
    pass




#####################################################
#                     DAG Define                    #
#####################################################
# task 정의 - Extract, Transform, Load
# 테이블 생성 : 첫 실행에서만 적용되게끔 IF NOT EXISTS로 구성
# 대시보드에 admin>connectinos>하위에 사전 등록

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
    task_create_table = SQLExecuteQueryOperator(
        task_id = "create_table",
        conn_id = "mysql_default", 
        sql = '''
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sensor_id VARCHAR(50),
                timestamp DATETIME,
                temperature_c FLOAT,
                temperature_f FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        '''
    )
    task_extract = PythonOperator(
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




#####################################################
#                      의존성 정의                     #
#####################################################
    # 의존성 정의 -> 시나리오별 준비 
    # task_create_table >> task_extract >> task_trasform >> task_load
    task_create_table >> task_extract >> task_trasform >> task_load