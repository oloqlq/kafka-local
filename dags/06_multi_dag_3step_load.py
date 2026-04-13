#####################################################
#                     패키지 호출                      #
#####################################################


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

DATA_PATH = '/opt/airflow/dags/data'
os.makedirs(DATA_PATH, exist_ok=True)


#####################################################
#                        LOAD                       #
#####################################################
# csv => df => mysql 적재
# mysql 연결 => MySqlHook 사용
# 전체를 try ~ except로 감싸기(I/O)
# 실제는 실패 작업인데, 성공으로 오인할수 있다 -> 예외 던지기 필요함
# 여러 데이터를 한번에 넣을때 유용 => executemany() 대응


def _load(**kwargs):
    ti = kwargs['ti']
    csv_path = ti.xcom_pull(task_ids='trasform')
    df = pd.read_csv(csv_path)
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn       = mysql_hook.get_conn()
    try:
        with conn.cursor() as cursor:        
            sql = '''
                insert into sensor_readings
                (sensor_id, timestamp, temperature_c, temperature_f)
                values (%s, %s, %s, %s)
            '''            
            params = [
                ( data['sensor_id'],     data['timestamp'], 
                  data['temperature'], data['temperature_f'] )
                for _, data in df.iterrows()
            ]
            logging.info(f'입력할 데이터(파라미터) {params}')
            cursor.executemany( sql, params )
            conn.commit()
            logging.info('mysql에 적제 완료')
            pass        
    except Exception as e:
        logging.info(f'적제 오류 : {e}')
        pass
    finally:
        if conn:
            conn.close()
            logging.info(f'mysql 연결 종료 (뒷정리)')
    
    pass



#####################################################
#                     DAG Define                    #
#####################################################


with DAG(
    dag_id      = "06_multi_dag_3step_load", 
    description = "load 전용 DAG",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['load', 'etl'],
) as dag:
    task_load       = PythonOperator(
        task_id = "load",
        python_callable = _load
    )