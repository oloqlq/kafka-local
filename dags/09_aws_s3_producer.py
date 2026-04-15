'''
데이터 생산(etl등 통해서) -> CSV -> s3 업로드(PUSH) 처리
'''

# 1. 패키지
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# 2. 환경변수
# 버킷명, 파일명, s3 키, 로컬 주소
BUCKET_NAME     = "de-ai-14-827913617635-ap-northeast-1-an" 
FILE_NAME       = 'sensor_data.csv'
S3_KEY          = f'income/{FILE_NAME}'
LOCAL_PATH      = f'/opt/airflow/dags/data/{FILE_NAME}'


# 3. DAG 정의
with DAG(
    dag_id      = "09_aws_s3_producer", 
    description = "aws 연동, s3 업로드",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = None,
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 's3', 'producer'],
) as dag:
    # 4. Task 정의
    task_create_dummy_data_csv = BashOperator(
        task_id = "create_dummy_data_csv",
        bash_command = f'echo "id,timestamp,value\n1,$(date),100\n2,$(date),500" > {LOCAL_PATH}'

    )
    task_upload_to_s3 = LocalFilesystemToS3Operator(
        task_id = "upload_to_s3",
        filename = LOCAL_PATH,  
        dest_key = S3_KEY,   
        dest_bucket = BUCKET_NAME,  
        aws_conn_id = 'aws_default', 
        replace  = True #동일한 파일이 존재하면 덮어쓴다. -> 
    )
    pass


    # 5. 의존성 정의
    task_create_dummy_data_csv >> task_upload_to_s3
    pass