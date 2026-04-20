'''
[상황 가정 : Athena DB, S3 데이터 존재]
- airflow를 통해서 athena 기본 연동

task1   - 테이블 생성 -> Location 정보로 특정 CSV가 존재하는 s3 bucket을 지정.
        - 버킷 내 데이터를 쿼리로 액세스

task2   - 최신 상태를 유지. 기존 내용은 제거처리

task3   - 획득한 데이터를 분석, 결과를 s3에 저장 및 압축.

스케줄 매일 1회 진행
'''

# 1. 패키지 호출
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
import logging

# 2. 환경변수
BUCKET_NAME         = 'de-ai-14-827913617635-ap-northeast-1-an'
ATHENA_DB_NAME      = 'de-ai-14-an1-glue-db'
SRC_TABLE           = 'athena_s3_data_tbl'
TARGET_TABLE        = 'pass_student'

S3_TARGET_LOC  = f's3://{BUCKET_NAME}/athena/tbl/{TARGET_TABLE}/'
S3_QUERY_LOG_LOC = f's3://{BUCKET_NAME}/athena/query_logs/'

# 3. DAG 정의 
with DAG(
    dag_id      = "10_aws_athena_ctas_etl", 
    description = "athena ctas 작업",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = None,
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 's3', 'athena', 'ctas'],
) as dag:
    # 4. Task 정의
    # DAG 작동 -> S3 clean 작업 : 매 가동시 초기상태 유지전략 (멱등성 유지)

    # 임시로 사용한 s3 공간 정리
    task1 = S3DeleteObjectsOperator(
        task_id         = 'clean_s3_target',
        bucket          = BUCKET_NAME,
        prefix          = f'athena/tbl/{TARGET_TABLE}/',
        aws_conn_id     = 'aws_default'
    )

    # 임시로 사용한 테이블 삭제
    task2 = AthenaOperator(
        task_id         = 'drop_table',
        query           = f'drop table if exists {TARGET_TABLE}',
        database        = ATHENA_DB_NAME,
        output_location = S3_QUERY_LOG_LOC,
        aws_conn_id     = 'aws_default'
    )

    # 90점 이상 학생만 추출 -> PARQUET 포맷 변환 -> GZIP 압축 -> S3_TARGET_LOG 저장
    # TARGET TABLE이 해당 소스를 참조하여 Athena를 통해 쿼리를 수행, 결과를 출력.
    query = f'''
        CREATE TABLE {TARGET_TABLE}
        WITH (
            format = 'PARQUET',
            parquet_compression = 'GZIP',
            external_location   = '{S3_TARGET_LOC}'
        )
        AS
        SELECT id, name, score, created_at
        FROM {SRC_TABLE}
        WHERE score >= 90
        ORDER BY score DESC
    '''

    task3 = AthenaOperator(
        task_id     = 'create_table_format_parquet',
        query       = query,
        database    = ATHENA_DB_NAME,
        output_location= S3_QUERY_LOG_LOC,
        aws_conn_id = 'aws_default',
        do_xcom_push = True
    )
    task4 = AthenaSensor(
        task_id             = 'sensor',
        query_execution_id  = "{{ task_instance.xcom_pull(task_ids='create_table_format_parquet') }}",
        poke_interval       = 10,
        timeout             = 600,
        aws_conn_id         = 'aws_default'
    )

# 5. 의존성 정의

    task1 >> task2 >> task3 >> task4    
    pass