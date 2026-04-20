'''
- DAG 스케줄은 하루에 한번(00시00분00초) 지정하뒤 -> 테스트는 트리거 작동
- T1 : S3에 특정위치에 적제된 데이터를 기반으로 테이블 구성
    - ~/csvs/ 하위 데이터를 기반으로 테이블 구성 -> s3_exam_csv_tbl
- T2 : 해당 테이블을 이용하여 분석결과를 담은 테이블 삭제(존재하면)
    - daily_report_tbl 삭제 쿼리 수행(존재하면)
- T3 : T1에서 만들어진 테이블을 기반으로 분석 결과를 도출하여 분석결과를 담은 테이블에 연결 -> 결과레포트용 데이터 구성
    - 시험 결과를 기반으로, 결과, 카운트, 평균, 최소, 최대 -> 그룹화 수행(기준 result)
    - 테이블명 => daily_report_tbl
        - format = 'PARQUET'
        - external_location = '원하는 s3 위치로 지정' -> 쿼리 결과가 저장되는 곳
    - output_location = '원하는 s3 위치로 지정', -> 테이블의 메타 정보가 저장되는 곳
- 미구현 -> T3 데이터를 기반으로 대시보드 구성 -> 원하는 시간에 결과 파악
- 의종성 : T1 >> T2 >> T3
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
SRC_TABLE           = 's3_exam_csv_tbl'
TARGET_TABLE        = 'daily_report_tbl'

S3_TARGET_LOC  = f's3://{BUCKET_NAME}/athena/tbl/{TARGET_TABLE}/'
S3_QUERY_LOG_LOC = f's3://{BUCKET_NAME}/athena/query_logs/'


# 3. DAG 정의
with DAG (
    dag_id      = "10_aws_athena_query", 
    description = "athena query 작업",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = None,
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['', '', '', '']
) as dag :
    # 4. Task 정의
    task1   = S3DeleteObjectsOperator(
        task_id         = 'clean_s3_target',
        bucket          = BUCKET_NAME,
        prefix          = f'athena/tbl/{TARGET_TABLE}/',
        aws_conn_id     = 'aws_default'
    )

    task2   = AthenaOperator(
        task_id         = 'drop_table',
        query           = f'drop table if exists {TARGET_TABLE}',
        database        = ATHENA_DB_NAME,
        output_location = S3_QUERY_LOG_LOC,
        aws_conn_id     = 'aws_default'
    )

    # daily_report_tbl 테이블 생성 쿼리
    # daily_report_tbl 테이블 생성 쿼리
    query = f'''
        CREATE TABLE {TARGET_TABLE}
        WITH (
            format = 'PARQUET',
            parquet_compression = 'GZIP',
            external_location = '{S3_TARGET_LOC}'
        )
        AS
        SELECT
            result,
            COUNT(*) AS result_count,
            AVG(score) AS avg_score,
            MIN(score) AS min_score,
            MAX(score) AS max_score
        FROM {SRC_TABLE}
        GROUP BY result
        ORDER BY result
    '''

    task3 = AthenaOperator(
        task_id='create_table_daily_report',
        query=query,
        database=ATHENA_DB_NAME,
        output_location=S3_QUERY_LOG_LOC,
        aws_conn_id='aws_default',
        do_xcom_push=True
    )

    
    # 5. 의존성 정의

    task1 >> task2 >> task3