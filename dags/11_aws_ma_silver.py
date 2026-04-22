
# 1. 패키지 호출
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# 2. 환경변수
DATABASE_BRONZE = 'de_ai_14_ma_bronze_db' 
DATABASE_SILVER = 'de_ai_14_ma_silver_db'
SILVER_S3_PATH  = 's3://de-ai-14-827913617635-ap-northeast-1-an/medallion/silver/'
ATHENA_RESULTS  = 's3://de-ai-14-827913617635-ap-northeast-1-an/athena-results/'
SILVER_TBL_NAME = 'sales_silver_tbl'

# 3. DAG 정의
with DAG(
    dag_id      = "11_medallion_bronze_to_silver_ctas", 
    description = "athena ctas 작업",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '10 * * * *', # 매시 10분에 진행
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 'medallion', 'silver', 'athena', 'ctas'],
) as dag:
    # 4. task 정의 (2개)
    drop_silver_task = AthenaOperator(
        task_id = 'drop_silver_tbl',
        query   = 'drop table if exists {{ params.database_silver }}.{{ params.tbl_nm }};',
        database= DATABASE_SILVER,
        output_location = ATHENA_RESULTS,
        params  = {'database_silver':DATABASE_SILVER, 'tbl_nm':SILVER_TBL_NAME} 
    )
    # 수행시간 => airflow context에 정보가 기롟되어 있음, 
    # Jinja 템프릿 활용중 => {{ execution_date.foramt('YYYY') }} => 2026 세팅됨
    ctas_silver_task = AthenaOperator(
        task_id = 'ctas_silver',
        query   = '''
            Create Table if not exists {{ params.database_silver }}.{{ params.tbl_nm }}
            with (
                format              = 'PARQUET',
                parquet_compression = 'SNAPPY',
                external_location   = '{{ params.silver_path }}',
                partitioned_by      = ARRAY['dt','hr']
            ) As 
            Select 
                event_id,
                event_time as event_timestamp,
                data.user_id,
                data.item_id,
                data.price,
                data.qty,
                (data.price * data.qty) as total_price ,
                data.store_id,
                source_ip,
                user_agent,
                cast(year || '-' || month || '-' || day as VARCHAR) as dt,
                hour as hr
            from {{ params.database_bronze }}.raw_bronze_tbl
            where   year = '{{ execution_date.format('YYYY') }}'
                and month= '{{ execution_date.format('MM') }}'
                and day  = '{{ execution_date.format('DD') }}'
                and hour = '10'
            ;

        ''',
        # and hour = '{{ execution_date.format('HH') }}'
        database= DATABASE_SILVER,
        params  = {
            'database_bronze':DATABASE_BRONZE, 
            'database_silver':DATABASE_SILVER, 
            'tbl_nm':SILVER_TBL_NAME,
            'silver_path':SILVER_S3_PATH
        },
        output_location = ATHENA_RESULTS 
    )

    # 5. 의존성(injection) 구성
    drop_silver_task >> ctas_silver_task




    