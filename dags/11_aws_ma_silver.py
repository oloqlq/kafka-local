'''

데이터 전처리 수행 (flatten, 파생변수, 칼럼 명 변경)

- event_id
- event_time --> event_timestamp
- data.user_id
- data.itemid
- data.price
- data.qty
- (data.price * data.qty) as total_price
- data.store_id
- source_ip
- user_agent
- dt (year-month-day)
- hour as hr

'''


# 1. 패키지 호출
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# 2. 환경변수
DATABASE_BRONZE = 'de-ai-14-ma-bronze_db' 
DATABASE_SILVER = 'de-ai-14-ma-silver_db'
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
    schedule_interval = '10 * * * *',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 'medallion', 'silver', 'athena', 'ctas'],

) as dag:
    drop_silver_task = AthenaOperator(
        task_id = 'drop_silver_tbl',
        query   = 'drop table if exists {{ params.database_silver }}.{{ params.tbl_nm }}',
        database= DATABASE_SILVER,
        output_location= ATHENA_RESULTS,
        params  = {'database_silver' : DATABASE_SILVER, 'tbl_nm':SILVER_TBL_NAME}
    )
    ctas_silver_task = AthenaOperator(
        task_id = 'ctas_sivler_tbl',
        
    )

    # 4. Task 정의
    drop_silver_task >> ctas_silver_task




    # 5. 의존성 정의