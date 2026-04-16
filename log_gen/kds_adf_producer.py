'''
- 주가 로그 생성기, boto3를 이용하여 직접 연계
- 로그는 kinesis로 전달
'''

# 1. 패키지 호출
import time
import random
import json
from datetime import datetime
import boto3 
from dotenv import load_dotenv
import os

# 2. 환경변수 세팅
load_dotenv()
ACCESS_KEY  = os.getenv("ACCESS_KEY")
SECRET_KEY  = os.getenv("SECRET_KEY")
REGION      = 'ap-northeast-1'

print( ACCESS_KEY, SECRET_KEY )


# 3. 클라이언트 생성
def get_client( service_name='firehose', is_in_aws=True ):
    if not is_in_aws:
        # AWS 외부에서 진행
        session   = boto3.Session(
            aws_access_key_id     = ACCESS_KEY,
            aws_secret_access_key = SECRET_KEY,
            region_name           = REGION
        )
        return session.client(service_name)    
    # AWS 내부에서 진행
    return boto3.client(service_name, region_name = REGION)

kinesis = get_client('kinesis', False)
print( kinesis )


# 4. 데이터 제너레이터 함수 구성
def gen_stock_data():
    ticker = ['NVDA', 'GOOGL', 'AAPL', 'TSLA', 'AMZN', 'MSFT']
    return {
        "event_time" : datetime.now().isoformat(),
        "ticker" : random.choice(ticker),
        "price" : round( random.uniform(100,1000), 2),
        "volume" : random.randint(1, 100),
        "trade_id" : random.randint(100000, 9999999)
    }

# 5. 데이터 전송
print('stock 거래 데이터 전송 시작...')
try:
    while True:
        # 데이터 생성
        data = gen_stock_data()
        print( f"전송전: {data}")
        # kinesis 전달
        kinesis.put_record(
            StreamName = "de-ai-14-an1-kds-stock-analysis ",
            Data = json.dumps( data ),
            PartitionKey = data['ticker'] # 해당 컬럼의 고유값의 개수만큼 조각(샤드, 전용 차선)구성
        )
        print( f"전송: {data}")
        time.sleep(0.5)
except Exception as e:
    print('중단 ', e)

