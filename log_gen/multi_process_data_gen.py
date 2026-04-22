'''
- 멀티 프로세스로 데이터 발생
  - store-01 ~ store-(프로세스 수 ) : 점포별로 데이터 발생 구성
- 실행
  airflow-local> python ./log.gen/multi_process_data_gen.py
'''


# 1. 모듈 가져오기
import json
import os
import uuid
import random
import boto3
import time
from datetime import datetime, UTC
from faker import Faker
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import multiprocessing 


load_dotenv()
# 2. Faker 생성
fake = Faker()
AWS_ACCESS_KEY = os.getenv('ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('SECRET_KEY')

AWS_REGION = 'ap-northeast-1'
KINESIS_DATA_STREAM_NAME = 'de-ai-14-ap1-kdf-medallion-bronze-stream'

# 3. AWS연동-> Session설정 -> I/O
try:
  # kinesis 세션 획득
  session = boto3.Session(
      aws_access_key_id=AWS_ACCESS_KEY,
      aws_secret_access_key=AWS_SECRET_KEY,
      region_name=AWS_REGION
  )
  kinesis_client = session.client('kinesis')
  print('AWS 연동 성공')
except Exception as e:
    print('AWS 연동 실패')


# 데이터 더미 생성
def gen_data(store_id):
  items = [
        {"item_id": "bread-001", "item_name": "우유식빵", "price": 5500},
        {"item_id": "bread-002", "item_name": "천연발효버터치아바타", "price": 4800},
        {"item_id": "coffee-01", "item_name": "아메리카노", "price": 4000},
        {"item_id": "jam-01", "item_name": "수제 딸기잼", "price": 8500}
    ]

  selected_item = random.choice(items)
  qty = random.randint(1, 3)
  current_utc_time = datetime.now(UTC).isoformat().replace("+00:00", "Z")

  raw_log = {
      "event_id": str(uuid.uuid4()),
      "event_time": current_utc_time,
      "source_ip": fake.ipv4(),
      "user_agent": fake.user_agent(),
      "data": {
          "user_id": f"user_{random.randint(100, 999)}",
          "item_id": selected_item["item_id"],
          "price": selected_item["price"],
          "qty": qty,
          "store_id": store_id
      },
      "ingested_at": current_utc_time
  }
  return raw_log

# kinesis로 데이터 전송
def send_to_kinesis(log_entry):
  try:
    kinesis_client.put_record(
      StreamName  = KINESIS_DATA_STREAM_NAME,
      Data        = json.dumps(log_entry),
      PartitionKey= log_entry['event_id']
    )
    return True
  except Exception as e:
    print( 'aws 전송 에러', e )
    return False
  pass

def run_producer(i, store_id):
  try:
    print(f'프로세스-{i} 가동')
    while True:
      log_entry = gen_data(store_id)
      if send_to_kinesis(log_entry):
        print(f'{log_entry["event_time"]} 전송 성공 { store_id }')
      time.sleep(random.uniform(0.5, 1.5))
  except KeyboardInterrupt:
    print('발생 중단')
  print(f'프로세스-{i} 종료')


if __name__ == '__main__':
  NUM_STORES    = 4
  processes     = list()
  print(f'{NUM_STORES}개의 프로세스가 데이터를 발생시켜 kinesis에 전송')

  for i in range(NUM_STORES):
    store_id    = f"store-{str(i+1).zfill(2)}"
    p = multiprocessing.Process(target=run_producer, args=(i, store_id))
    processes.append(p)
    p.start()
  
  try:
    for p in processes:
      p.join()
  except Exception:
    print('종료')


