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


load_dotenv()
# 2. Faker 생성
fake = Faker()
AWS_ACCESS_KEY = os.getenv('ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('SECRET_KEY')

AWS_REGION = 'ap-northeast-1'
KINESIS_DATA_STREAM_NAME = 'de-ai-14-an1-kdf-medallion-bronze-stream'

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
def gen_data():
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
          "store_id": "gwangmyeong-01"
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

if __name__ == '__main__':
  try:
    while True:
      log_entry = gen_data()
      if send_to_kinesis(log_entry):
        print(f'{log_entry["event_time"]} 전송 성공 { log_entry["event_id"][:6] }')
      time.sleep(random.uniform(0.5, 1.5))
  except KeyboardInterrupt:
    print('발생 중단')