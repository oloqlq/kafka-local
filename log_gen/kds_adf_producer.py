'''
- 주가 로그 생성기, boto3를 이용하여 직접 연계
- 로그는 kinesis로 전달
'''
import time
import random
import json
from datetime import datetime
import boto3 
from dotenv import load_dotenv

# 2. 환경변수 세팅
ACCESS_KEY = ''
SECRET_KEY = ''