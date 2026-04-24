##############################
#  로그 생성 -> 파일에 기록(json) #
##############################

# 1. 패키지 호출
import json
import time
import datetime
import os

# 2. 로그 저장 디렉토리 생성
log_dir = './sensor_logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 3. 로그 발생 및 저장
def generate_logs():
    data = {
        "timestamp"     : datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sensor_id"     : "AI-FACTORY-001",
        "temperature"   : 87.5, 
        "humidity"      : 41.1, 
        "status"        : "RUNNING"
    }
    # json 형태로 기록 : dict 직렬화 처리
    with open(f"{log_dir}/sensor_json.log", "a", encoding="utf-8") as f:  # "a" : append; 파일에 덧붙여서 쓰기 모드
        f.write( json.dumps(data) + "\n" )
    # text 형태로 기록 -> f-string구성
    text = f"[{ data["timestamp"] }] ID={data["sensor_id"]} |     TEMP:{data["temperature"]} |   HUMI:{data["humidity"]} |    STAT:{data["status"]}"
    with open(f"{log_dir}/sensor_text.log", "a", encoding="utf-8") as f:
        f.write(json.dumps(text) + "\n")
    
    print(f"로그발생완료 {data["timestamp"]}")

# 4. 로그발생기 가동
def main():
    try:
        while True:
            generate_logs()
            time.sleep(2)
    except Exception:
        print('종료 완료')
    pass

# 5. 프로그램 시작
if __name__ == '__main__':
    print('로그 발생 시작. 종료 ctrl+c')
    main()

