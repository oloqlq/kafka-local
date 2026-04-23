# 주제
- kafka

# repo
- https://github.com/edu-kenshi/kafka-local.git

# 설치
```
    docker compose up -d
```

# 패키지 설치
```
pip install kafka-python
```

# CLI 모드로 테스트
```
# 접속
# 호스트  PC
PS ~\kafka-local> docker container exec -it kafka bash

# 컨테이너
root@4527e52ba5f6:/# pwd
/
root@4527e52ba5f6:/# cd /opt/kafka/bin

# 토픽 생성
> kafka-topics.sh --create --topic spacex --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1

```