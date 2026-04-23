



# 패키지 설치
# pip install kafka-python

# CLI모드 접속
'''
oloqlq@oloqlqui-MacBookAir kafka-local % docker container exec -it kafka bash
root@2d6461f5c379:/# pwd
/
root@2d6461f5c379:/# cd /opt/kafka/bin
root@2d6461f5c379:/opt/kafka/bin#
'''


# 토픽 생성
'''
> kafka-topics.sh --create --topic spacex --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1
'''

# 토픽 전송
'''
> kafka-console-producer.sh --topic spacex --bootstrap-server 127.0.0.1:9092
'''

# 컨슈머
'''
> kafka-console-consumer.sh --topic spacex --bootstrap-server 127.0.0.1:9092
'''
