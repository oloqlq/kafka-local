'''
- 로그 생성기 사용 예시
'''
import json
import time
# 현재 워킹디렉토리에서 코드를 작동할때 경로
from log_generator import LogGenerator

def make_log( config ):
  log_gen = LogGenerator()
  log_gen_map = {
    "finance":log_gen.finance, 
    "factory":log_gen.factory,
  }

  print(f'{config["target_industry"]} 로그 생성 시작')
  print('-'*50)
  for i in range(config['total_count']): 
    cur_func = log_gen_map.get( config['target_industry'] )
    log  = cur_func()
    log_json = json.dumps( log, ensure_ascii=False )
    print( f'[Log-{i+1}] {log_json}')
    time.sleep( log_gen.get_interval_time( config['mode'], config['interval'] ) )
  print('-'*50)

log_gen_g = LogGenerator()
def make_one_log():  
  return json.dumps( log_gen_g.finance(), ensure_ascii=False ) # dict -> str : 객체직렬화
    
if __name__ == '__main__':
    config = {
       "target_industry":"finance", 
       "mode":"random", 
       "interval":1,   
       "total_count":10,
       "loop":False     
    }
    make_log( config )