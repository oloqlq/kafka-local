
# 로그 제너레이터
    - 패키지 설치
    '''
        pip install faker
    '''
    - 구조
    '''
        /log_gen
        L log_generator.py  : 로그 생성 클래스 모듈 포함
        L run.py            : 생성 코드 샘플

    '''

    - 산업별 (금융, 이커머스, ...) 로그 생성 클래스 모듈
    - 실행
        '''
            # 워킹 디렉토리
            cd log_gen
            ~/log_gen> python run.py
        '''


# .env git에서 제거 
```
    # 캐싱 제거 (터미널에서 실행)
    git rm --cached .env

    # aws 브런치에서 진행
    # 스테이징 -> 커밋 -> 푸시
    git add .
    git commit -m "Feat: .env 캐시 제거"
    git push
```