# Trading Project
학습된 딥러닝 모델을 통한 주가 동향 예측 및 매매 프로그램

# 개발 환경
Databases: MySQL -> Apache Spark(?) & Hadoop(?)

Server: AWS RDS(?)

Tools: Airflow(?)

Language: Python 3.12 64bits & 3.7.9. 32bits(가상환경)

API: 키움증권

# 매뉴얼
0. 아래 과정은 cmd에서 진행해야 함.
1. 프로젝트 폴더 생성
2. 파이썬 3.7.X 32bits로 가상환경 생성: py -<버전> -m venv .venv
[참고](https://www.luck7owl.com/it/python/%ED%8C%8C%EC%9D%B4%EC%8D%AC-venv-%EA%B0%80%EC%83%81%ED%99%98%EA%B2%BD-%EA%B5%AC%EC%B6%95%ED%8C%8C%EC%9D%B4%EC%8D%AC-%EB%B2%84%EC%A0%84-%EC%A7%80%EC%A0%95/)
3. 가상환경 활성화: .venv\Scripts\activate
4. sqlalchemy 버전: pip install sqlalchemy==1.4.46
5. 파일 실행: 우클릭 X, python main.py 으로 실행

초기 설정: [참고](https://wikidocs.net/126081#google_vignette)

# 트러블슈팅
[qt.qpa.plugin: Could not find the Qt platform plugin "windows" in ""
This application failed to start because no Qt platform plugin could be initialized. Reinstalling the application may fix this problem. 오류 발생 시](https://log-mylife.tistory.com/entry/Could-not-load-the-Qt-platform-plugin-%EB%AC%B8%EC%A0%9C-%ED%95%B4%EA%B2%B0%EB%B2%95)
- 디렉더리 관련 메타데이터(이름 등) 함수로 수정하면 안 됨. 환경 변수 수정해야 함.

# 참고
- main.py를 실행할 때는 3.7.9 32bit, 나머지 파일은 3.12로 구동 가능.
- 서버급 OS를 지원하지 않음
- 05:00에는 접속 자체가 불가
- 1초당 5회 제한

# TODO
- ERD 재설계
