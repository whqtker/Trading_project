import csv
import pandas as pd
from config import db_config
import mysql.connector
from sqlalchemy import create_engine

def print_csv(file_path):
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        for row in reader:
            print(row)  

def opt10081_to_csv():
    # MySQL 데이터베이스 연결
    db = mysql.connector.connect(
        user=db_config['user'],
        password=db_config['password'],
        host=db_config['host'],
        port=db_config['port'],
        database=db_config['database']
    )

    # SQLAlchemy 엔진 생성
    engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

    # 커서 생성
    cursor = db.cursor()

    # SQL 쿼리 실행
    query = "SELECT * FROM opt10081"
    cursor.execute(query)

    # 결과를 배치로 가져오기
    batch_size = 1000  # 한 번에 가져올 데이터의 양
    rows = []
    columns = [i[0] for i in cursor.description]

    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        rows.extend(batch)

    # DataFrame 생성
    df = pd.DataFrame(rows, columns=columns)

    # 결과를 CSV 파일로 저장
    df.to_csv('opt10081_data.csv', index=False)

    # 커서와 연결 종료
    cursor.close()
    db.close()

def row_count(file_path):
    # CSV 파일 로드
    df = pd.read_csv(file_path)

    # 행의 수 출력
    row_count = len(df)
    print(f"행의 수: {row_count}")

# print_csv('merged_v1.csv')
# opt10081_to_csv()
# row_count('opt10081_data.csv')