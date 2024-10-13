import csv
import pandas as pd
from config import db_config
import mysql.connector
from sqlalchemy import create_engine

# csv 파일을 읽어서 출력
def print_csv(file_path):
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        for row in reader:
            print(row)  

# opt10081 테이블의 데이터를 읽어 CSV 파일로 저장
def opt10081_to_csv():
    try:
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
        
        # 커서 생성 및 쿼리 실행
        cursor = db.cursor()
        query = "SELECT * FROM opt10081"
        cursor.execute(query)

        # 데이터 저장 및 CSV 파일로 저장
        batch_size = 1000
        columns = [i[0] for i in cursor.description]
        with open('opt10081_data.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(columns)
            
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                writer.writerows(batch)

    except mysql.connector.Error as e:
        print(f"Error: {e}")
    finally:
        # 커서와 연결 종료
        cursor.close()
        db.close()

# CSV 파일의 행의 수 출력
def row_count(file_path):
    # CSV 파일 로드
    df = pd.read_csv(file_path)

    # 행의 수 출력
    row_count = len(df)
    print(f"행의 수: {row_count}")
