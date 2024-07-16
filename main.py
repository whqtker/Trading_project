from pykiwoom.kiwoom import *
import datetime
import time
import pandas as pd
import os
import shutil
import mysql.connector

# 로그인
kiwoom = Kiwoom()
kiwoom.CommConnect()

# 전종목 종목코드
kospi = kiwoom.GetCodeListByMarket('0')
kosdaq = kiwoom.GetCodeListByMarket('10')
codes = kospi + kosdaq

# MySQL 데이터베이스 연결 정보
db_config = {
    'user': 'root',
    'password': '1234',
    'host': 'localhost',
    'port': 3306,
    'database': 'project'
}

# MySQL 데이터베이스 연결
db = mysql.connector.connect(
    user=db_config['user'],
    password=db_config['password'],
    host=db_config['host'],
    port=db_config['port'],
    database=db_config['database']
)

cursor = db.cursor()

def insert_today_stock_data():
    now = datetime.datetime.now()
    today = now.strftime("%Y%m%d")

    opt = "opt10081"
    for i, code in enumerate(codes):
        print(f"{i}/{len(codes)} {code}")
        data = kiwoom.block_request(opt,
                              종목코드=code,
                              기준일자=today,
                              수정주가구분=1,
                              output="주식일봉차트조회",
                              next=0)
        
        print(data)

        # 데이터 전처리: 빈 값('')을 None으로 변환
        preprocessed_data = []
        for row in data.itertuples(index=False):
            preprocessed_row = (code,) + tuple(None if cell == '' else cell for cell in row[1:])
            preprocessed_data.append(preprocessed_row)

        # INSERT 쿼리 작성
        sql = "INSERT INTO stock (stock_code, current_price, trading_volume, trading_amount, date, open_price, high_price, low_price, adjusted_price_flag, adjusted_price_ratio, sector, sub_sector, stock_info, adjusted_price_event, previous_close_price) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        
        # 쿼리 실행
        if preprocessed_data:  # 데이터가 비어있지 않은 경우에만 실행
            cursor.executemany(sql, preprocessed_data)

        # 변경 사항 커밋
        db.commit()

        time.sleep(3.6)

if __name__ == "__main__":
    insert_today_stock_data()

    db.close()