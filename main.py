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

def insert_opt10081():
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
        
        # 데이터 전처리: 빈 값('')을 None으로 변환
        preprocessed_data = []
        for row in data.itertuples(index=False):
            preprocessed_row = (code,) + tuple(None if cell == '' else cell for cell in row[1:])
            preprocessed_data.append(preprocessed_row)

        # INSERT 쿼리 작성
        sql = "INSERT IGNORE INTO opt10081 (stock_code, current_price, trading_volume, trading_amount, date, open_price, high_price, low_price, adjusted_price_flag, adjusted_price_ratio, sector, sub_sector, stock_info, adjusted_price_event, previous_close_price) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        
        # 쿼리 실행
        if preprocessed_data:  # 데이터가 비어있지 않은 경우에만 실행
            cursor.executemany(sql, preprocessed_data)

        # 변경 사항 커밋
        db.commit()

        time.sleep(3.6)

def insert_opt10001():
    now = datetime.datetime.now()
    today = now.strftime("%Y%m%d")

    opt = "opt10001"
    for i, code in enumerate(codes):
        print(f"{i}/{len(codes)} {code}")
        data = kiwoom.block_request(opt,
                              종목코드=code,
                              기준일자=today,
                              수정주가구분=1,
                              output="주식일봉차트조회",
                              next=0)
                        
        # 데이터 전처리: 빈 값('')을 None으로 변환
        preprocessed_data = []
        for row in data.itertuples(index=False):
            preprocessed_row = (code,) + tuple(None if cell == '' else cell for cell in row[1:])
            preprocessed_data.append(preprocessed_row)

        # INSERT 쿼리 작성
        sql = "INSERT INTO opt10001 (stock_code, stock_name, fiscal_month, par_value, capital, listed_shares, credit_ratio, year_high, year_low, market_cap, market_cap_ratio, foreign_ownership_ratio, reference_price, PER, EPS, ROE, PBR, EV, BPS, revenue, operating_profit, net_income, high_250, low_250, open_price, high_price, low_price, upper_limit, lower_limit, standard_price, expected_price, expected_volume, high_250_date, high_250_ratio, low_250_date, low_250_ratio, current_price, change_symbol, change_from_prev, change_rate, volume, volume_ratio, par_value_unit, outstanding_shares, float_ratio)   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE stock_name = VALUES(stock_name), fiscal_month = VALUES(fiscal_month), par_value = VALUES(par_value), capital = VALUES(capital), listed_shares = VALUES(listed_shares), credit_ratio = VALUES(credit_ratio), year_high = VALUES(year_high), year_low = VALUES(year_low), market_cap = VALUES(market_cap), market_cap_ratio = VALUES(market_cap_ratio), foreign_ownership_ratio = VALUES(foreign_ownership_ratio), reference_price = VALUES(reference_price), PER = VALUES(PER), EPS = VALUES(EPS), ROE = VALUES(ROE), PBR = VALUES(PBR), EV = VALUES(EV), BPS = VALUES(BPS), revenue = VALUES(revenue), operating_profit = VALUES(operating_profit), net_income = VALUES(net_income), high_250 = VALUES(high_250), low_250 = VALUES(low_250), open_price = VALUES(open_price), high_price = VALUES(high_price), low_price = VALUES(low_price), upper_limit = VALUES(upper_limit), lower_limit = VALUES(lower_limit), standard_price = VALUES(standard_price), expected_price = VALUES(expected_price), expected_volume = VALUES(expected_volume), high_250_date = VALUES(high_250_date), high_250_ratio = VALUES(high_250_ratio), low_250_date = VALUES(low_250_date), low_250_ratio = VALUES(low_250_ratio), current_price = VALUES(current_price), change_symbol = VALUES(change_symbol), change_from_prev = VALUES(change_from_prev), change_rate = VALUES(change_rate), volume = VALUES(volume), volume_ratio = VALUES(volume_ratio), par_value_unit = VALUES(par_value_unit), outstanding_shares = VALUES(outstanding_shares), float_ratio = VALUES(float_ratio)"
        
        # 쿼리 실행
        if preprocessed_data:  # 데이터가 비어있지 않은 경우에만 실행
            cursor.executemany(sql, preprocessed_data)

        # 변경 사항 커밋
        db.commit()

        time.sleep(3.6)

if __name__ == "__main__":
    # insert_opt10081()
    insert_opt10001()

    db.close()