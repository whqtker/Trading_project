from pykiwoom.kiwoom import *
import datetime
import time
import pandas as pd
import os
import shutil
import mysql.connector
from sqlalchemy import create_engine

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

# SQLAlchemy 엔진 생성
engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

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

def filtering():
    sql = """
    WITH RankedData AS (
        SELECT *,
            NTILE(100) OVER (ORDER BY PBR) AS PBR_Percentile,
            NTILE(100) OVER (ORDER BY PER) AS PER_Percentile,
            NTILE(100) OVER (ORDER BY ROE) AS ROE_Percentile
        FROM opt10001
    )
    SELECT stock_code, stock_name, PBR, PER, ROE
    FROM RankedData
    WHERE PBR_Percentile > 30 AND PER_Percentile > 30 AND ROE_Percentile > 10;
    """
    
    cursor.execute(sql)
    
    result = cursor.fetchall()
    
    for row in result:
        print(row)

def data_processing():
    query = "SELECT DISTINCT stock_code FROM opt10081"
    unique_code_df = pd.read_sql(query, db)
    unique_code_list = unique_code_df['stock_code'].tolist()
    
    for code in unique_code_list:
        sql = f"SELECT * FROM opt10081 WHERE stock_code = '{code}'"
        df = pd.read_sql(sql, db)
        df.sort_values(by='date', ascending=True, inplace=True)

        clo5 = df['current_price'].rolling(window=5).mean()
        clo20 = df['current_price'].rolling(window=20).mean()
        clo60 = df['current_price'].rolling(window=60).mean()
        clo120 = df['current_price'].rolling(window=120).mean()
        clo200 = df['current_price'].rolling(window=200).mean()

        df['clo5'] = round(clo5, 2)
        df['clo20'] = round(clo20, 2)
        df['clo60'] = round(clo60, 2)
        df['clo120'] = round(clo120, 2)
        df['clo200'] = round(clo200, 2)

        std5 = df['current_price'].rolling(window=5).std()
        std20 = df['current_price'].rolling(window=20).std()
        std60 = df['current_price'].rolling(window=60).std()
        std120 = df['current_price'].rolling(window=120).std()
        std200 = df['current_price'].rolling(window=200).std()

        df['Price_Disparity_5'] = df['current_price'] / clo5
        df['Price_Disparity_20'] = df['current_price'] / clo20
        df['Price_Disparity_60'] = df['current_price'] / clo60
        df['Price_Disparity_120'] = df['current_price'] / clo120
        df['Price_Disparity_200'] = df['current_price'] / clo200

        df['Price_Volatility_5'] = std5 / clo5
        df['Price_Volatility_20'] = std20 / clo20
        df['Price_Volatility_60'] = std60 / clo60
        df['Price_Volatility_120'] = std120 / clo120
        df['Price_Volatility_200'] = std200 / clo200

        vol5 = df['trading_volume'].rolling(window=5).mean()
        vol20 = df['trading_volume'].rolling(window=20).mean()
        vol60 = df['trading_volume'].rolling(window=60).mean()
        vol120 = df['trading_volume'].rolling(window=120).mean()
        vol200 = df['trading_volume'].rolling(window=200).mean()

        df['vol5'] = df['trading_volume'].rolling(window=5).mean()
        df['vol10'] = df['trading_volume'].rolling(window=10).mean()
        df['vol20'] = df['trading_volume'].rolling(window=20).mean()
        df['vol40'] = df['trading_volume'].rolling(window=40).mean()
        df['vol60'] = df['trading_volume'].rolling(window=60).mean()
        df['vol80'] = df['trading_volume'].rolling(window=80).mean()
        df['vol100'] = df['trading_volume'].rolling(window=100).mean()
        df['vol120'] = df['trading_volume'].rolling(window=120).mean()

        vstd5 = df['trading_volume'].rolling(window=5).std()
        vstd20 = df['trading_volume'].rolling(window=20).std()
        vstd60 = df['trading_volume'].rolling(window=60).std()
        vstd120 = df['trading_volume'].rolling(window=120).std()
        vstd200 = df['trading_volume'].rolling(window=200).std()

        df['Volume_Disparity_5'] = df['trading_volume'] / vol5
        df['Volume_Disparity_20'] = df['trading_volume'] / vol20
        df['Volume_Disparity_60'] = df['trading_volume'] / vol60
        df['Volume_Disparity_120'] = df['trading_volume'] / vol120
        df['Volume_Disparity_200'] = df['trading_volume'] / vol200

        df['Volume_Volatility_5'] = vstd5 / vol5
        df['Volume_Volatility_20'] = vstd20 / vol20
        df['Volume_Volatility_60'] = vstd60 / vol60
        df['Volume_Volatility_120'] = vstd120 / vol120
        df['Volume_Volatility_200'] = vstd200 / vol200

        df['Price_Disparity_5_20'] = clo5 / clo20
        df['Price_Disparity_5_60'] = clo5 / clo60
        df['Price_Disparity_5_120'] = clo5 / clo120
        df['Price_Disparity_5_200'] = clo5 / clo200
        df['Price_Disparity_20_60'] = clo20 / clo60
        df['Price_Disparity_20_120'] = clo20 / clo120
        df['Price_Disparity_20_200'] = clo20 / clo200
        df['Price_Disparity_60_120'] = clo60 / clo120
        df['Price_Disparity_60_200'] = clo60 / clo200
        df['Price_Disparity_120_200'] = clo120 / clo200

        df['Price_Volatility_5_20'] = std5 / std20
        df['Price_Volatility_5_60'] = std5 / std60
        df['Price_Volatility_5_120'] = std5 / std120
        df['Price_Volatility_5_200'] = std5 / std200
        df['Price_Volatility_20_60'] = std20 / std60
        df['Price_Volatility_20_120'] = std20 / std120
        df['Price_Volatility_20_200'] = std20 / std200
        df['Price_Volatility_60_120'] = std60 / std120
        df['Price_Volatility_60_200'] = std60 / std200
        df['Price_Volatility_120_200'] = std120 / std200

        df['Volume_Disparity_5_20'] = vol5 / vol20
        df['Volume_Disparity_5_60'] = vol5 / vol60
        df['Volume_Disparity_5_120'] = vol5 / vol120
        df['Volume_Disparity_5_200'] = vol5 / vol200
        df['Volume_Disparity_20_60'] = vol20 / vol60
        df['Volume_Disparity_20_120'] = vol20 / vol120
        df['Volume_Disparity_20_200'] = vol20 / vol200
        df['Volume_Disparity_60_120'] = vol60 / vol120
        df['Volume_Disparity_60_200'] = vol60 / vol200
        df['Volume_Disparity_120_200'] = vol120 / vol200

        df['Volume_Volatility_5_20'] = vstd5 / vstd20
        df['Volume_Volatility_5_60'] = vstd5 / vstd60
        df['Volume_Volatility_5_120'] = vstd5 / vstd120
        df['Volume_Volatility_5_200'] = vstd5 / vstd200
        df['Volume_Volatility_20_60'] = vstd20 / vstd60
        df['Volume_Volatility_20_120'] = vstd20 / vstd120
        df['Volume_Volatility_20_200'] = vstd20 / vstd200
        df['Volume_Volatility_60_120'] = vstd60 / vstd120
        df['Volume_Volatility_60_200'] = vstd60 / vstd200
        df['Volume_Volatility_120_200'] = vstd120 / vstd200

        # Nan, inf 값 0으로 치환
        df.replace([float('inf'), -float('inf')], 0, inplace=True)
        df.fillna(0, inplace=True)

        df.to_sql(name='data_processed', con=engine, if_exists='append', index=False)

if __name__ == "__main__":
    # insert_opt10081()
    # insert_opt10001()
    # filtering()
    data_processing()

    db.close()
    engine.dispose()