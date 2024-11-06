import pandas as pd
from sqlalchemy import text
from telegram_bot import send_message
from database import get_latest_dates

# PBR 상위 30% && PER 상위 30% && ROE 상위 10%인 종목 필터링
def filtering_function1(cursor, lastest_date):
    sql_filtering = f"""
    WITH RankedData AS (
        SELECT *,
            NTILE(100) OVER (ORDER BY PBR) AS PBR_Percentile,
            NTILE(100) OVER (ORDER BY PER) AS PER_Percentile,
            NTILE(100) OVER (ORDER BY ROE) AS ROE_Percentile
        FROM opt10001
        WHERE date = '{lastest_date}'
    )
    SELECT stock_code, date
    FROM RankedData
    WHERE PBR_Percentile > 30 AND PER_Percentile > 30 AND ROE_Percentile > 10;
    """
    
    cursor.execute(sql_filtering)
    filtered_stocks = cursor.fetchall()
    
    filtered_stock_codes = {code[0] for code in filtered_stocks}

    return filtered_stock_codes

# 표편/이평 상하위 10%, 거래대금 하위 30% 제거
def filtering_function2(db, lastest_date):
    sql = f"""
    SELECT stock_code, current_price, trading_amount FROM opt10081 WHERE date = '{lastest_date}'
    """

    df = pd.read_sql(sql, db)

    clo20 = df['current_price'].rolling(window=20).mean()
    df['clo20'] = round(clo20, 2)
    std20 = df['current_price'].rolling(window=20).std()
    df['Price_Volatility_20'] = std20 / clo20

    # Price_Volatility_20의 상하위 10% 제거
    df = df[(df['Price_Volatility_20'] >= df['Price_Volatility_20'].quantile(0.10)) & 
            (df['Price_Volatility_20'] <= df['Price_Volatility_20'].quantile(0.90))]

    # trading_amount의 하위 30% 제거
    df = df[df['trading_amount'] > df['trading_amount'].quantile(0.30)]

    return df['stock_code'].unique()

# PBR, PER, ROE를 통해 필터링
async def filtering(cursor, db, engine):
    await send_message("filtering 시작")

    lastest_date = get_latest_dates(cursor)

    # PBR 상위 30% && PER 상위 30% && ROE 상위 10%인 종목 필터링
    filtered_codes1 = filtering_function1(cursor, lastest_date)  

    # 표편/이평 상하위 10%, 거래대금 하위 30% 제거
    filtered_codes2 = filtering_function2(db, lastest_date)

    filtered_codes_final = filtered_codes1.union(filtered_codes2)

    # 필터링 결과가 있다면
    if filtered_codes_final:
        # 최종 필터링된 종목들을 stock_signal 테이블의 filtering 열에 1로 업데이트
        codes_placeholder = ', '.join(['%s'] * len(filtered_codes_final))
        sql_update_filtered = f"""
        INSERT INTO stock_signal (stock_code, date, filtering)
        VALUES ({codes_placeholder}, %s, 1)
        ON DUPLICATE KEY UPDATE filtering = 1
        """
        cursor.execute(sql_update_filtered, (*filtered_codes_final, lastest_date))
        
        # 최종 필터링되지 않은 종목들을 stock_signal 테이블의 filtering 열에 0으로 업데이트
        sql_update_unfiltered = f"""
        INSERT INTO stock_signal (stock_code, date, filtering)
        SELECT stock_code, %s, 0
        FROM opt10001
        WHERE stock_code NOT IN ({codes_placeholder})
        ON DUPLICATE KEY UPDATE filtering = 0
        """
        cursor.execute(sql_update_unfiltered, (lastest_date, *filtered_codes_final))
        db.commit()
        await send_message("filtering 완료")
    else:
        await send_message("filtering 결과 없음")

# 실제 rolling 연산 수행
async def rolling_window(df):
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

    # vol5 = df['trading_volume'].rolling(window=5).mean()
    # vol20 = df['trading_volume'].rolling(window=20).mean()
    # vol60 = df['trading_volume'].rolling(window=60).mean()
    # vol120 = df['trading_volume'].rolling(window=120).mean()
    # vol200 = df['trading_volume'].rolling(window=200).mean()

    # df['vol5'] = df['trading_volume'].rolling(window=5).mean()
    # df['vol10'] = df['trading_volume'].rolling(window=10).mean()
    # df['vol20'] = df['trading_volume'].rolling(window=20).mean()
    # df['vol40'] = df['trading_volume'].rolling(window=40).mean()
    # df['vol60'] = df['trading_volume'].rolling(window=60).mean()
    # df['vol80'] = df['trading_volume'].rolling(window=80).mean()
    # df['vol100'] = df['trading_volume'].rolling(window=100).mean()
    # df['vol120'] = df['trading_volume'].rolling(window=120).mean()

    # vstd5 = df['trading_volume'].rolling(window=5).std()
    # vstd20 = df['trading_volume'].rolling(window=20).std()
    # vstd60 = df['trading_volume'].rolling(window=60).std()
    # vstd120 = df['trading_volume'].rolling(window=120).std()
    # vstd200 = df['trading_volume'].rolling(window=200).std()

    # df['Volume_Disparity_5'] = df['trading_volume'] / vol5
    # df['Volume_Disparity_20'] = df['trading_volume'] / vol20
    # df['Volume_Disparity_60'] = df['trading_volume'] / vol60
    # df['Volume_Disparity_120'] = df['trading_volume'] / vol120
    # df['Volume_Disparity_200'] = df['trading_volume'] / vol200

    # df['Volume_Volatility_5'] = vstd5 / vol5
    # df['Volume_Volatility_20'] = vstd20 / vol20
    # df['Volume_Volatility_60'] = vstd60 / vol60
    # df['Volume_Volatility_120'] = vstd120 / vol120
    # df['Volume_Volatility_200'] = vstd200 / vol200

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

    # df['Volume_Disparity_5_20'] = vol5 / vol20
    # df['Volume_Disparity_5_60'] = vol5 / vol60
    # df['Volume_Disparity_5_120'] = vol5 / vol120
    # df['Volume_Disparity_5_200'] = vol5 / vol200
    # df['Volume_Disparity_20_60'] = vol20 / vol60
    # df['Volume_Disparity_20_120'] = vol20 / vol120
    # df['Volume_Disparity_20_200'] = vol20 / vol200
    # df['Volume_Disparity_60_120'] = vol60 / vol120
    # df['Volume_Disparity_60_200'] = vol60 / vol200
    # df['Volume_Disparity_120_200'] = vol120 / vol200

    # df['Volume_Volatility_5_20'] = vstd5 / vstd20
    # df['Volume_Volatility_5_60'] = vstd5 / vstd60
    # df['Volume_Volatility_5_120'] = vstd5 / vstd120
    # df['Volume_Volatility_5_200'] = vstd5 / vstd200
    # df['Volume_Volatility_20_60'] = vstd20 / vstd60
    # df['Volume_Volatility_20_120'] = vstd20 / vstd120
    # df['Volume_Volatility_20_200'] = vstd20 / vstd200
    # df['Volume_Volatility_60_120'] = vstd60 / vstd120
    # df['Volume_Volatility_60_200'] = vstd60 / vstd200
    # df['Volume_Volatility_120_200'] = vstd120 / vstd200

    return df

# rolling window를 통한 데이터 전처리
async def data_processing(cursor, db, engine):
    await send_message("데이터 전처리 시작")

    lastest_date = get_latest_dates(cursor)
    lastest_date = '2024-11-05'

    # stock_signal 테이블에서 필터링된 종목들을 가져옴
    query = f"SELECT DISTINCT stock_code FROM stock_signal WHERE date = '{lastest_date}' and filtering = 1"
    query_result = pd.read_sql(query, db)
    unique_code_list = query_result['stock_code'].tolist()
    
    # @@ 효율적인 방법 구상 @@
    for i, code in enumerate(unique_code_list):
        print(f"{i+1}/{len(unique_code_list)}: {code}")
        
        sql = f"SELECT * FROM opt10081 WHERE stock_code = '{code}'"
        df = pd.read_sql(sql, db)
        df.sort_values(by='date', ascending=True, inplace=True)

        # rolling window 연산 수행
        df = await rolling_window(df)

        # Nan, inf 값 0으로 치환
        df.replace([float('inf'), -float('inf')], 0, inplace=True)
        df.fillna(0, inplace=True)

        # date 열에 존재하는 0값을 None으로 치환
        df['date'] = df['date'].apply(lambda x: None if x == 0 else x)

        # 변환된 데이터 확인 (디버깅용)
        # 만약 date 열에 0이 있다? 그럼 다시 심사숙고 하세요...
        # print(df['date'].unique())
        # print(df['stock_code'].unique())

        # DataFrame을 직접 data_processed 테이블에 삽입
        for index, row in df.iterrows():
            # 'date' 열이 None인지 확인하고, None이 아닌 경우에만 삽입
            if row['date'] is not None:
                insert_query = text("""
                INSERT INTO data_processed (stock_code, date, current_price, trading_volume, clo5, clo20, clo60, clo120, clo200, 
                            Price_Disparity_5, Price_Disparity_20, Price_Disparity_60, Price_Disparity_120, Price_Disparity_200, 
                            Price_Volatility_5, Price_Volatility_20, Price_Volatility_60, Price_Volatility_120, Price_Volatility_200,
                            Price_Disparity_5_20, Price_Disparity_5_60, Price_Disparity_5_120, Price_Disparity_5_200,
                            Price_Disparity_20_60, Price_Disparity_20_120, Price_Disparity_20_200, Price_Disparity_60_120, 
                            Price_Disparity_60_200, Price_Disparity_120_200,
                            Price_Volatility_5_20, Price_Volatility_5_60, Price_Volatility_5_120, Price_Volatility_5_200,
                            Price_Volatility_20_60, Price_Volatility_20_120, Price_Volatility_20_200, 
                            Price_Volatility_60_120, Price_Volatility_60_200, Price_Volatility_120_200)
                VALUES (:stock_code, :date, :current_price, :trading_volume, :clo5, :clo20, :clo60, :clo120, :clo200, 
                :Price_Disparity_5, :Price_Disparity_20, :Price_Disparity_60, :Price_Disparity_120, :Price_Disparity_200, 
                :Price_Volatility_5, :Price_Volatility_20, :Price_Volatility_60, :Price_Volatility_120, :Price_Volatility_200,
                :Price_Disparity_5_20, :Price_Disparity_5_60, :Price_Disparity_5_120, :Price_Disparity_5_200,
                :Price_Disparity_20_60, :Price_Disparity_20_120, :Price_Disparity_20_200, :Price_Disparity_60_120, 
                :Price_Disparity_60_200, :Price_Disparity_120_200,
                :Price_Volatility_5_20, :Price_Volatility_5_60, :Price_Volatility_5_120, :Price_Volatility_5_200,
                :Price_Volatility_20_60, :Price_Volatility_20_120, :Price_Volatility_20_200, 
                :Price_Volatility_60_120, :Price_Volatility_60_200, :Price_Volatility_120_200)
                """)
                with engine.connect() as conn:
                    conn.execute(insert_query, **row.to_dict())

    await send_message("데이터 전처리 완료")
