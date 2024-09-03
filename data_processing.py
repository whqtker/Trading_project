import pandas as pd
from sqlalchemy import text

# PBR, PER, ROE를 통해 필터링
def filtering(cursor, db):
    sql_filtering = """
    WITH RankedData AS (
        SELECT *,
            NTILE(100) OVER (ORDER BY PBR) AS PBR_Percentile,
            NTILE(100) OVER (ORDER BY PER) AS PER_Percentile,
            NTILE(100) OVER (ORDER BY ROE) AS ROE_Percentile
        FROM opt10001
    )
    SELECT stock_code, date
    FROM RankedData
    WHERE PBR_Percentile > 30 AND PER_Percentile > 30 AND ROE_Percentile > 10;
    """
    
    cursor.execute(sql_filtering)
    filtered_stocks = cursor.fetchall()
    
    # Step 2: Create a list of stock codes that meet the criteria
    filtered_stock_codes = {code[0] for code in filtered_stocks}  # Set for faster lookup
    date_value = filtered_stocks[0][1] if filtered_stocks else None  # Get the date from the first result

    # Step 3: Update the filtering values in stock_signal table
    if date_value is not None:
        # Update filtering to 1 for filtered stocks
        sql_update_filtered = f"""
        INSERT INTO stock_signal (stock_code, date, filtering)
        SELECT stock_code, %s, 1
        FROM opt10001
        WHERE stock_code IN ({', '.join(['%s'] * len(filtered_stock_codes))})
        ON DUPLICATE KEY UPDATE filtering = 1;
        """
        
        cursor.execute(sql_update_filtered, (date_value, *filtered_stock_codes))
        
        # Update filtering to 0 for stocks that are not filtered
        sql_update_unfiltered = f"""
        INSERT INTO stock_signal (stock_code, date, filtering)
        SELECT stock_code, %s, 0
        FROM opt10001
        WHERE stock_code NOT IN ({', '.join(['%s'] * len(filtered_stock_codes))})
        ON DUPLICATE KEY UPDATE filtering = 0;
        """
        
        cursor.execute(sql_update_unfiltered, (date_value, *filtered_stock_codes))
        
        db.commit()  # Commit the changes to the database
        print("필터링 완료")

# rolling window를 통한 데이터 전처리
def data_processing(cursor, db, engine):
    query = "SELECT DISTINCT stock_code FROM opt10081"
    unique_code_df = pd.read_sql(query, db)
    unique_code_list = unique_code_df['stock_code'].tolist()
    
    for i, code in enumerate(unique_code_list):
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

        # DataFrame을 직접 data_processed 테이블에 삽입
        for index, row in df.iterrows():
            insert_query = text("""
                INSERT INTO data_processed (stock_code, date, current_price, trading_volume, clo5, clo20, clo60, clo120, clo200, 
                                            Price_Disparity_5, Price_Disparity_20, Price_Disparity_60, Price_Disparity_120, Price_Disparity_200, 
                                            Price_Volatility_5, Price_Volatility_20, Price_Volatility_60, Price_Volatility_120, Price_Volatility_200, 
                                            vol5, vol10, vol20, vol40, vol60, vol80, vol100, vol120, 
                                            Volume_Disparity_5, Volume_Disparity_20, Volume_Disparity_60, Volume_Disparity_120, Volume_Disparity_200, 
                                            Volume_Volatility_5, Volume_Volatility_20, Volume_Volatility_60, Volume_Volatility_120, Volume_Volatility_200, 
                                            Price_Disparity_5_20, Price_Disparity_5_60, Price_Disparity_5_120, Price_Disparity_5_200, 
                                            Price_Disparity_20_60, Price_Disparity_20_120, Price_Disparity_20_200, Price_Disparity_60_120, 
                                            Price_Disparity_60_200, Price_Disparity_120_200, Price_Volatility_5_20, Price_Volatility_5_60, 
                                            Price_Volatility_5_120, Price_Volatility_5_200, Price_Volatility_20_60, Price_Volatility_20_120, 
                                            Price_Volatility_20_200, Price_Volatility_60_120, Price_Volatility_60_200, Price_Volatility_120_200, 
                                            Volume_Disparity_5_20, Volume_Disparity_5_60, Volume_Disparity_5_120, Volume_Disparity_5_200, 
                                            Volume_Disparity_20_60, Volume_Disparity_20_120, Volume_Disparity_20_200, Volume_Disparity_60_120, 
                                            Volume_Disparity_60_200, Volume_Disparity_120_200, Volume_Volatility_5_20, Volume_Volatility_5_60, 
                                            Volume_Volatility_5_120, Volume_Volatility_5_200, Volume_Volatility_20_60, Volume_Volatility_20_120, 
                                            Volume_Volatility_20_200, Volume_Volatility_60_120, Volume_Volatility_60_200, Volume_Volatility_120_200)
                VALUES (:stock_code, :date, :current_price, :trading_volume, :clo5, :clo20, :clo60, :clo120, :clo200, 
                        :Price_Disparity_5, :Price_Disparity_20, :Price_Disparity_60, :Price_Disparity_120, :Price_Disparity_200, 
                        :Price_Volatility_5, :Price_Volatility_20, :Price_Volatility_60, :Price_Volatility_120, :Price_Volatility_200, 
                        :vol5, :vol10, :vol20, :vol40, :vol60, :vol80, :vol100, :vol120, 
                        :Volume_Disparity_5, :Volume_Disparity_20, :Volume_Disparity_60, :Volume_Disparity_120, :Volume_Disparity_200, 
                        :Volume_Volatility_5, :Volume_Volatility_20, :Volume_Volatility_60, :Volume_Volatility_120, :Volume_Volatility_200, 
                        :Price_Disparity_5_20, :Price_Disparity_5_60, :Price_Disparity_5_120, :Price_Disparity_5_200, 
                        :Price_Disparity_20_60, :Price_Disparity_20_120, :Price_Disparity_20_200, :Price_Disparity_60_120, 
                        :Price_Disparity_60_200, :Price_Disparity_120_200, :Price_Volatility_5_20, :Price_Volatility_5_60, 
                        :Price_Volatility_5_120, :Price_Volatility_5_200, :Price_Volatility_20_60, :Price_Volatility_20_120, 
                        :Price_Volatility_20_200, :Price_Volatility_60_120, :Price_Volatility_60_200, :Price_Volatility_120_200, 
                        :Volume_Disparity_5_20, :Volume_Disparity_5_60, :Volume_Disparity_5_120, :Volume_Disparity_5_200, 
                        :Volume_Disparity_20_60, :Volume_Disparity_20_120, :Volume_Disparity_20_200, :Volume_Disparity_60_120, 
                        :Volume_Disparity_60_200, :Volume_Disparity_120_200, :Volume_Volatility_5_20, :Volume_Volatility_5_60, 
                        :Volume_Volatility_5_120, :Volume_Volatility_5_200, :Volume_Volatility_20_60, :Volume_Volatility_20_120, 
                        :Volume_Volatility_20_200, :Volume_Volatility_60_120, :Volume_Volatility_60_200, :Volume_Volatility_120_200)
                ON DUPLICATE KEY UPDATE
                    current_price = VALUES(current_price),
                    trading_volume = VALUES(trading_volume),
                    clo5 = VALUES(clo5),
                    clo20 = VALUES(clo20),
                    clo60 = VALUES(clo60),
                    clo120 = VALUES(clo120),
                    clo200 = VALUES(clo200),
                    Price_Disparity_5 = VALUES(Price_Disparity_5),
                    Price_Disparity_20 = VALUES(Price_Disparity_20),
                    Price_Disparity_60 = VALUES(Price_Disparity_60),
                    Price_Disparity_120 = VALUES(Price_Disparity_120),
                    Price_Disparity_200 = VALUES(Price_Disparity_200),
                    Price_Volatility_5 = VALUES(Price_Volatility_5),
                    Price_Volatility_20 = VALUES(Price_Volatility_20),
                    Price_Volatility_60 = VALUES(Price_Volatility_60),
                    Price_Volatility_120 = VALUES(Price_Volatility_120),
                    Price_Volatility_200 = VALUES(Price_Volatility_200),
                    vol5 = VALUES(vol5),
                    vol10 = VALUES(vol10),
                    vol20 = VALUES(vol20),
                    vol40 = VALUES(vol40),
                    vol60 = VALUES(vol60),
                    vol80 = VALUES(vol80),
                    vol100 = VALUES(vol100),
                    vol120 = VALUES(vol120),
                    Volume_Disparity_5 = VALUES(Volume_Disparity_5),
                    Volume_Disparity_20 = VALUES(Volume_Disparity_20),
                    Volume_Disparity_60 = VALUES(Volume_Disparity_60),
                    Volume_Disparity_120 = VALUES(Volume_Disparity_120),
                    Volume_Disparity_200 = VALUES(Volume_Disparity_200),
                    Volume_Volatility_5 = VALUES(Volume_Volatility_5),
                    Volume_Volatility_20 = VALUES(Volume_Volatility_20),
                    Volume_Volatility_60 = VALUES(Volume_Volatility_60),
                    Volume_Volatility_120 = VALUES(Volume_Volatility_120),
                    Volume_Volatility_200 = VALUES(Volume_Volatility_200),
                    Price_Disparity_5_20 = VALUES(Price_Disparity_5_20),
                    Price_Disparity_5_60 = VALUES(Price_Disparity_5_60),
                    Price_Disparity_5_120 = VALUES(Price_Disparity_5_120),
                    Price_Disparity_5_200 = VALUES(Price_Disparity_5_200),
                    Price_Disparity_20_60 = VALUES(Price_Disparity_20_60),
                    Price_Disparity_20_120 = VALUES(Price_Disparity_20_120),
                    Price_Disparity_20_200 = VALUES(Price_Disparity_20_200),
                    Price_Disparity_60_120 = VALUES(Price_Disparity_60_120),
                    Price_Disparity_60_200 = VALUES(Price_Disparity_60_200),
                    Price_Disparity_120_200 = VALUES(Price_Disparity_120_200),
                    Price_Volatility_5_20 = VALUES(Price_Volatility_5_20),
                    Price_Volatility_5_60 = VALUES(Price_Volatility_5_60),
                    Price_Volatility_5_120 = VALUES(Price_Volatility_5_120),
                    Price_Volatility_5_200 = VALUES(Price_Volatility_5_200),
                    Price_Volatility_20_60 = VALUES(Price_Volatility_20_60),
                    Price_Volatility_20_120 = VALUES(Price_Volatility_20_120),
                    Price_Volatility_20_200 = VALUES(Price_Volatility_20_200),
                    Price_Volatility_60_120 = VALUES(Price_Volatility_60_120),
                    Price_Volatility_60_200 = VALUES(Price_Volatility_60_200),
                    Price_Volatility_120_200 = VALUES(Price_Volatility_120_200),
                    Volume_Disparity_5_20 = VALUES(Volume_Disparity_5_20),
                    Volume_Disparity_5_60 = VALUES(Volume_Disparity_5_60),
                    Volume_Disparity_5_120 = VALUES(Volume_Disparity_5_120),
                    Volume_Disparity_5_200 = VALUES(Volume_Disparity_5_200),
                    Volume_Disparity_20_60 = VALUES(Volume_Disparity_20_60),
                    Volume_Disparity_20_120 = VALUES(Volume_Disparity_20_120),
                    Volume_Disparity_20_200 = VALUES(Volume_Disparity_20_200),
                    Volume_Disparity_60_120 = VALUES(Volume_Disparity_60_120),
                    Volume_Disparity_60_200 = VALUES(Volume_Disparity_60_200),
                    Volume_Disparity_120_200 = VALUES(Volume_Disparity_120_200),
                    Volume_Volatility_5_20 = VALUES(Volume_Volatility_5_20),
                    Volume_Volatility_5_60 = VALUES(Volume_Volatility_5_60),
                    Volume_Volatility_5_120 = VALUES(Volume_Volatility_5_120),
                    Volume_Volatility_5_200 = VALUES(Volume_Volatility_5_200),
                    Volume_Volatility_20_60 = VALUES(Volume_Volatility_20_60),
                    Volume_Volatility_20_120 = VALUES(Volume_Volatility_20_120),
                    Volume_Volatility_20_200 = VALUES(Volume_Volatility_20_200),
                    Volume_Volatility_60_120 = VALUES(Volume_Volatility_60_120),
                    Volume_Volatility_60_200 = VALUES(Volume_Volatility_60_200),
                    Volume_Volatility_120_200 = VALUES(Volume_Volatility_120_200)
            """)
            with engine.connect() as conn:
                conn.execute(insert_query, **row.to_dict())

        print(f"{i+1}/{len(unique_code_list)} {code} 데이터 전처리 완료")

    print("데이터 전처리 완료")