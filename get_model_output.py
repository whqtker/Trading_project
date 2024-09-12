import numpy as np
import pandas as pd
import joblib
import mysql.connector
from database import connect_and_create_engine, get_latest_dates
from config import db_config

def get_model_output():
    try:
        db, cursor, engine = connect_and_create_engine()

        # 최신 날짜
        lastest_date = get_latest_dates(cursor)

        query = f"SELECT * FROM data_processed WHERE date = '{lastest_date}'"  
        data = pd.read_sql(query, engine)

        # 피처 컬럼 정의
        feature_columns = [
            'Price_Disparity_5', 'Price_Disparity_20', 'Price_Disparity_60', 'Price_Disparity_120', 'Price_Disparity_200',
            'Price_Volatility_5', 'Price_Volatility_20', 'Price_Volatility_60', 'Price_Volatility_120', 'Price_Volatility_200',
            'Price_Disparity_5_20', 'Price_Disparity_5_60', 'Price_Disparity_5_120', 'Price_Disparity_5_200',
            'Price_Disparity_20_60', 'Price_Disparity_20_120', 'Price_Disparity_20_200',
            'Price_Disparity_60_120', 'Price_Disparity_60_200', 'Price_Disparity_120_200',
            'Volume_Disparity_5', 'Volume_Disparity_20', 'Volume_Disparity_60', 'Volume_Disparity_120', 'Volume_Disparity_200',
            'Volume_Volatility_5', 'Volume_Volatility_20', 'Volume_Volatility_60', 'Volume_Volatility_120', 'Volume_Volatility_200',
            'Volume_Disparity_5_20', 'Volume_Disparity_5_60', 'Volume_Disparity_5_120', 'Volume_Disparity_5_200',
            'Volume_Disparity_20_60', 'Volume_Disparity_20_120', 'Volume_Disparity_20_200',
            'Volume_Disparity_60_120', 'Volume_Disparity_60_200', 'Volume_Disparity_120_200',
            'Price_Volatility_5_20', 'Price_Volatility_5_60', 'Price_Volatility_5_120', 'Price_Volatility_5_200',
            'Price_Volatility_20_60', 'Price_Volatility_20_120', 'Price_Volatility_20_200',
            'Price_Volatility_60_120', 'Price_Volatility_60_200', 'Price_Volatility_120_200',
            'Volume_Volatility_5_20', 'Volume_Volatility_5_60', 'Volume_Volatility_5_120', 'Volume_Volatility_5_200',
            'Volume_Volatility_20_60', 'Volume_Volatility_20_120', 'Volume_Volatility_20_200',
            'Volume_Volatility_60_120', 'Volume_Volatility_60_200', 'Volume_Volatility_120_200'
        ]

        # 피처 데이터를 넘파이 배열로 변환
        features = data[feature_columns].values.astype(np.float32)

        # 학습된 모델 로드
        model = joblib.load('./learned_model/logstacking_model_maxfeatures.pkl')

        # 모델 예측 수행
        predictions = model.predict(features)
        probabilities = model.predict_proba(features)[:, 1]  # 클래스 1의 확률

        # stock_code와 date를 가져옴
        stock_codes = data['stock_code'].values
        dates = data['date'].values

        # 예측값과 확률에 맞게 stock_signal 테이블 업데이트
        for stock_code, date, prediction, probability in zip(stock_codes, dates, predictions, probabilities):
            sql_update = """
            INSERT INTO stock_signal (stock_code, date, Predicted_Class, Predicted_Probability)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Predicted_Class = VALUES(Predicted_Class),
                Predicted_Probability = VALUES(Predicted_Probability)
            """
            cursor.execute(sql_update, (stock_code, date, int(prediction), float(probability)))

        db.commit() 
        print("stock_signal 테이블 업데이트 완료")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'db' in locals() and db:
            db.close()
        if 'engine' in locals() and engine:
            engine.dispose()