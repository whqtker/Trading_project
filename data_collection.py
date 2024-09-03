import datetime
import time
from database import get_latest_dates

# 600일 치 opt10081 데이터 수집 및 저장
def insert_opt10081(cursor, db, kiwoom, codes):
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

# 600일 치 opt10001 데이터 수집 및 저장
# 값이 변동되지 않았다면 단순히 날짜만 수정
def insert_opt10001(cursor, db, kiwoom, codes):
    date = get_latest_dates()

    opt = "opt10001"
    for i, code in enumerate(codes):
        print(f"{i}/{len(codes)} {code}")
        data = kiwoom.block_request(opt,
                              종목코드=code,
                              기준일자=date,
                              수정주가구분=1,
                              output="주식일봉차트조회",
                              next=0)
                        
        # 데이터 전처리: 빈 값('')을 None으로 변환
        preprocessed_data = []
        for row in data.itertuples(index=False):
            preprocessed_row = (code, date) + tuple(None if cell == '' else cell for cell in row[1:])
            preprocessed_data.append(preprocessed_row)

        # INSERT 쿼리 작성
        sql_insert = """
        INSERT INTO opt10001 (stock_code, date, stock_name, fiscal_month, par_value, capital, listed_shares, credit_ratio, year_high, year_low, market_cap, market_cap_ratio, foreign_ownership_ratio, reference_price, PER, EPS, ROE, PBR, EV, BPS, revenue, operating_profit, net_income, high_250, low_250, open_price, high_price, low_price, upper_limit, lower_limit, standard_price, expected_price, expected_volume, high_250_date, high_250_ratio, low_250_date, low_250_ratio, current_price, change_symbol, change_from_prev, change_rate, volume, volume_ratio, par_value_unit, outstanding_shares, float_ratio)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            stock_name = VALUES(stock_name),
            date = VALUES(date),
            fiscal_month = VALUES(fiscal_month),
            par_value = VALUES(par_value),
            capital = VALUES(capital),
            listed_shares = VALUES(listed_shares),
            credit_ratio = VALUES(credit_ratio),
            year_high = VALUES(year_high),
            year_low = VALUES(year_low),
            market_cap = VALUES(market_cap),
            market_cap_ratio = VALUES(market_cap_ratio),
            foreign_ownership_ratio = VALUES(foreign_ownership_ratio),
            reference_price = VALUES(reference_price),
            PER = VALUES(PER),
            EPS = VALUES(EPS),
            ROE = VALUES(ROE),
            PBR = VALUES(PBR),
            EV = VALUES(EV),
            BPS = VALUES(BPS),
            revenue = VALUES(revenue),
            operating_profit = VALUES(operating_profit),
            net_income = VALUES(net_income),
            high_250 = VALUES(high_250),
            low_250 = VALUES(low_250),
            open_price = VALUES(open_price),
            high_price = VALUES(high_price),
            low_price = VALUES(low_price),
            upper_limit = VALUES(upper_limit),
            lower_limit = VALUES(lower_limit),
            standard_price = VALUES(standard_price),
            expected_price = VALUES(expected_price),
            expected_volume = VALUES(expected_volume),
            high_250_date = VALUES(high_250_date),
            high_250_ratio = VALUES(high_250_ratio),
            low_250_date = VALUES(low_250_date),
            low_250_ratio = VALUES(low_250_ratio),
            current_price = VALUES(current_price),
            change_symbol = VALUES(change_symbol),
            change_from_prev = VALUES(change_from_prev),
            change_rate = VALUES(change_rate),
            volume = VALUES(volume),
            volume_ratio = VALUES(volume_ratio),
            par_value_unit = VALUES(par_value_unit),
            outstanding_shares = VALUES(outstanding_shares),
            float_ratio = VALUES(float_ratio)
        """

        # 쿼리 실행
        for row in preprocessed_data:
            stock_code = row[0]
            date = row[1]
            values_without_date = row[2:]

            # 최신 날짜의 튜플 조회
            cursor.execute(f"SELECT * FROM opt10001 WHERE stock_code = %s ORDER BY date DESC LIMIT 1", (stock_code,))
            latest_row = cursor.fetchone()

            if latest_row:
                latest_values_without_date = latest_row[2:]

                # 최신 날짜의 튜플과 값 비교
                if values_without_date == latest_values_without_date:
                    # 최신 날짜의 튜플의 date 값을 업데이트
                    cursor.execute(f"UPDATE opt10001 SET date = %s WHERE stock_code = %s AND date = %s", (date, stock_code, latest_row[1]))
                else:
                    # 새로운 튜플 삽입
                    cursor.execute(sql_insert, row)
            else:
                # 새로운 튜플 삽입
                cursor.execute(sql_insert, row)

        # 변경 사항 커밋
        db.commit()

        time.sleep(3.6)

# 600일 치 opt20006 데이터 수집 및 저장
def insert_opt20006(cursor, db, kiwoom):
    date = get_latest_dates()

    opt = "opt20006"
    index = ["001", "101"]
    for i, code in enumerate(index):
        print(f"{i}/{len(index)} {code}")
        data = kiwoom.block_request(opt,
                              업종코드=code,
                              기준일자=date,
                              output="업종일봉차트조회",
                              next=0)
        
        # 데이터 전처리: 빈 값('')을 None으로 변환
        preprocessed_data = []
        for row in data.itertuples(index=False):
            preprocessed_row = (code,) + tuple(None if cell == '' else cell for cell in row[1:])
            preprocessed_data.append(preprocessed_row)

        # INSERT 쿼리 작성
        sql = "INSERT IGNORE INTO opt20006 (stock_code, trading_volume, date, open_price, high_price, low_price, trading_amount, sector, sub_sector, stock_info, previous_close_price) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        
        # 쿼리 실행
        if preprocessed_data:  # 데이터가 비어있지 않은 경우에만 실행
            cursor.executemany(sql, preprocessed_data)

        # 변경 사항 커밋
        db.commit()

        time.sleep(3.6)

# 역대 모든 opt20006 데이터 수집 및 저장
def insert_all_opt20006(cursor, db, kiwoom):
    now = datetime.datetime.now()
    today = now.strftime("%Y%m%d")

    opt = "opt20006"
    index = ["001", "101"]
    for i, code in enumerate(index):
        print(f"{i}/{len(index)} {code}")
        data = kiwoom.block_request(opt,
                              업종코드=code,
                              기준일자=today,
                              output="업종일봉차트조회",
                              next=0)
        
        # 데이터 전처리: 빈 값('')을 None으로 변환
        preprocessed_data = []
        for row in data.itertuples(index=False):
            preprocessed_row = (code,) + tuple(None if cell == '' else cell for cell in row[1:])
            preprocessed_data.append(preprocessed_row)

        print('데이터 수집 시작.. ({}~)'.format(data.loc[0,'일자']))
        print('데이터 수집 중.. (~{})'.format(data.loc[len(data)-1,'일자']))

        while kiwoom.tr_remained:
            data = kiwoom.block_request(opt,
                                업종코드=code,
                                기준일자=today,
                                output="업종일봉차트조회",
                                next=2)
            
            for row in data.itertuples(index=False):
                preprocessed_row = (code,) + tuple(None if cell == '' else cell for cell in row[1:])
                preprocessed_data.append(preprocessed_row)

            time.sleep(3.6)

            print('데이터 수집 중.. (~{})'.format(data.loc[len(data)-1,'일자']))

            if kiwoom.tr_remained == False:
                print('데이터 수집 완료: {}'.format(code))

        # INSERT 쿼리 작성
        sql = "INSERT IGNORE INTO opt20006 (stock_code, trading_volume, date, open_price, high_price, low_price, trading_amount, sector, sub_sector, stock_info, previous_close_price) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        
        # 쿼리 실행
        if preprocessed_data:  # 데이터가 비어있지 않은 경우에만 실행
            try:
                cursor.executemany(sql, preprocessed_data)
                db.commit()  # 변경 사항 커밋
            except Exception as e:
                print(f"SQL 실행 중 오류 발생: {e}")

        # 변경 사항 커밋
        db.commit()

        time.sleep(3.6)