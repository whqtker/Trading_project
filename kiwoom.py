from pykiwoom.kiwoom import Kiwoom
from config import acc_config
import mysql.connector
import datetime
from database import connect_and_create_engine
from sqlalchemy import text

# 키움증권 API에 로그인
def login():
    kiwoom = Kiwoom()
    kiwoom.CommConnect()
    return kiwoom

# 상장된 모든 종목 코드 얻기 & 가장 최근의 장마감 날짜와 이전 600일 치 정보 DB에 삽입
def get_all_codes(kiwoom):
    kospi = kiwoom.GetCodeListByMarket('0')
    kosdaq = kiwoom.GetCodeListByMarket('10')

    codes = kospi + kosdaq

    # 현재 날짜
    now = datetime.datetime.now()
    today = now.strftime("%Y%m%d")

    opt = "opt10081"
    data = kiwoom.block_request(opt,
                              종목코드=kospi[0],
                              기준일자=today,
                              수정주가구분=1,
                              output="주식일봉차트조회",
                              next=0)
    
    dates = data['일자'].tolist()

    # 데이터베이스 연결 설정
    db, cursor, engine = connect_and_create_engine()

    try:
        # stock 테이블에 종목 코드와 날짜 저장
        for code in codes:
            # 이미 존재하는 (stock_code, date) 쌍 조회
            existing_dates_query = text("""
            SELECT date FROM stock WHERE stock_code = :stock_code
            """)
            with engine.connect() as conn:
                result = conn.execute(existing_dates_query, {'stock_code': code})
                existing_dates = {row['date'] for row in result}

            # 중복되지 않는 데이터 필터링
            new_dates = [date for date in dates if date not in existing_dates]

            # 새로운 데이터만 삽입
            if new_dates:
                insert_query = text("""
                INSERT IGNORE INTO stock (stock_code, date)
                VALUES (:stock_code, :date)
                """)
                with engine.connect() as conn:
                    conn.execute(insert_query, [{'stock_code': code, 'date': date} for date in new_dates])

    finally:
        cursor.close()
        db.close()
        engine.dispose()

    return kospi + kosdaq

# 매수한 종목 정보 얻기(종목 이름, 매수량 등)
def get_bought_codes(kiwoom):
    opt = "opw00004"

    data = kiwoom.block_request(opt,
                            계좌번호=acc_config['account'],
                            비밀번호="",
                            상장폐지조회구분=0,
                            비밀번호입력매체구분=00,
                            output="종목별계좌평가현황",
                            next=0)
        
    return data

# 종목 이름으로 종목 코드 얻기
def get_codes_from_name(cursor, stock_names):
    try:
        if not stock_names:
            return []

        # Convert stock_names list to a string format suitable for SQL IN clause
        stock_names_str = ','.join(f"'{name}'" for name in stock_names)
        
        query = f"""
        SELECT stock_code
        FROM opt10001
        WHERE stock_name IN ({stock_names_str});
        """
        cursor.execute(query)
        codes = cursor.fetchall()
        
        # Extract stock codes from the query result
        stock_codes = [code[0] for code in codes]
        
        return stock_codes

    except mysql.connector.Error as err:
        print(f"Database connection error: {err}")
        return []

# 매수 종목 코드 얻기
def get_buy_codes(cursor):
    # 필터링 결과가 1이고 모델 확률 예측 값이 높은 종목 n개
    try:
        # 모델 예측 확률 상위 n개 종목 결정
        n = 5
        query = f"""
        SELECT stock_code
        FROM stock_signal
        WHERE filtering = 1
        ORDER BY predicted_probability DESC
        LIMIT {n};
        """
        cursor.execute(query)
        buy_codes = cursor.fetchall()
        
        # 매수할 종목 코드 추출
        buy_codes = [code[0] for code in buy_codes]
        
        return buy_codes

    except mysql.connector.Error as err:
        print(f"Database connection error: {err}")
        return []
    
# 매도 종목 코드, 매도량 얻기
def get_sell_codes(cursor, kiwoom, limit_cnt):
    bought_name = get_bought_codes(kiwoom)['종목명'].tolist() # 종목 이름
    sell_amount = get_bought_codes(kiwoom)['보유수량'].tolist() # 보유량(매도량)
    bought_codes = get_codes_from_name(cursor, bought_name)

    if not bought_codes:
        return [], []

    if len(bought_codes) >= limit_cnt:
        # 매수한 종목들에 대하여 확률 가장 낮은 값 매도하기
        # 단, 남은 종목들의 수는 limit_cnt개여야 함
        try:
            sell_cnt = len(bought_codes) - limit_cnt
            bought_codes_str = ','.join(f"'{code}'" for code in bought_codes)
            
            query = f"""
            SELECT stock_code
            FROM stock_signal
            WHERE stock_code IN ({bought_codes_str})
            ORDER BY predicted_probability ASC
            LIMIT {sell_cnt};
            """
            cursor.execute(query)
            sell_codes = cursor.fetchall()
            
            sell_codes = [code[0] for code in sell_codes]
            
            return sell_codes, sell_amount

        except mysql.connector.Error as err:
            print(f"Database connection error: {err}")
            return [], []
    else:
        # 매수한 종목들에 대하여 재무재표와 확률값 구하기
        # 매수 조건에 맞지 않으면 매도
        try:
            # 보유 종목
            bought_codes_str = ','.join(f"'{code}'" for code in bought_codes)
            
            query = f"""
            SELECT stock_code
            FROM stock_signal
            WHERE stock_code IN ({bought_codes_str})
            AND (filtering = 0 OR predicted_probability < 0.5);
            """
            cursor.execute(query)
            sell_codes = cursor.fetchall()
            
            # 매도 종목 코드 추출
            sell_codes = [code[0] for code in sell_codes]
            
            return sell_codes, sell_amount

        except mysql.connector.Error as err:
            print(f"Database connection error: {err}")
            return [], []

# 사용자 정보 구하기
def user_info(kiwoom):
    account_num = kiwoom.GetLoginInfo("ACCOUNT_CNT")        # 전체 계좌수
    accounts = kiwoom.GetLoginInfo("ACCNO")                 # 전체 계좌 리스트
    user_id = kiwoom.GetLoginInfo("USER_ID")                # 사용자 ID
    user_name = kiwoom.GetLoginInfo("USER_NAME")            # 사용자명
    keyboard = kiwoom.GetLoginInfo("KEY_BSECGB")            # 키보드보안 해지여부
    firewall = kiwoom.GetLoginInfo("FIREW_SECGB")           # 방화벽 설정 여부

    print(account_num)
    print(accounts)
    print(user_id)
    print(user_name)
    print(keyboard)
    print(firewall)