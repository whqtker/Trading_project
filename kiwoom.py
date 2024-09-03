from pykiwoom.kiwoom import Kiwoom
from config import acc_config
import mysql.connector

# 키움증권 API에 로그인
def login():
    kiwoom = Kiwoom()
    kiwoom.CommConnect()
    return kiwoom

# 상장된 모든 종목 코드 얻기
def get_all_codes(kiwoom):
    kospi = kiwoom.GetCodeListByMarket('0')
    kosdaq = kiwoom.GetCodeListByMarket('10')
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
        return None

# 매수 종목 코드 얻기
def get_buy_codes(cursor):
    try:
        query = """
        SELECT stock_code
        FROM stock_signal
        WHERE filtering = 1
        ORDER BY predicted_probability DESC
        LIMIT 5;
        """
        cursor.execute(query)
        buy_codes = cursor.fetchall()
        
        # Extract stock codes from the query result
        buy_codes = [code[0] for code in buy_codes]
        
        return buy_codes

    except mysql.connector.Error as err:
        print(f"Database connection error: {err}")
        return None

# 매도 종목 코드, 매도량 얻기
def get_sell_codes(cursor, kiwoom):
    bought_name = get_bought_codes(kiwoom)['종목명'].tolist()
    sell_amount = get_bought_codes(kiwoom)['보유수량'].tolist()
    bought_codes = get_codes_from_name(cursor, bought_name)

    if (len(bought_codes) >= 25):
        # 매수한 종목들에 대하여 확률 가장 낮은 값 매도하기
        # 단, 남은 종목들의 수는 25개여야 함
        try:
            sell_cnt = len(bought_codes) - 25
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
            
            # Extract stock codes from the query result
            sell_codes = [code[0] for code in sell_codes]
            
            return sell_codes, sell_amount

        except mysql.connector.Error as err:
            print(f"Database connection error: {err}")
            return None
    else:
        # 매수한 종목들에 대하여 재무재표와 확률값 구하기
        # 매수 조건에 맞지 않으면 매도
        try:
            # Convert bought_codes list to a string format suitable for SQL IN clause
            bought_codes_str = ','.join(f"'{code}'" for code in bought_codes)
            
            query = f"""
            SELECT stock_code
            FROM stock_signal
            WHERE stock_code IN ({bought_codes_str})
            AND (filtering = 0 OR predicted_probability < 0.5);
            """
            cursor.execute(query)
            sell_codes = cursor.fetchall()
            
            # Extract stock codes from the query result
            sell_codes = [code[0] for code in sell_codes]
            
            return sell_codes, sell_amount

        except mysql.connector.Error as err:
            print(f"Database connection error: {err}")
            return None, None

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