from config import acc_config
from kiwoom import get_buy_codes, get_sell_codes

# 실제 매수 함수
def buy(cursor, kiwoom):
    buy_codes = get_buy_codes(cursor)
    for code in buy_codes:
        kiwoom.SendOrder("시장가매수", "0101", acc_config['account'], 1, code, 10, 0, "03", "")
        print(f"{code} 매수 주문 완료")

# 실제 매도 함수
def sell(cursor, kiwoom):
    sell_codes, amount = get_sell_codes(cursor, kiwoom)
    for code in sell_codes:
        kiwoom.SendOrder("시장가매도", "0101", acc_config['account'], 2, code, amount, 0, "03", "")
        print(f"{code} 매도 주문 완료")