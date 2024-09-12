from config import acc_config
from kiwoom import get_buy_codes, get_sell_codes
from telegram_bot import send_message

# 실제 매수 함수
async def buy(cursor, kiwoom):
    buy_codes = get_buy_codes(cursor)
    if not buy_codes:
        await send_message("매수할 종목이 없습니다.")
        return

    await send_message("매수 종목 리스트: " + ", ".join(buy_codes))
    for code in buy_codes:
        kiwoom.SendOrder("시장가매수", "0101", acc_config['account'], 1, code, 1, 0, "03", "")
        await send_message(f"{code} 매수 주문 완료")

# 실제 매도 함수
# 최대 limit_cnt만큼 종목 보유
async def sell(cursor, kiwoom, limit_cnt):
    sell_codes, amount = get_sell_codes(cursor, kiwoom, limit_cnt)
    if not sell_codes:
        await send_message("매도할 종목이 없습니다.")
        return

    await send_message("매도 종목 리스트: " + ", ".join(sell_codes))
    for code in sell_codes:
        kiwoom.SendOrder("시장가매도", "0101", acc_config['account'], 2, code, amount, 0, "03", "")
        await send_message(f"{code} 매도 주문 완료")