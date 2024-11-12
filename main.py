from database import connect_and_create_engine
from kiwoom import login, get_all_codes, user_info
from data_collection import insert_opt10081, insert_opt10001, insert_opt20006, insert_all_opt20006
from data_processing import filtering, data_processing
from trading import buy, sell
from subprocess_wrapper import sub_get_model_output
from telegram_bot import send_message
import asyncio

async def Run(func, *args):
    db, cursor, engine = connect_and_create_engine()
    try:
        await func(cursor, db, engine, *args)
    finally:
        cursor.close()
        db.close()
        engine.dispose()

# 실제 매매 프로그램
async def run(kiwoom, codes):
    await send_message("프로그램 시작")

    #await Run(insert_opt10081, kiwoom, codes)
    #await Run(insert_opt10001, kiwoom, codes)
    await Run(filtering)
    #await Run(data_processing)

    await sub_get_model_output()

    # await Run(buy, kiwoom)
    # await Run(sell, kiwoom, 5)

    await send_message("프로그램 종료")

if __name__ == "__main__":
    kiwoom = login()

    codes = get_all_codes(kiwoom)

    # 기존 이벤트 루프를 가져와서 run 함수를 실행
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(kiwoom, codes))
