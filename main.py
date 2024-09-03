from database import connect_to_db, create_db_engine
from kiwoom import login, get_all_codes, user_info
from data_collection import insert_opt10081, insert_opt10001, insert_opt20006, insert_all_opt20006
from data_processing import filtering, data_processing
from trading import buy, sell
from subprocess_wrapper import sub_get_model_output
from telegram_bot import send_message
import asyncio

# 실제 매매 프로그램
async def run(cursor, db, engine, kiwoom, codes):
    await send_message("프로그램 시작")

    await insert_opt10081(cursor, db, kiwoom, codes)
    await insert_opt10001(cursor, db, kiwoom, codes)

    await filtering(cursor, db)
    await data_processing(cursor, db, engine)
    await sub_get_model_output()

    await buy(cursor, kiwoom)
    await sell(cursor, kiwoom)

    await send_message("프로그램 종료")

if __name__ == "__main__":
    db, cursor = connect_to_db()
    engine = create_db_engine()

    kiwoom = login()

    codes = get_all_codes(kiwoom)

    # 기존 이벤트 루프를 가져와서 run 함수를 실행
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(cursor, db, engine, kiwoom, codes))

    db.close()
    engine.dispose()