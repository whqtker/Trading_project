from database import connect_to_db, create_db_engine
from kiwoom import login, get_all_codes, user_info
from data_collection import insert_opt10081, insert_opt10001, insert_opt20006, insert_all_opt20006
from data_processing import filtering, data_processing
from trading import buy, sell
from subprocess_wrapper import sub_get_model_output

# 실제 매매 프로그램
def run():
    insert_opt10081()
    insert_opt10001()
    filtering()
    data_processing()

    sub_get_model_output()

    buy(cursor, kiwoom)
    sell(cursor, kiwoom)

if __name__ == "__main__":
    db, cursor = connect_to_db()
    engine = create_db_engine()

    kiwoom = login()

    codes = get_all_codes(kiwoom)

    #run()

    db.close()
    engine.dispose()