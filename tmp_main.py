from pykiwoom.kiwoom import *
import os
import shutil
import requests
from config import db_config, acc_config
import csv
import joblib
import numpy as np




# 실제 프로그램 동작 함수
def run():
    insert_opt10081()
    insert_opt10001()
    filtering()
    data_processing()

    sub_get_model_output()

    buy()
    sell()

if __name__ == "__main__":
    run()

    db.close()
    engine.dispose()