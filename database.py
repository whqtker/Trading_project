import mysql.connector
from sqlalchemy import create_engine as sqlalchemy_create_engine
from config import db_config
import pandas as pd

# 데이터베이스 연결, dn, cursor 리턴
def connect_to_db():
    db = mysql.connector.connect(
        user=db_config['user'],
        password=db_config['password'],
        host=db_config['host'],
        port=db_config['port'],
        database=db_config['database']
    )
    cursor = db.cursor()
    return db, cursor

# 데이터베이스 엔진 생성
def create_db_engine():
    engine = sqlalchemy_create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")
    return engine

# 데이터베이스 연결 및 엔진 생성(올인원)
def connect_and_create_engine():
    db, cursor = connect_to_db()
    engine = create_db_engine()
    return db, cursor, engine

# opt10081 테이블의 최신 날짜 조회
def get_latest_dates(cursor):
    try:
        cursor.execute("SELECT MAX(date) FROM stock")
        latest_date = cursor.fetchone()[0]

        # Return the result
        return latest_date

    except mysql.connector.Error as err:
        print(f"Database connection error: {err}")
        return None

# 특정 테이블의 데이터를 hdf5 파일로 내보내기
def export_table_to_hdf5(cursor, db, table_name):
    # Define the chunk size
    chunk_size = 1000  # Adjust the chunk size based on your memory capacity

    # SQL query to select all data from the table
    query = f"SELECT * FROM {table_name}"
    
    # Get the file name from the user
    file_name = input("저장할 HDF5 파일명을 입력하세요 (확장자 제외): ")
    file_name_with_extension = f"{file_name}.h5"

    # Calculate the total number of rows in the table
    total_rows_query = f"SELECT COUNT(*) FROM {table_name}"
    cursor.execute(total_rows_query)
    total_rows = cursor.fetchone()[0]

    # Open the HDF5 file for writing
    with pd.HDFStore(file_name_with_extension, mode='w') as store:
        chunk_number = 0
        processed_rows = 0
        for chunk in pd.read_sql(query, con=db, chunksize=chunk_size):
            # Convert the 'date' column to string format
            if 'date' in chunk.columns:
                chunk['date'] = chunk['date'].astype(str)
            store.append(table_name, chunk, format='table', data_columns=True, min_itemsize={'adjusted_price_flag': 3})
            chunk_number += 1
            processed_rows += len(chunk)
            progress_percentage = (processed_rows / total_rows) * 100
            print(f"Chunk {chunk_number} processed. Progress: {progress_percentage:.2f}%")

    print(f"{file_name_with_extension} 파일로 저장되었습니다.")