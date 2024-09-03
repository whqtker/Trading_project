import subprocess
import os
from telegram_bot import send_message

async def sub_get_model_output():
    await send_message("DB에 모델 출력 삽입 시작.")
    python_3_12_path = r"C:\Users\chosh\AppData\Local\Programs\Python\Python312\python.exe"  # Windows의 경우
    script_to_run = os.path.join(os.path.dirname(__file__), "get_model_output.py")  # 스크립트 경로 설정

    try:
        result = subprocess.run([python_3_12_path, script_to_run], check=True, capture_output=True, text=True)
        print("Output:", result.stdout)
        print("Errors:", result.stderr)
    except subprocess.CalledProcessError as e:
        print("Error occurred:", e)
    except FileNotFoundError as e:
        print("File not found:", e)
    except Exception as e:
        print("An unexpected error occurred:", e)

    await send_message("DB에 모델 출력 삽입 완료.")