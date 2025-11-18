import os
import threading
import time
from datetime import datetime
import requests
import csv
import io
import psycopg2
from flask import Flask

app = Flask(__name__)

DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
SHEET_ID = os.environ.get("SHEET_ID")
SHEET_GID = os.environ.get("SHEET_GID")  # ID листа test_2

CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SHEET_GID}"

def import_google_sheets_to_postgres():
    print("=== Запуск импорта данных из Google Sheets (лист test_2) ===")
    print(f"CSV URL: {CSV_URL}")
    try:
        response = requests.get(CSV_URL)
        response.raise_for_status()
        f = io.StringIO(response.content.decode('utf-8'))
        reader = csv.DictReader(f)
        print(f"CSV fieldnames: {reader.fieldnames}")
        records = list(reader)
        print(f"Loaded {len(records)} records from Google Sheets (лист test_2)")
        if len(records) > 0:
            print(f"First record sample: {records[0]}")

        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cur = conn.cursor()

        for i, row in enumerate(records):
            try:
                date_str = row.get('report_date')
                report_date = None
                if date_str:
                    fixed_date_str = date_str.replace('.', '-')
                    try:
                        report_date = datetime.strptime(fixed_date_str, '%Y-%m-%d').date()
                    except ValueError:
                        try:
                            report_date = datetime.strptime(fixed_date_str, '%Y-%d-%m').date()
                        except ValueError:
                            print(f"Error parsing date: '{date_str}'")

                k_str = row.get('k')
                coefficient = None
                if k_str:
                    coefficient = float(k_str.replace(',', '.'))

                last_updated = datetime.now()

                cur.execute("""
                    INSERT INTO model_coefficients (project_code, project_part, section, report_date, coefficient, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (project_code, report_date) DO UPDATE SET
                        project_part = EXCLUDED.project_part,
                        section = EXCLUDED.section,
                        coefficient = EXCLUDED.coefficient,
                        last_updated = EXCLUDED.last_updated;
                """, (
                    row.get('project_code'),
                    row.get('project_part'),
                    row.get('section'),
                    report_date,
                    coefficient,
                    last_updated
                ))
                print(f"Inserted row {i+1}: {row.get('project_code')}, {row.get('report_date')}")
            except Exception as err_row:
                print(f"Error inserting row {i+1}: {row} -> {err_row}")

        conn.commit()
        cur.close()
        conn.close()
        print(f"Data synchronized successfully at {datetime.now()}")

    except Exception as e:
        print(f"Error during import: {e}")

def background_job():
    while True:
        import_google_sheets_to_postgres()
        time.sleep(7 * 24 * 3600)  # Обновление раз в неделю

@app.route("/")
def index():
    return "Google Sheets to PostgreSQL sync service is running."

@app.route("/update-now", methods=["GET"])
def update_now():
    try:
        import_google_sheets_to_postgres()
        return "Данные успешно обновлены!", 200
    except Exception as e:
        return f"Ошибка обновления данных: {e}", 500

if __name__ == '__main__':
    print("=== Запуск Flask-сервиса и фонового потока синхронизации ===")
    import_google_sheets_to_postgres()  # первый запуск сразу
    thread = threading.Thread(target=background_job, daemon=True)
    thread.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
