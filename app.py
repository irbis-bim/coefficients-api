import os
from datetime import datetime
import requests
import csv
import io
import psycopg2
from flask import Flask

app = Flask(__name__)

# ENV
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
SHEET_ID = os.environ.get("SHEET_ID")
SHEET_GID = os.environ.get("SHEET_GID")  # gid листа (например, test_2)

CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SHEET_GID}"

def normalize_date_to_varchar(date_raw: str) -> str | None:
    """
    Приводит строку даты к формату YYYY.MM.DD для сохранения в varchar.
    Поддерживает разделители '.' и '-'; добавляет ведущие нули для месяца/дня.
    """
    if not date_raw:
        return None
    s = date_raw.strip().replace('-', '.')
    parts = s.split('.')
    if len(parts) == 3 and all(parts):
        y = parts[0]
        m = parts[1].zfill(2)
        d = parts[2].zfill(2)
        return f"{y}.{m}.{d}"
    return s or None

def parse_coefficient(k_raw: str) -> float | None:
    if not k_raw:
        return None
    txt = k_raw.strip().replace(',', '.')
    return float(txt) if txt else None

def import_google_sheets_to_postgres() -> bool:
    print("=== Импорт из Google Sheets (ручной запуск) ===")
    print(f"CSV URL: {CSV_URL}")
    try:
        # 1) загрузка CSV
        resp = requests.get(CSV_URL, timeout=60)
        resp.raise_for_status()
        f = io.StringIO(resp.content.decode('utf-8'))
        reader = csv.DictReader(f)
        print(f"CSV fieldnames: {reader.fieldnames}")
        rows = list(reader)
        print(f"Loaded {len(rows)} records")

        # 2) подключение к БД
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cur = conn.cursor()

        # 3) вставка/обновление
        for i, row in enumerate(rows, start=1):
            try:
                report_date_str = normalize_date_to_varchar(row.get('report_date'))
                coefficient = parse_coefficient(row.get('k'))  # столбец 'k' в листе
                last_updated = datetime.now()

                cur.execute("""
                    INSERT INTO model_coefficients
                        (project_code, project_part, section, report_date, coefficient, last_updated)
                    VALUES
                        (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (project_code, report_date) DO UPDATE SET
                        project_part = EXCLUDED.project_part,
                        section = EXCLUDED.section,
                        coefficient = EXCLUDED.coefficient,
                        last_updated = EXCLUDED.last_updated;
                """, (
                    row.get('project_code'),
                    row.get('project_part'),
                    row.get('section'),
                    report_date_str,   # varchar вида YYYY.MM.DD
                    coefficient,
                    last_updated
                ))
            except Exception as err_row:
                print(f"[error] row {i} failed: {row} -> {err_row}")

        conn.commit()
        cur.close()
        conn.close()
        print(f"Done at {datetime.now()}")
        return True

    except Exception as e:
        print(f"[fatal] import failed: {e}")
        return False

@app.route("/")
def index():
    return "Service is up. Use /update-now to refresh."

@app.route("/update-now", methods=["GET"])
def update_now():
    ok = import_google_sheets_to_postgres()
    return ("Данные успешно обновлены!", 200) if ok else ("Ошибка обновления, см. логи", 500)

if __name__ == '__main__':
    print("=== Запуск Flask-сервиса (только ручное обновление) ===")
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
