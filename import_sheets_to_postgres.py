import gspread
import psycopg2
from datetime import datetime
import schedule
import time

# Конфигурация подключения к БД (через переменные окружения на Render лучше)
DB_HOST = "92.255.78.188"
DB_NAME = "default_db"
DB_USER = "gen_user"
DB_PASSWORD = "A-gcss6#w$FsWW"

# Ссылка на публичную Google Sheets
SHEET_URL = "https://docs.google.com/spreadsheets/d/1ZJzMWpMiN55e13Xad2wrrwX7I1zihOKLyELNCGLm37Q/edit?usp=sharing"

def import_google_sheets_to_postgres():
    try:
        # Подключение к Google Sheets
        gc = gspread.public()
        worksheet = gc.open_by_url(SHEET_URL).sheet1
        records = worksheet.get_all_records()

        # Подключение к PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()

        for row in records:
            # Преобразование даты
            report_date = datetime.strptime(row['report_date'], '%Y-%m-%d').date() if row['report_date'] else None
            last_updated = datetime.now()

            cur.execute("""
                INSERT INTO model_coefficients (id, project_code, project_part, section, report_date, coefficient, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                  project_code = EXCLUDED.project_code,
                  project_part = EXCLUDED.project_part,
                  section = EXCLUDED.section,
                  report_date = EXCLUDED.report_date,
                  coefficient = EXCLUDED.coefficient,
                  last_updated = EXCLUDED.last_updated;
            """, (
                row['id'],
                row['project_code'],
                row.get('project_part'),
                row.get('section'),
                report_date,
                row['coefficient'],
                last_updated
            ))

        conn.commit()
        cur.close()
        conn.close()
        print(f"Data synchronized successfully at {datetime.now()}")

    except Exception as e:
        print(f"Error during import: {e}")

# Фоновое обновление каждые 1 час
schedule.every(1).hours.do(import_google_sheets_to_postgres)

if __name__ == "__main__":
    print("Starting background Google Sheets to PostgreSQL sync...")
    import_google_sheets_to_postgres()  # Первый запуск сразу
    while True:
        schedule.run_pending()
        time.sleep(60)
