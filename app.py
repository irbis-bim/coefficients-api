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

CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv"

def import_google_sheets_to_postgres():
    try:
        response = requests.get(CSV_URL)
        response.raise_for_status()
        f = io.StringIO(response.text)
        reader = csv.DictReader(f)
        records = list(reader)

        print(f"Loaded {len(records)} records from Google Sheets")
        if len(records) > 0:
            print(f"First record sample: {records[0]}")

        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cur = conn.cursor()

        for row in records:
            report_date = datetime.strptime(row['report_date'], '%Y-%m-%d').date() if row.get('report_date') else None
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
                row['project_code'],
                row.get('project_part'),
                row.get('section'),
                report_date,
                float(row['coefficient']),
                last_updated
            ))

        conn.commit()
        cur.close()
        conn.close()
        print(f"Data synchronized successfully at {datetime.now()}")

    except Exception as e:
        print(f"Error during import: {e}")

def background_job():
    while True:
        import_google_sheets_to_postgres()
        time.sleep(3600)

@app.route("/")
def index():
    return "Google Sheets to PostgreSQL sync service is running."

if __name__ == '__main__':
    thread = threading.Thread(target=background_job, daemon=True)
    thread.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
