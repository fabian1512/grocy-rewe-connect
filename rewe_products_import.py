import os
import sqlite3
import csv
import requests
from datetime import datetime, timedelta

DB_FILE = "rewe_products.db"
TABLE_NAME = "products"
BASE_URL = "https://rewe.nicoo.org/"
BUNDESLAND = "schleswig-holstein"
START_DATE = datetime(2025, 6, 15)
END_DATE = datetime.today()

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    brand TEXT,
    ean TEXT,
    price REAL,
    grammage TEXT,
    category TEXT,
    sale TEXT,
    image TEXT,
    date TEXT
);
"""

def download_csv(date):
    date_str = date.strftime("%Y-%m-%d")
    filename = f"{date_str}_{BUNDESLAND}.csv"
    url = f"{BASE_URL}{filename}"
    print(f"Versuche Download: {url}")
    r = requests.get(url)
    if r.status_code == 200 and r.content:
        with open(filename, "wb") as f:
            f.write(r.content)
        print(f"Heruntergeladen: {filename}")
        return filename
    print(f"Keine Datei für {date_str} gefunden.")
    return None

def import_csv_to_db(csv_file, conn, date_str):
    with open(csv_file, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows_new = []
        rows_update = []
        for row in reader:
            price_raw = row.get("price")
            if price_raw is None or price_raw.strip().upper() == "NA" or price_raw.strip() == "":
                price = 0
            else:
                try:
                    price = float(price_raw.replace(",", "."))
                except Exception:
                    price = 0
            ean = row.get("ean")
            if ean:
                cur = conn.execute(f"SELECT id FROM {TABLE_NAME} WHERE ean = ?", (ean,))
                result = cur.fetchone()
                if result:
                    rows_update.append((price, date_str, ean))
                else:
                    rows_new.append((
                        row.get("name"),
                        row.get("brand"),
                        ean,
                        price,
                        row.get("grammage"),
                        row.get("category"),
                        row.get("sale"),
                        row.get("image"),
                        date_str
                    ))
        if rows_new:
            conn.executemany(
                f"""INSERT INTO {TABLE_NAME}
                (name, brand, ean, price, grammage, category, sale, image, date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows_new
            )
            print(f"{len(rows_new)} neue Zeilen aus {csv_file} importiert.")
        if rows_update:
            conn.executemany(
                f"""UPDATE {TABLE_NAME} SET price = ?, date = ? WHERE ean = ?""",
                rows_update
            )
            print(f"{len(rows_update)} Zeilen in {csv_file} aktualisiert.")
    try:
        os.remove(csv_file)
        print(f"{csv_file} gelöscht.")
    except Exception as e:
        print(f"Fehler beim Löschen von {csv_file}: {e}")

def get_latest_date_from_db(conn):
    cur = conn.execute(f"SELECT MAX(date) FROM {TABLE_NAME}")
    result = cur.fetchone()
    if result and result[0]:
        try:
            return datetime.strptime(result[0], "%Y-%m-%d")
        except Exception:
            pass
    return None

def main():
    conn = sqlite3.connect(DB_FILE)
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()

    latest_date = get_latest_date_from_db(conn)
    if latest_date:
        date = latest_date + timedelta(days=1)
    else:
        date = datetime(2025, 6, 15)  # Fallback-Startdatum

    END_DATE = datetime.today()
    days_without_file = 0
    max_days_without_file = 10

    while date <= END_DATE:
        date_str = date.strftime("%Y-%m-%d")
        filename = f"{date_str}_{BUNDESLAND}.csv"
        if not os.path.exists(filename):
            downloaded = download_csv(date)
            if not downloaded:
                days_without_file += 1
                if days_without_file >= max_days_without_file:
                    print(f"{max_days_without_file} Tage in Folge keine Datei gefunden, Abbruch.")
                    break
                date += timedelta(days=1)
                continue
        else:
            print(f"{filename} bereits vorhanden, überspringe Download.")

        import_csv_to_db(filename, conn, date_str)
        conn.commit()
        date += timedelta(days=1)
        days_without_file = 0  # Reset, wenn eine Datei gefunden wurde

    conn.close()
    print("Import abgeschlossen.")

if __name__ == "__main__":
    main()