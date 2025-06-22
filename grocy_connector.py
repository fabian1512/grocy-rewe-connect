# grocy_conector.py


import requests
# import csv
import unicodedata
import difflib
import os
import base64
import sqlite3
import re
from colorTerminal import OK, WARN, ERROR
from config import GROCY_API_URL, GROCY_API_KEY

GROCY_BASE_URL = GROCY_API_URL + "/api"
GROCY_HEADER = {
    "GROCY-API-KEY": GROCY_API_KEY,
    "accept": "application/json",
    "Content-Type": "application/json",
}
ENDPOINT_GET_BYBARCODE = GROCY_BASE_URL + "/stock/products/by-barcode/"
ENDPOINT_ADD_PRODUCT = GROCY_BASE_URL + "/objects/products"
ENDPOINT_ADD_BARCODE = GROCY_BASE_URL + "/objects/product_barcodes"
ENDPOINT_ADD_STOCK = GROCY_BASE_URL + "/stock/products/{product_id}/add"

DB_FILE = "rewe_products.db"

# Globale Datenbankverbindung herstellen
db_conn = sqlite3.connect(DB_FILE)
db_conn.row_factory = sqlite3.Row

def normalize_string(s):
    s = unicodedata.normalize('NFKD', s)
    s = s.replace("’", "'").replace("‘", "'")
    s = s.lower().strip()
    return s

def grocy_product_name_exists(product_name):
    url = GROCY_BASE_URL + "/objects/products"
    try:
        r = requests.get(url, headers=GROCY_HEADER, timeout=10, verify=False)
        r.raise_for_status()
        products = r.json()
        for product in products:
            if product.get("name", "").strip().lower() == product_name.strip().lower():
                return product.get("id")
        return None
    except Exception as e:
        print(f"{WARN} Fehler bei Grocy-Namensabfrage: {e}")
        return None

def create_product_in_grocy(product_data, ean):
    LOCATION_ID_KUEHLSCHRANK = 1
    SHOPPING_LOCATION_ID_REWE = 1

    product_name = product_data.get("product_name", "Unbenanntes Produkt")
    # Prüfe, ob Produktname schon existiert
    existing_id = grocy_product_name_exists(product_name)
    if existing_id:
        print(f"{WARN} Produktname '{product_name}' existiert bereits in Grocy (ID {existing_id}), lege nicht erneut an.")
        return existing_id

    product_info = {
        "name": product_name,
        "qu_id_stock": 2,
        "qu_id_purchase": 2,
        "qu_id_price": 2,
        "default_best_before_days": 30,
        "location_id": LOCATION_ID_KUEHLSCHRANK,
        "shopping_location_id": SHOPPING_LOCATION_ID_REWE,
        "min_stock_amount": 0,
    }
    try:
        r = requests.post(
            ENDPOINT_ADD_PRODUCT,
            headers=GROCY_HEADER,
            json=product_info,
            timeout=10,
            verify=False,
        )
        r.raise_for_status()
        product_id = r.json().get("created_object_id")
        print(f"{OK} Produkt '{product_info['name']}' in Grocy angelegt mit ID {product_id}.")

        ean_str = str(ean).strip()
        print(f"{OK} Verarbeite EAN: '{ean_str}'")

        image_url = get_image_url_by_ean(ean_str)
        if image_url:
            print(f"{OK} Bild-URL aus DB für EAN '{ean_str}': {image_url}")
        else:
            print(f"{WARN} Keine Bild-URL in DB für EAN '{ean_str}'")
        
        if image_url:
            image_filename = f"temp_product_image_{ean_str}.jpg"
            if download_image(image_url, image_filename):
                upload_product_image(product_id, image_filename)
                try:
                    os.remove(image_filename)
                except Exception:
                    pass

        return product_id
    except Exception as e:
        if hasattr(e, 'response') and e.response is not None:
            print(f"{ERROR} Grocy-Fehler: {e.response.text}")
        print(f"{WARN} Fehler beim Anlegen des Produkts in Grocy: {e}")
        return None

def get_ean_from_product_name(product_name):
    name_norm = normalize_string(product_name)
    cur = db_conn.execute("SELECT ean FROM products WHERE lower(trim(name)) = ?", (name_norm,))
    row = cur.fetchone()
    if row and row["ean"]:
        print(f"{OK} Direkter Namens-Treffer: '{product_name}' → EAN {row['ean']}")
        return row["ean"]
    print(f"{WARN} Kein direkter Namens-Treffer für '{product_name}'")
    return None

def get_ean_from_product_name_fuzzy(product_name, cutoff=0.8):
    name_norm = normalize_string(product_name)
    # Hole alle Namen aus der DB
    cur = db_conn.execute("SELECT name, ean FROM products")
    names = []
    name_to_ean = {}
    for row in cur.fetchall():
        n = normalize_string(row["name"])
        names.append(n)
        name_to_ean[n] = row["ean"]
    import difflib
    matches = difflib.get_close_matches(name_norm, names, n=1, cutoff=cutoff)
    if matches:
        match = matches[0]
        ean = name_to_ean.get(match)
        print(f"{OK} Fuzzy-Treffer: '{product_name}' ≈ '{match}' → EAN {ean}")
        return ean
    print(f"{WARN} Kein fuzzy Namens-Treffer für '{product_name}'")
    return None

def get_ean_from_rewe_code(rewe_code):
    cur = db_conn.execute("SELECT ean FROM products WHERE trim(ean) = ?", (str(rewe_code).strip(),))
    row = cur.fetchone()
    if row and row["ean"]:
        print(f"{OK} REWE-Code-Treffer: {rewe_code} → EAN {row['ean']}")
        return row["ean"]
    print(f"{WARN} Kein REWE-Code-Treffer für {rewe_code}")
    return None

def get_image_url_by_ean(ean):
    cur = db_conn.execute("SELECT image FROM products WHERE ean = ?", (str(ean).strip(),))
    row = cur.fetchone()
    if row and row["image"]:
        return row["image"]
    return None

def download_image(image_url, filename):
    try:
        response = requests.get(image_url, timeout=10, verify=False)
        response.raise_for_status()
        with open(filename, 'wb') as f:
            f.write(response.content)
        print(f"{OK} Bild heruntergeladen: {filename}")
        return True
    except Exception as e:
        print(f"{WARN} Fehler beim Herunterladen des Bildes: {e}")
        return False

def upload_product_image(product_id, image_path):
    file_name = f"{product_id}.jpg"
    # Base64-url-safe kodierter Dateiname ohne Padding "="
    file_name_b64 = base64.urlsafe_b64encode(file_name.encode()).decode().rstrip("=")

    upload_url = f"{GROCY_API_URL}/api/files/productpictures/{file_name_b64}"
    headers_upload = {
        "GROCY-API-KEY": GROCY_API_KEY,
        "Content-Type": "application/octet-stream"
    }

    try:
        with open(image_path, "rb") as image_file:
            resp = requests.put(upload_url, data=image_file, headers=headers_upload, timeout=10, verify=False)
        print(f"Upload Status: {resp.status_code} {resp.text}")
        if resp.status_code not in (200, 204):
            print(f"[WARN] Produktbild konnte nicht hochgeladen werden: {resp.status_code} {resp.text}")
            return False
    except Exception as e:
        print(f"[WARN] Fehler beim Upload: {e}")
        return False

    # Produkt mit Bilddateiname per PUT aktualisieren
    success = update_product_picture(product_id, file_name)
    if not success:
        print(f"[WARN] Produktbild konnte nicht im Produkt hinterlegt werden.")
        return False

    print(f"{OK} Produktbild für Produkt-ID {product_id} erfolgreich hochgeladen und zugewiesen.")
    return True

def update_product_picture(product_id, file_name):
    url = f"{GROCY_API_URL}/api/objects/products/{product_id}"
    headers = {
        "GROCY-API-KEY": GROCY_API_KEY,
        "Content-Type": "application/json"
    }
    data = {
        "picture_file_name": file_name
    }
    try:
        resp = requests.put(url, headers=headers, json=data, timeout=10, verify=False)
        if resp.status_code not in (200, 204):
            print(f"[WARN] Fehler beim Setzen des Bildnamens: {resp.status_code} {resp.text}")
            return False
        return True
    except Exception as e:
        print(f"[WARN] Fehler beim Setzen des Bildnamens: {e}")
        return False

def create_product_in_grocy(product_data, ean):
    LOCATION_ID_KUEHLSCHRANK = 1
    SHOPPING_LOCATION_ID_REWE = 1

    product_name = product_data.get("product_name", "Unbenanntes Produkt")
    # Prüfe, ob Produktname schon existiert
    existing_id = grocy_product_name_exists(product_name)
    if existing_id:
        print(f"{WARN} Produktname '{product_name}' existiert bereits in Grocy (ID {existing_id}), lege nicht erneut an.")
        return existing_id

    product_info = {
        "name": product_name,
        "qu_id_stock": 2,
        "qu_id_purchase": 2,
        "qu_id_price": 2,
        "default_best_before_days": 30,
        "location_id": LOCATION_ID_KUEHLSCHRANK,
        "shopping_location_id": SHOPPING_LOCATION_ID_REWE,
        "min_stock_amount": 0,
    }
    try:
        r = requests.post(
            ENDPOINT_ADD_PRODUCT,
            headers=GROCY_HEADER,
            json=product_info,
            timeout=10,
            verify=False,
        )
        r.raise_for_status()
        product_id = r.json().get("created_object_id")
        print(f"{OK} Produkt '{product_info['name']}' in Grocy angelegt mit ID {product_id}.")

        ean_str = str(ean).strip()
        print(f"{OK} Verarbeite EAN: '{ean_str}'")

        image_url = get_image_url_by_ean(ean_str)
        if image_url:
            print(f"{OK} Bild-URL aus DB für EAN '{ean_str}': {image_url}")
        else:
            print(f"{WARN} Keine Bild-URL in DB für EAN '{ean_str}'")
        
        if image_url:
            image_filename = f"temp_product_image_{ean_str}.jpg"
            if download_image(image_url, image_filename):
                upload_product_image(product_id, image_filename)
                try:
                    os.remove(image_filename)
                except Exception:
                    pass

        return product_id
    except Exception as e:
        if hasattr(e, 'response') and e.response is not None:
            print(f"{ERROR} Grocy-Fehler: {e.response.text}")
        print(f"{WARN} Fehler beim Anlegen des Produkts in Grocy: {e}")
        return None

def add_barcode_to_product(product_id, ean):
    barcode_info = {
        "barcode": str(ean),
        "product_id": product_id,
        "amount": 1,
    }
    try:
        r = requests.post(
            ENDPOINT_ADD_BARCODE,
            headers=GROCY_HEADER,
            json=barcode_info,
            timeout=10,
            verify=False,
        )
        r.raise_for_status()
        print(f"{OK} Barcode {ean} zum Produkt {product_id} hinzugefügt.")
        return True
    except Exception as e:
        print(f"{WARN} Fehler beim Hinzufügen des Barcodes: {e}")
        return False

def update_stock(product_id, amount, price):
    url = ENDPOINT_ADD_STOCK.format(product_id=product_id)
    stock_info = {
        "amount": amount,
        "transaction_type": "purchase",
        "price": price,
    }
    try:
        r = requests.post(
            url,
            headers=GROCY_HEADER,
            json=stock_info,
            timeout=10,
            verify=False,
        )
        r.raise_for_status()
        print(f"{OK} Bestand für Produkt-ID {product_id} aktualisiert.")
        return True
    except Exception as e:
        print(f"{WARN} Fehler beim Aktualisieren des Bestands: {e}")
        return False

def grocy_product_exists(ean):
    url = ENDPOINT_GET_BYBARCODE + str(ean)
    try:
        r = requests.get(url, headers=GROCY_HEADER, timeout=10, verify=False)
        if r.status_code == 200:
            data = r.json()
            return "product" in data
        return False
    except Exception as e:
        print(f"{WARN} Fehler bei Grocy-Abfrage: {e}")
        return False

def get_grocy_product_id_by_ean(ean):
    url = ENDPOINT_GET_BYBARCODE + str(ean)
    try:
        r = requests.get(url, headers=GROCY_HEADER, timeout=10, verify=False)
        if r.status_code == 200:
            data = r.json()
            product = data.get("product")
            if product and "id" in product:
                return product["id"]
        return None
    except Exception as e:
        print(f"{WARN} Fehler beim Abrufen der Produkt-ID von Grocy: {e}")
        return None

def remove_quantity_from_name(name):
    """
    Entfernt typische Mengen-/Gewichtsangaben am Ende des Produktnamens.
    Beispiele: "191g", "1kg", "1 Stück", "ca. 200g", "0,25l", "8x100g", "1Stück", "1,5kg"
    """
    patterns = [
        r"\s*\d+[.,]?\d*\s*([xX]\s*\d+)?\s*(Stück|Stk\.?|kg|g|l|ml|cl|dl|mg|µg|Packung|Becher|Dose|Flasche|Tüte|Bund|Pck|Pckg|Paket)\.?$",
        r"\s*ca\.\s*\d+[.,]?\d*\s*(g|kg|l|ml|Stück)\.?$",
        r"\s*\d+[.,]?\d*\s*(g|kg|l|ml|Stück)\.?$",
        r"\s*\d+\s*x\s*\d+\s*(g|ml|Stück)\.?$",
        r"\s*\d+[.,]?\d*\s*(%|vol|Vol)\.?$",
    ]
    new_name = name
    for pattern in patterns:
        new_name = re.sub(pattern, '', new_name, flags=re.IGNORECASE)
    return new_name.strip()

def add_or_update_product(ean, amount, price, bon_product_name=None):
    # 1. EAN aus DB anhand des Bon-Namens bestimmen (direkt oder fuzzy)
    if bon_product_name:
        ean_db = get_ean_from_product_name(bon_product_name)
        if not ean_db:
            ean_db = get_ean_from_product_name_fuzzy(bon_product_name)
        if ean_db:
            ean = ean_db  # Überschreibe EAN mit der aus der DB gefundenen EAN

    # 2. Suche in Grocy nach der EAN
    if grocy_product_exists(ean):
        product_id = get_grocy_product_id_by_ean(ean)
        if product_id:
            return update_stock(product_id, amount, price)
        else:
            print(f"{WARN} Produkt-ID für EAN {ean} konnte nicht gefunden werden.")
            return False
    else:
        product_data = fetch_product_from_off(ean) or {}
        # Immer den Namen aus dem Bon verwenden, falls vorhanden!
        if bon_product_name:
            clean_name = remove_quantity_from_name(bon_product_name)
            product_data["product_name"] = clean_name
        else:
            product_data["product_name"] = remove_quantity_from_name(product_data.get("product_name") or str(ean))
        product_id = create_product_in_grocy(product_data, ean)
        if not product_id:
            return False
        if not add_barcode_to_product(product_id, ean):
            return False
        return update_stock(product_id, amount, price)

def fetch_product_from_off(ean):
    """Hole Produktdaten von Open Food Facts anhand der EAN."""
    url = f"https://world.openfoodfacts.org/api/v0/product/{ean}.json"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("status") == 1:
            return data.get("product", {})
        else:
            print(f"{WARN} Kein Produkt bei Open Food Facts für EAN {ean}")
            return None
    except Exception as e:
        print(f"{WARN} Fehler beim Abrufen von Open Food Facts: {e}")
        return None


