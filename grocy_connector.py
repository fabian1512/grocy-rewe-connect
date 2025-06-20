# grocy_conector.py


import requests
import csv
import unicodedata
import difflib
import os
import base64
from colorTerminal import OK, WARN, ERROR
from config import GROCY_API_URL, GROCY_API_KEY

IGNORE_FILE = "ignore.txt"
REWE_CSV_PATH = "rewe.csv"

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

rewe_code_to_ean = {}
product_name_to_ean = {}
all_product_names = []
rewe_code_to_image_url = {}
product_name_to_image_url = {}

def normalize_string(s):
    s = unicodedata.normalize('NFKD', s)
    s = s.replace("’", "'").replace("‘", "'")
    s = s.lower().strip()
    return s

# NEUE GLOBALE VARIABLE
ean_to_image_url = {}

def load_rewe_price_data():
    global rewe_code_to_ean, product_name_to_ean, all_product_names, ean_to_image_url  # Update
    
    # ... (bestehender Code bis Zeile 48) ...
    
    try:
        with open(REWE_CSV_PATH, encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            print(f"{OK} CSV-Spalten: {reader.fieldnames}")
            for row in reader:
                # ... (bestehender Code für article_number und name) ...
                
                # NEU: Direkte EAN-zu-Bild-URL Mapping
                ean_value = ean.strip() if ean else None
                if ean_value and image_url and image_url.strip():
                    ean_to_image_url[ean_value] = image_url.strip()
                    print(f"{OK} Bild-URL für EAN {ean_value} gespeichert")
                
                # ... (bestehender Code für rewe_code_to_ean und product_name_to_ean) ...
                
        print(f"{OK} EAN-zu-Bild-Mapping geladen: {len(ean_to_image_url)} Einträge")
        
    # ... (bestehender Rest der Funktion) ...

def create_product_in_grocy(product_data, ean):
    # ... (bestehender Code) ...
    
    ean_str = str(ean).strip()
    print(f"{OK} Verarbeite EAN: '{ean_str}'")
    
    # NEUE LOGIK: Priorität für CSV-Bilder
    image_url = None
    
    # 1. Versuch: Bild aus CSV via EAN
    if ean_str in ean_to_image_url:
        image_url = ean_to_image_url[ean_str]
        print(f"{OK} Bild-URL aus CSV für EAN '{ean_str}': {image_url}")
    else:
        print(f"{WARN} Keine Bild-URL in CSV für EAN '{ean_str}'")
        # 2. Versuch: Bild von OpenFoodFacts
        image_url = product_data.get("image_front_url") or product_data.get("image_url")
        if image_url:
            print(f"{OK} Fallback Bild-URL aus Open Food Facts: {image_url}")
        else:
            print(f"{WARN} Kein Bild in Open Food Facts gefunden")
    
    # ... (bestehender Code für Bilddownload und Upload) ...


def get_ean_from_product_name(product_name):
    name_norm = normalize_string(product_name)
    ean = product_name_to_ean.get(name_norm)
    if ean:
        print(f"{OK} Direkter Namens-Treffer: '{product_name}' → EAN {ean}")
    else:
        print(f"{WARN} Kein direkter Namens-Treffer für '{product_name}'")
    return ean

def get_ean_from_product_name_fuzzy(product_name, cutoff=0.8):
    name_norm = normalize_string(product_name)
    matches = difflib.get_close_matches(name_norm, all_product_names, n=1, cutoff=cutoff)
    if matches:
        match = matches[0]
        ean = product_name_to_ean.get(match)
        print(f"{OK} Fuzzy-Treffer: '{product_name}' ≈ '{match}' → EAN {ean}")
        return ean
    print(f"{WARN} Kein fuzzy Namens-Treffer für '{product_name}'")
    return None

def get_ean_from_rewe_code(rewe_code):
    ean = rewe_code_to_ean.get(str(rewe_code).strip())
    if ean:
        print(f"{OK} REWE-Code-Treffer: {rewe_code} → EAN {ean}")
    else:
        print(f"{WARN} Kein REWE-Code-Treffer für {rewe_code}")
    return ean

def fetch_product_from_off(ean):
    url = f"https://world.openfoodfacts.org/api/v0/product/{ean}.json"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("status") == 1:
            print(f"{OK} Produktdaten von Open Food Facts für EAN {ean} abgerufen.")
            return data["product"]
        else:
            print(f"{WARN} Kein Produkt in Open Food Facts für EAN {ean} gefunden.")
            return None
    except Exception as e:
        print(f"{WARN} Fehler bei Open Food Facts API: {e}")
        return None

def download_image(url, filename):
    try:
        r = requests.get(url, stream=True, timeout=10)
        r.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        print(f"{OK} Bild heruntergeladen: {filename}")
        return True
    except Exception as e:
        print(f"{WARN} Fehler beim Herunterladen des Bildes: {e}")
        return False

def update_product_picture(product_id, file_name, grocy_url, api_key):
    headers = {"GROCY-API-KEY": api_key}

    get_url = f"{grocy_url}/api/objects/products/{product_id}"
    response = requests.get(get_url, headers=headers)
    response.raise_for_status()
    product_obj = response.json()

    product_obj['picture_file_name'] = file_name

    product_obj.pop('userfields', None)
    product_obj.pop('id', None)

    put_url = get_url
    headers_put = headers.copy()
    headers_put["Content-Type"] = "application/json"
    resp_put = requests.put(put_url, headers=headers_put, json=product_obj)
    print(f"PUT Status: {resp_put.status_code} {resp_put.text}")

    return resp_put.status_code in (200, 204)

def upload_product_image(product_id, image_path):
    file_name = f"{product_id}.jpg"
    file_name_b64 = base64.urlsafe_b64encode(file_name.encode()).decode().rstrip("=")

    upload_url = f"{GROCY_API_URL}/api/files/productpictures/{file_name_b64}"
    headers_upload = {
        "GROCY-API-KEY": GROCY_API_KEY,
        "Content-Type": "application/octet-stream"
    }

    try:
        with open(image_path, "rb") as image_file:
            resp = requests.put(upload_url, data=image_file, headers=headers_upload)
        print(f"Upload Status: {resp.status_code} {resp.text}")
        if resp.status_code not in (200, 204):
            print(f"[WARN] Produktbild konnte nicht hochgeladen werden: {resp.status_code} {resp.text}")
            return False
    except Exception as e:
        print(f"[WARN] Fehler beim Upload: {e}")
        return False

    success = update_product_picture(product_id, file_name, GROCY_API_URL, GROCY_API_KEY)
    if not success:
        print(f"[WARN] Produktbild konnte nicht im Produkt hinterlegt werden.")
        return False

    print(f"{OK} Produktbild für Produkt-ID {product_id} erfolgreich hochgeladen und zugewiesen.")
    return True

def create_product_in_grocy(product_data, ean):
    LOCATION_ID_KUEHLSCHRANK = 1
    SHOPPING_LOCATION_ID_REWE = 1

    product_info = {
        "name": product_data.get("product_name", "Unbenanntes Produkt"),
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

        image_url = None
        if ean_str in rewe_code_to_image_url:
            image_url = rewe_code_to_image_url[ean_str]
            print(f"{OK} Bild-URL aus REWE CSV für EAN '{ean_str}': {image_url}")
        else:
            print(f"{WARN} Keine Bild-URL in REWE CSV für EAN '{ean_str}'")

        if not image_url:
            image_url = product_data.get("image_front_url") or product_data.get("image_url")
            if image_url:
                print(f"{OK} Fallback Bild-URL aus Open Food Facts: {image_url}")
            else:
                print(f"{WARN} Kein Bild in Open Food Facts gefunden")

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
        print(f"{WARN} Fehler beim Anlegen des Produkts in Grocy: {e}")
        return None

# Die weiteren Funktionen (add_barcode_to_product, update_stock, grocy_product_exists, etc.) bleiben unverändert.



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

def add_or_update_product(ean, amount, price):
    if grocy_product_exists(ean):
        product_id = get_grocy_product_id_by_ean(ean)
        if product_id:
            return update_stock(product_id, amount, price)
        else:
            print(f"{WARN} Produkt-ID für EAN {ean} konnte nicht gefunden werden.")
            return False
    else:
        product_data = fetch_product_from_off(ean)
        if not product_data:
            print(f"{WARN} Produktdaten für EAN {ean} nicht verfügbar, kann nicht angelegt werden.")
            return False
        product_id = create_product_in_grocy(product_data, ean)
        if not product_id:
            return False
        if not add_barcode_to_product(product_id, ean):
            return False
        return update_stock(product_id, amount, price)
