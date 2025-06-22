import requests
from colorTerminal import OK, WARN, ERROR
from config import BON_HISTORY, HARDCODED_RTSP_TOKEN
from grocy_connector import (
    add_or_update_product,
#    load_rewe_price_data,
    get_ean_from_product_name,
    get_ean_from_product_name_fuzzy,
    get_ean_from_rewe_code,
    grocy_product_exists,
)
from rewe_products_import import main as update_rewe_products_db

import grocy_connector
import json

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

RECEIPT_URL = "https://shop.rewe.de/api/receipts/"

def prerequisites():
    try:
        with open("ignore.txt", "r", encoding='utf-8') as f:
            f.read().splitlines()
    except FileNotFoundError:
        print(f"{ERROR} ignore.txt nicht gefunden. Datei wird erstellt..")
        with open("ignore.txt", "w", encoding='utf-8') as f:
            f.write("")

def fetch_rewe_bon(rtsp: str):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(RECEIPT_URL, cookies={"rstp": rtsp}, headers=headers, timeout=10)
        response.raise_for_status()
        receipt_list = response.json()
    except requests.exceptions.RequestException as e:
        print(f"{ERROR} HTTP-Fehler beim Abrufen der eBon-Liste: {e}")
        return None, None
    if 'items' not in receipt_list:
        print(f"{ERROR} Keine 'items' in der Antwort gefunden.")
        return None, None

    option_receipts = receipt_list['items']
    print(f"{OK} Empfange eBon-Liste der letzten Einkäufe:")
    for x in range(min(BON_HISTORY, len(option_receipts))):
        receipt = option_receipts[x]
        print(f"ID: {x}; Vom: {receipt['receiptTimestamp']}; Summe: {receipt['receiptTotalPrice']/100:.2f}€")

    while True:
        try:
            option = int(input(f"Welchen Bon möchtest du an Grocy senden? (ID 0-{min(BON_HISTORY, len(option_receipts))-1}): "))
            if 0 <= option < min(BON_HISTORY, len(option_receipts)):
                break
            else:
                print(f"{ERROR} Bitte wähle einen Bon zwischen 0 und {min(BON_HISTORY, len(option_receipts))-1} aus.")
        except ValueError:
            print(f"{ERROR} Bitte eine gültige Zahl eingeben.")

    try:
        rewe_bon_response = requests.get(RECEIPT_URL + option_receipts[option]['receiptId'], cookies={"rstp": rtsp}, headers=headers, timeout=10)
        rewe_bon_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"{ERROR} Fehler beim Abrufen des ausgewählten Rewe-Bons: {e}")
        return None, None

    print(f"{OK} Rewe-Bon mit der UUID {option_receipts[option]['receiptId']} wurde erfolgreich abgerufen")
    # Extrahiere das Kaufdatum
    purchased_date = option_receipts[option]['receiptTimestamp'][:10]  # "YYYY-MM-DD"
    return rewe_bon_response.json().get("articles", []), purchased_date

def processrewe_bon(rewe_bon, purchased_date=None):
    for product in rewe_bon:
        product_name = product.get("productName", "")
        if not product_name:
            print(f"{WARN} Produkt ohne 'productName' gefunden, wird übersprungen: {product}")
            continue
        ean = get_ean_from_product_name(product_name)
        if not ean:
            ean = get_ean_from_product_name_fuzzy(product_name)
        if not ean:
            rewe_code = product.get("nan", "")
            ean = get_ean_from_rewe_code(rewe_code)
        if not ean:
            ean = str(product.get("nan", ""))
        quantity = int(product.get("quantity", 0))
        unit_price = product.get("unitPrice", 0) / 100
        print(f"Verarbeite Produkt: Name='{product_name}', EAN={ean}, Menge={quantity}, Preis={unit_price:.2f}€")

        # Hier das Kaufdatum mitgeben!
        add_or_update_product(ean, quantity, unit_price, bon_product_name=product_name, purchased_date=purchased_date)

def main():
    prerequisites()
    print("Willkommen im Rewe2Grocy Connector!")
    print("Der RTSP Token ist hart im Script hinterlegt und wird verwendet.\n")

    rtsp = HARDCODED_RTSP_TOKEN
    rewe_bon, purchased_date = fetch_rewe_bon(rtsp)

    if rewe_bon:
        processrewe_bon(rewe_bon, purchased_date=purchased_date)
    else:
        print(f"{ERROR} Kein gültiger eBon abgerufen. Bitte Token prüfen und erneut versuchen.")
    grocy_connector.db_conn.close()

if __name__ == "__main__":
    # Prüfe und aktualisiere die REWE-Produktdatenbank, falls neue Daten vorhanden sind
    update_rewe_products_db()
    # Starte danach die eigentliche Hauptfunktion
    main()
