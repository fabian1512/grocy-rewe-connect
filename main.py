import requests
from colorTerminal import OK, WARN, ERROR
from config import BON_HISTORY
from grocy_connector import (
    add_or_update_product,
#    load_rewe_price_data,
    get_ean_from_product_name,
    get_ean_from_product_name_fuzzy,
    get_ean_from_rewe_code,
    grocy_product_exists,
)

import grocy_connector
import json

RECEIPT_URL = "https://shop.rewe.de/api/receipts/"

# Hier deinen RTSP-Token hart eintragen (komplett, ohne Anführungszeichen im Token)
HARDCODED_RTSP_TOKEN = "YOUR_RTSP_TOKEN_HERE"

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
        return None
    if 'items' not in receipt_list:
        print(f"{ERROR} Keine 'items' in der Antwort gefunden.")
        return None

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
        return None

    print(f"{OK} Rewe-Bon mit der UUID {option_receipts[option]['receiptId']} wurde erfolgreich abgerufen")
    return rewe_bon_response.json().get("articles", [])

def processrewe_bon(rewe_bon):
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

        add_or_update_product(ean, quantity, unit_price, bon_product_name=product_name)

def main():
    prerequisites()
    print("Willkommen im Rewe2Grocy Connector!")
    print("Der RTSP Token ist hart im Script hinterlegt und wird verwendet.\n")

    rtsp = HARDCODED_RTSP_TOKEN
    rewe_bon = fetch_rewe_bon(rtsp)

    if rewe_bon:
        print(f"{OK} --- Kompletter Bon-Inhalt ---")
        print(json.dumps(rewe_bon, indent=2, ensure_ascii=False))
        print(f"{OK} --- Ende Bon-Inhalt ---\n")
        processrewe_bon(rewe_bon)
    else:
        print(f"{ERROR} Kein gültiger eBon abgerufen. Bitte Token prüfen und erneut versuchen.")
    grocy_connector.db_conn.close()

if __name__ == "__main__":
    main()
