#!/usr/bin/env python3
"""
ImmoScout24 → Airtable Sync (CHATBOT Table)
Synced die ImmoScout-Daten zu deiner Chatbot-Tabelle

Author: Paul Probodziak
"""

import os
import sys
import csv
import json
import time

try:
    import requests
except ImportError:
    print("[ERROR] requests nicht installiert:")
    print("  pip3 install requests --break-system-packages")
    sys.exit(1)

# ===========================================================================
# KONFIGURATION
# ===========================================================================

# Airtable Config (SETZE DEINE WERTE!)
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE = os.getenv("AIRTABLE_BASE_CHATBOT", "")
AIRTABLE_TABLE = os.getenv("AIRTABLE_TABLE_CHATBOT", "")

# CSV Input
CSV_FILE = "immoscout_mutzel.csv"

# Chatbot Table Config
MAX_DESCRIPTION_LENGTH = 5000  # Max Beschreibung für Chatbot
MAX_IMAGES = 5  # Nur erste 5 Bilder für Chatbot

# ===========================================================================
# AIRTABLE API
# ===========================================================================

def get_airtable_headers():
    return {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }

def get_all_records():
    """Hole alle existierenden Records aus Airtable"""
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}"
    headers = get_airtable_headers()
    
    all_records = []
    offset = None
    
    while True:
        params = {}
        if offset:
            params["offset"] = offset
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"[ERROR] Airtable GET failed: {response.status_code}")
            print(response.text[:500])
            break
        
        data = response.json()
        all_records.extend(data.get("records", []))
        
        offset = data.get("offset")
        if not offset:
            break
    
    return all_records

def delete_all_records(records):
    """Lösche alle Records (für sauberen Sync)"""
    if not records:
        return
    
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}"
    headers = get_airtable_headers()
    
    print(f"\n[AIRTABLE] Lösche {len(records)} alte Records...")
    
    # Airtable erlaubt max 10 Deletes pro Request
    for i in range(0, len(records), 10):
        batch = records[i:i+10]
        record_ids = [r["id"] for r in batch]
        
        # DELETE mit record IDs als Query Params
        params = {"records[]": record_ids}
        response = requests.delete(url, headers=headers, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"  [ERROR] Delete failed: {response.status_code}")
        else:
            print(f"  ✅ Gelöscht: {len(record_ids)} Records")
        
        time.sleep(0.5)  # Rate limiting

def create_records(records):
    """Erstelle neue Records"""
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}"
    headers = get_airtable_headers()
    
    print(f"\n[AIRTABLE] Erstelle {len(records)} neue Records...")
    
    # Airtable erlaubt max 10 Creates pro Request
    for i in range(0, len(records), 10):
        batch = records[i:i+10]
        
        payload = {"records": batch}
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code != 200:
            print(f"  [ERROR] Create failed: {response.status_code}")
            print(response.text[:500])
        else:
            print(f"  ✅ Erstellt: {len(batch)} Records")
        
        time.sleep(0.5)  # Rate limiting

# ===========================================================================
# CSV → AIRTABLE MAPPING
# ===========================================================================

def csv_to_airtable_record(row: dict) -> dict:
    """Konvertiere CSV Row zu Airtable Record (CHATBOT Format)"""
    
    # Beschreibung (max 5000 Zeichen für Chatbot)
    beschreibung = row.get("beschreibung", "")
    if len(beschreibung) > MAX_DESCRIPTION_LENGTH:
        beschreibung = beschreibung[:MAX_DESCRIPTION_LENGTH-3] + "..."
    
    # Bilder - Nur ERSTE URL als Text (für Chatbot)
    bilder_str = row.get("bilder", "")
    erste_bild_url = ""
    
    if bilder_str:
        bilder_list = bilder_str.split("\n")
        bilder_list = [b.strip() for b in bilder_list if b.strip()]
        if bilder_list:
            erste_bild_url = bilder_list[0]  # Nur erste URL
    
    # Preis - extrahiere nur die Zahl
    preis = row.get("preis", "")
    # "299.000,00 €" → 299000
    preis_clean = preis.replace(".", "").replace(",00", "").replace(",", "").replace("€", "").strip()
    try:
        preis_value = int(preis_clean) if preis_clean else preis
    except:
        preis_value = preis  # Fallback auf Original
    
    # Mapping zu deinen Airtable Fields
    fields = {
        "Titel": row.get("titel", ""),
        "Kategorie": row.get("kategorie", ""),
        "Webseite": row.get("url", ""),
        "Objektnummer": row.get("expose_id", ""),
        "Beschreibung": beschreibung,
        "Bild": erste_bild_url,  # Nur erste URL als Text!
        "Preis": preis_value,  # Als Zahl
        "Standort": f"{row.get('plz', '')} {row.get('ort', '')}".strip(),
    }
    
    return {"fields": fields}

# ===========================================================================
# MAIN
# ===========================================================================

def main():
    print("=" * 80)
    print("IMMOSCOUT24 → AIRTABLE SYNC (CHATBOT)")
    print("=" * 80)
    
    # Validate Config
    if not AIRTABLE_TOKEN:
        print("\n[ERROR] AIRTABLE_TOKEN nicht gesetzt!")
        print("Setze: export AIRTABLE_TOKEN='your_token'")
        return
    
    if not AIRTABLE_BASE or not AIRTABLE_TABLE:
        print("\n[ERROR] AIRTABLE_BASE_CHATBOT oder AIRTABLE_TABLE_CHATBOT nicht gesetzt!")
        print("Setze:")
        print("  export AIRTABLE_BASE_CHATBOT='appXXXXXXXXXXXXXX'")
        print("  export AIRTABLE_TABLE_CHATBOT='tblXXXXXXXXXXXXXX'")
        return
    
    print(f"\n[CONFIG]")
    print(f"  Base: {AIRTABLE_BASE}")
    print(f"  Table: {AIRTABLE_TABLE}")
    print(f"  CSV: {CSV_FILE}")
    
    # Read CSV
    print(f"\n[PHASE 1] Lese CSV...")
    if not os.path.exists(CSV_FILE):
        print(f"[ERROR] {CSV_FILE} nicht gefunden!")
        print("Führe zuerst aus: python3 immoscout_mobile_api_scraper.py")
        return
    
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print(f"  ✅ {len(rows)} Immobilien gefunden")
    
    # Filter: Nur aktive (nicht "Vermarktet")
    active_rows = [r for r in rows if r.get("status", "") != "Vermarktet"]
    print(f"  ✅ {len(active_rows)} aktive Immobilien (ohne Vermarktet)")
    
    # Convert to Airtable format
    print(f"\n[PHASE 2] Konvertiere zu Airtable Format...")
    airtable_records = [csv_to_airtable_record(row) for row in active_rows]
    print(f"  ✅ {len(airtable_records)} Records bereit")
    
    # Get existing records
    print(f"\n[PHASE 3] Hole existierende Records...")
    existing_records = get_all_records()
    print(f"  ✅ {len(existing_records)} existierende Records")
    
    # Delete old records
    if existing_records:
        # Auto-confirm in GitHub Actions (kein interaktives Terminal)
        auto_confirm = os.getenv("AIRTABLE_AUTO_CONFIRM", "false").lower() == "true"
        
        if auto_confirm:
            print(f"\n⚠️  Auto-Confirm: {len(existing_records)} Records werden gelöscht...")
        else:
            confirm = input(f"\n⚠️  {len(existing_records)} Records löschen und neu erstellen? (j/n): ")
            if confirm.lower() != "j":
                print("Abgebrochen!")
                return
        
        delete_all_records(existing_records)
    
    # Create new records
    if airtable_records:
        create_records(airtable_records)
    
    # Summary
    print("\n" + "=" * 80)
    print("✅ SYNC ABGESCHLOSSEN!")
    print("=" * 80)
    print(f"Gelöscht:  {len(existing_records)} alte Records")
    print(f"Erstellt:  {len(airtable_records)} neue Records")
    print(f"Status:    Nur aktive Immobilien (ohne Vermarktet)")
    print(f"Chatbot:   Max {MAX_DESCRIPTION_LENGTH} Zeichen Beschreibung")
    print(f"Bilder:    Erste {MAX_IMAGES} Bilder pro Immobilie")
    print("=" * 80)

if __name__ == "__main__":
    main()
