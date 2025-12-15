#!/usr/bin/env python3
"""
ImmoScout24 → Airtable Sync (PLUGIN Table)
Synced ALLE Daten für das Website-Plugin

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

# Airtable Config
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE = os.getenv("AIRTABLE_BASE_PLUGIN", "")
AIRTABLE_TABLE = os.getenv("AIRTABLE_TABLE_PLUGIN", "")

# CSV Input
CSV_FILE = "immoscout_mutzel.csv"

# ===========================================================================
# AIRTABLE API
# ===========================================================================

def get_airtable_headers():
    return {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }

def get_all_records():
    """Hole alle existierenden Records"""
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
            break
        
        data = response.json()
        all_records.extend(data.get("records", []))
        
        offset = data.get("offset")
        if not offset:
            break
    
    return all_records

def delete_all_records(records):
    """Lösche alle Records"""
    if not records:
        return
    
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}"
    headers = get_airtable_headers()
    
    print(f"\n[AIRTABLE] Lösche {len(records)} alte Records...")
    
    for i in range(0, len(records), 10):
        batch = records[i:i+10]
        record_ids = [r["id"] for r in batch]
        
        params = {"records[]": record_ids}
        response = requests.delete(url, headers=headers, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"  [ERROR] Delete failed: {response.status_code}")
        else:
            print(f"  ✅ Gelöscht: {len(record_ids)} Records")
        
        time.sleep(0.5)

def create_records(records):
    """Erstelle neue Records"""
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}"
    headers = get_airtable_headers()
    
    print(f"\n[AIRTABLE] Erstelle {len(records)} neue Records...")
    
    for i in range(0, len(records), 10):
        batch = records[i:i+10]
        
        payload = {"records": batch}
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code != 200:
            print(f"  [ERROR] Create failed: {response.status_code}")
            print(response.text[:500])
        else:
            print(f"  ✅ Erstellt: {len(batch)} Records")
        
        time.sleep(0.5)

# ===========================================================================
# CSV → AIRTABLE MAPPING (PLUGIN)
# ===========================================================================

def csv_to_airtable_plugin_record(row: dict) -> dict:
    """Konvertiere CSV Row zu Airtable Record (PLUGIN Format - ALLE Felder!)"""
    
    # Preis als Zahl
    preis = row.get("preis", "")
    preis_clean = preis.replace(".", "").replace(",00", "").replace("€", "").strip()
    try:
        preis_num = int(preis_clean) if preis_clean else None
    except:
        preis_num = None
    
    # Zimmer & Fläche als Zahlen
    try:
        zimmer_num = float(row.get("zimmer", "").replace(",", ".")) if row.get("zimmer") else None
    except:
        zimmer_num = None
    
    try:
        flaeche_str = row.get("wohnflaeche", "").replace("m²", "").replace(" ", "").replace(",", ".")
        wohnflaeche_num = float(flaeche_str) if flaeche_str else None
    except:
        wohnflaeche_num = None
    
    # Baujahr als Zahl
    try:
        baujahr_num = int(row.get("baujahr", "")) if row.get("baujahr") else None
    except:
        baujahr_num = None
    
    # ALLE Bilder (nicht nur erste)
    bilder_str = row.get("bilder", "")
    bilder_urls = ""
    if bilder_str:
        bilder_list = bilder_str.split("\n")
        bilder_list = [b.strip() for b in bilder_list if b.strip()]
        bilder_urls = "\n".join(bilder_list)  # ALLE Bilder
    
    # Erste Bild URL einzeln
    erste_bild = bilder_list[0] if bilder_list else ""
    
    # Mapping zu Plugin Airtable Fields
    fields = {
        # Basis Felder
        "title": row.get("titel", ""),
        "expose_id": row.get("expose_id", ""),
        "url": row.get("url", ""),
        
        # Kategorie & Typ
        "kategorie": row.get("kategorie", ""),  # Kauf/Miete
        "unterkategorie": row.get("unterkategorie", ""),  # Wohnung/Haus
        "marketing_typ": "BUY" if row.get("kategorie") == "Kauf" else "RENT",
        "objekt_typ": row.get("unterkategorie", ""),  # Wohnung/Haus
        "rs_typ": row.get("kategorie", ""),  # Kauf/Miete
        
        # Location
        "plz": row.get("plz", ""),
        "ort": row.get("ort", ""),
        "region": row.get("region", ""),
        "kurz_adresse": f"{row.get('plz', '')} {row.get('ort', '')}".strip(),
        
        # Preis & Details
        "preis": preis_num,
        "preis_text": row.get("preis", ""),  # Original mit €
        "zimmer": zimmer_num,
        "wohnfläche": wohnflaeche_num,
        
        # Beschreibung
        "beschreibung": row.get("beschreibung", ""),
        "ausstattung": row.get("ausstattung", ""),
        
        # Technische Details
        "baujahr": baujahr_num,
        "energieausweis": row.get("energieausweis", ""),
        
        # Bilder
        "bild_url": erste_bild,  # Erste für Thumbnails
        "bilder": bilder_urls,  # ALLE für Galerie (newline-separated)
        
        # Status
        "status": row.get("status", "Verfügbar"),
    }
    
    return {"fields": fields}

# ===========================================================================
# MAIN
# ===========================================================================

def main():
    print("=" * 80)
    print("IMMOSCOUT24 → AIRTABLE SYNC (PLUGIN)")
    print("=" * 80)
    
    # Validate Config
    if not AIRTABLE_TOKEN:
        print("\n[ERROR] AIRTABLE_TOKEN nicht gesetzt!")
        return
    
    if not AIRTABLE_BASE or not AIRTABLE_TABLE:
        print("\n[ERROR] AIRTABLE_BASE_PLUGIN oder AIRTABLE_TABLE_PLUGIN nicht gesetzt!")
        return
    
    print(f"\n[CONFIG]")
    print(f"  Base: {AIRTABLE_BASE}")
    print(f"  Table: {AIRTABLE_TABLE}")
    print(f"  CSV: {CSV_FILE}")
    
    # Read CSV
    print(f"\n[PHASE 1] Lese CSV...")
    if not os.path.exists(CSV_FILE):
        print(f"[ERROR] {CSV_FILE} nicht gefunden!")
        return
    
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print(f"  ✅ {len(rows)} Immobilien gefunden")
    
    # PLUGIN: ALLE Immobilien (auch Vermarktet für Referenzen)
    print(f"  ✅ {len(rows)} Immobilien (inkl. Vermarktet)")
    
    # Convert
    print(f"\n[PHASE 2] Konvertiere zu Airtable Format...")
    airtable_records = [csv_to_airtable_plugin_record(row) for row in rows]
    print(f"  ✅ {len(airtable_records)} Records bereit")
    
    # Get existing
    print(f"\n[PHASE 3] Hole existierende Records...")
    existing_records = get_all_records()
    print(f"  ✅ {len(existing_records)} existierende Records")
    
    # Delete old
    if existing_records:
        auto_confirm = os.getenv("AIRTABLE_AUTO_CONFIRM", "false").lower() == "true"
        
        if auto_confirm:
            print(f"\n⚠️  Auto-Confirm: {len(existing_records)} Records werden gelöscht...")
        else:
            confirm = input(f"\n⚠️  {len(existing_records)} Records löschen? (j/n): ")
            if confirm.lower() != "j":
                print("Abgebrochen!")
                return
        
        delete_all_records(existing_records)
    
    # Create new
    if airtable_records:
        create_records(airtable_records)
    
    # Summary
    print("\n" + "=" * 80)
    print("✅ SYNC ABGESCHLOSSEN!")
    print("=" * 80)
    print(f"Gelöscht:  {len(existing_records)} alte Records")
    print(f"Erstellt:  {len(airtable_records)} neue Records")
    print(f"Status:    ALLE Immobilien (inkl. Vermarktet)")
    print(f"Plugin:    Vollständige Daten für Website")
    print("=" * 80)

if __name__ == "__main__":
    main()
