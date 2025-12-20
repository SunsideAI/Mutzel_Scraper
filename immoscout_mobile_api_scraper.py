#!/usr/bin/env python3
"""
ImmoScout24 FINAL PRODUCTION SCRAPER
Nutzt Mobile API - KEIN Captcha, KEIN Browser!

Basiert auf: https://github.com/orangecoding/fredy
Author: Paul Probodziak / Sunside AI
Client: Christian Mutzel
"""

import os
import re
import sys
import csv
import json
import time
import random
from typing import List, Dict, Optional

try:
    import requests
except ImportError:
    print("[ERROR] requests nicht installiert:")
    print("  pip3 install requests --break-system-packages")
    sys.exit(1)

# ===========================================================================
# KONFIGURATION
# ===========================================================================

REALTOR_ID = "a663ec4c008d6d8835a44"
API_BASE = "https://pro-sov-agency-api.is24-realtor-directory.s24cloud.net"
MOBILE_API = "https://api.mobile.immobilienscout24.de"

REQUEST_DELAY = 2.0
MAX_RETRIES = 3

# Airtable (optional)
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE_CHATBOT = os.getenv("AIRTABLE_BASE_CHATBOT", "")
AIRTABLE_TABLE_CHATBOT = os.getenv("AIRTABLE_TABLE_CHATBOT", "")
AIRTABLE_BASE_PLUGIN = os.getenv("AIRTABLE_BASE_PLUGIN", "")
AIRTABLE_TABLE_PLUGIN = os.getenv("AIRTABLE_TABLE_PLUGIN", "")

# ===========================================================================
# HTTP HELPERS
# ===========================================================================

def get_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json",
    }

def get_mobile_headers():
    """Mobile API Headers - KEIN Captcha!"""
    return {
        "User-Agent": "ImmoScout_27.3_26.0_._",
        "Accept": "application/json",
        "Connection": "keep-alive",
    }

def make_request(url: str, retries: int = MAX_RETRIES, mobile: bool = False) -> Optional[requests.Response]:
    headers = get_mobile_headers() if mobile else get_headers()
    
    for attempt in range(retries):
        try:
            time.sleep(REQUEST_DELAY)
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                return response
                
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(REQUEST_DELAY * 2)
    
    return None

# ===========================================================================
# API - LISTINGS
# ===========================================================================

def get_listings_from_api(type_: str, real_estate_type: str) -> List[dict]:
    url = f"{API_BASE}/searchlistings"
    params = {
        "realtorEncryptedId": REALTOR_ID,
        "realtorCwid": "null",
        "pageNumber": 1,
        "pageSize": 100,
        "type": type_,
        "realEstateType": real_estate_type
    }
    
    query_string = "&".join(f"{k}={v}" for k, v in params.items())
    full_url = f"{url}?{query_string}"
    
    print(f"[API] {real_estate_type} {type_}")
    
    response = make_request(full_url)
    
    if response:
        try:
            data = response.json()
            if data.get("status") == "success":
                listings = data.get("data", [])
                print(f"  → {len(listings)} Listings")
                return listings
        except:
            pass
    
    return []

def collect_all_listings():
    all_listings = []
    
    combinations = [
        ("BUY", "RESIDENTIAL"),
        ("RENT", "RESIDENTIAL"),
        ("BUY", "COMMERCIAL"),
        ("RENT", "COMMERCIAL"),
    ]
    
    for type_, real_estate_type in combinations:
        listings = get_listings_from_api(type_, real_estate_type)
        all_listings.extend(listings)
    
    # Trenne aktiv vs. Referenzen
    active = [l for l in all_listings if not l.get("isReference", False)]
    references = [l for l in all_listings if l.get("isReference", False)]
    
    # Dedupliziere
    seen_ids = set()
    active_unique = []
    for l in active:
        if l["exposeId"] not in seen_ids:
            seen_ids.add(l["exposeId"])
            active_unique.append(l)
    
    references_unique = []
    for l in references:
        if l["exposeId"] not in seen_ids:
            seen_ids.add(l["exposeId"])
            references_unique.append(l)
    
    print(f"\n[SUMMARY] Gesamt: {len(seen_ids)} | Aktiv: {len(active_unique)} | Referenzen: {len(references_unique)}")
    
    return active_unique, references_unique

def parse_listing(listing: dict) -> dict:
    expose_id = listing.get("exposeId", "")
    is_buy = listing.get("isBuy", True)
    kategorie = "Kaufen" if is_buy else "Mieten"
    unterkategorie = listing.get("type", "Wohnung")
    
    price = listing.get("price", 0)
    price_formatted = listing.get("priceFormatted", "")
    preis = f"{price_formatted} €" if price_formatted else f"{price:,.0f} €".replace(",", ".")
    
    plz = listing.get("postcode", "")
    ort = listing.get("city", "")
    region = listing.get("region", "")
    wohnflaeche = listing.get("livingSpace", "")
    zimmer = listing.get("numberOfRooms", "")
    
    is_reference = listing.get("isReference", False)
    status = "Vermarktet" if is_reference else "Verfügbar"
    
    url = f"https://www.immobilien-mutzel.de/immobilie?id={expose_id}"
    
    return {
        "expose_id": str(expose_id),
        "titel": "",  # Wird von Mobile API gefüllt
        "kategorie": kategorie,
        "unterkategorie": unterkategorie,
        "preis": preis,
        "wohnflaeche": wohnflaeche,
        "zimmer": str(zimmer) if zimmer else "",
        "plz": plz,
        "ort": ort,
        "region": region,
        "beschreibung": "",
        "bilder": [],
        "ausstattung": "",
        "baujahr": "",
        "energieausweis": "",
        "status": status,
        "url": url,
    }

# ===========================================================================
# MOBILE API - DETAILS
# ===========================================================================

def get_details_from_mobile_api(expose_id: str) -> dict:
    """Hole ALLE Details via Mobile API - KEIN Captcha!"""
    url = f"{MOBILE_API}/expose/{expose_id}"
    
    print(f"  [MOBILE API] {expose_id}")
    
    response = make_request(url, mobile=True)
    
    if not response:
        print(f"    [ERROR] Request failed")
        return {}
    
    try:
        data = response.json()
    except:
        print(f"    [ERROR] JSON parse failed")
        return {}
    
    details = {}
    
    # Titel
    for section in data.get("sections", []):
        if section.get("type") == "TITLE":
            details["titel"] = section.get("title", "")
            print(f"    ✅ {details['titel'][:60]}...")
            break
    
    # Beschreibung
    beschreibung_parts = []
    for section in data.get("sections", []):
        if section.get("type") == "TEXT_AREA":
            title = section.get("title", "")
            # Text ist direkt im section object!
            text = section.get("text", "")
            
            if text:
                if title == "Objektbeschreibung":
                    beschreibung_parts.insert(0, text)  # Hauptbeschreibung zuerst
                else:
                    beschreibung_parts.append(f"\n\n{title}:\n{text}")
    
    if beschreibung_parts:
        details["beschreibung"] = "".join(beschreibung_parts)
        print(f"    📝 Beschreibung: {len(details['beschreibung'])} Zeichen")
    
    # ALLE Bilder
    bilder = []
    for section in data.get("sections", []):
        if section.get("type") == "MEDIA":
            for media in section.get("media", []):
                if media.get("type") == "PICTURE":
                    # Nutze fullImageUrl für höchste Qualität
                    img_url = media.get("fullImageUrl", "")
                    if img_url and img_url not in bilder:
                        bilder.append(img_url)
    
    if bilder:
        details["bilder"] = bilder
        print(f"    🖼️  {len(bilder)} Bilder")
    
    # Ausstattung & weitere Details
    attributes = []
    for section in data.get("sections", []):
        if section.get("type") == "ATTRIBUTE_LIST":
            # attributes ist direkt im section!
            for attr in section.get("attributes", []):
                label = attr.get("label", "")
                text = attr.get("text", "")
                value = attr.get("value", "")  # Manchmal value statt text
                
                display_value = text or value
                
                if label and display_value:
                    attributes.append(f"{label} {display_value}")
                    
                    # Spezielle Felder
                    if "baujahr" in label.lower() and "laut" not in label.lower():
                        details["baujahr"] = display_value
                    elif "energieausweis" in label.lower() or "endenergie" in label.lower():
                        if not details.get("energieausweis"):
                            details["energieausweis"] = f"{label} {display_value}"
    
    if attributes:
        details["ausstattung"] = " | ".join(attributes[:15])
    
    return details

# ===========================================================================
# EXPORT
# ===========================================================================

def export_csv(properties: List[dict], filename: str = "immoscout_mutzel.csv"):
    """Export zu CSV"""
    if not properties:
        return
    
    # Konvertiere Listen zu Strings
    for p in properties:
        if isinstance(p.get("bilder"), list):
            p["bilder"] = "\n".join(p["bilder"])
    
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, 
            fieldnames=properties[0].keys(),
            quoting=csv.QUOTE_MINIMAL  # Nur bei Bedarf quoten!
        )
        writer.writeheader()
        writer.writerows(properties)
    
    print(f"[CSV] ✅ {filename}")

# ===========================================================================
# MAIN
# ===========================================================================

def main():
    print("=" * 80)
    print("IMMOSCOUT24 FINAL SCRAPER - Mobile API (KEIN Captcha!)")
    print("=" * 80)
    print("Basiert auf: https://github.com/orangecoding/fredy")
    
    # PHASE 1: API
    print("\n[PHASE 1] Sammle Listings von API...")
    active, references = collect_all_listings()
    
    # PHASE 2: Parse
    print("\n[PHASE 2] Konvertiere Listings...")
    active_props = [parse_listing(l) for l in active]
    reference_props = [parse_listing(l) for l in references]
    all_props = active_props + reference_props
    
    print(f"  Aktiv: {len(active_props)}")
    print(f"  Referenzen: {len(reference_props)}\n")
    
    if not all_props:
        print("⚠️ Keine Immobilien gefunden!")
        return
    
    # PHASE 3: Details via Mobile API
    print("[PHASE 3] Hole Details via Mobile API (KEIN Captcha!)...")
    for i, prop in enumerate(all_props, 1):
        print(f"\n[{i}/{len(all_props)}]")
        details = get_details_from_mobile_api(prop["expose_id"])
        prop.update(details)
        
        # Rate limiting (höflich bleiben)
        if i < len(all_props):
            wait = random.uniform(2, 4)
            time.sleep(wait)
    
    # PHASE 4: Export
    print("\n[PHASE 4] Speichere CSV...")
    export_csv(all_props)
    
    # Summary
    print("\n" + "=" * 80)
    print("✅ SCRAPING ABGESCHLOSSEN!")
    print("=" * 80)
    print(f"Gesamt:      {len(all_props)} Immobilien")
    print(f"Verfügbar:   {len(active_props)}")
    print(f"Vermarktet:  {len(reference_props)}")
    print("=" * 80)
    
    # Beispiel
    if all_props:
        p = all_props[0]
        bilder_count = p["bilder"].count("\n") + 1 if p["bilder"] else 0
        print(f"\nBeispiel: {p['titel'][:60]}...")
        print(f"  Kategorie: {p['kategorie']} | {p['unterkategorie']}")
        print(f"  Preis: {p['preis']}")
        print(f"  Fläche: {p['wohnflaeche']} | Zimmer: {p['zimmer']}")
        print(f"  Ort: {p['plz']} {p['ort']}")
        print(f"  Beschreibung: {len(p.get('beschreibung', ''))} Zeichen")
        print(f"  Bilder: {bilder_count}")
        print(f"  Ausstattung: {p.get('ausstattung', '')[:80]}...")

if __name__ == "__main__":
    main()
