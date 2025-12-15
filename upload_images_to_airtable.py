#!/usr/bin/env python3
"""
Upload images from Airtable text field to Attachment field
Reads URLs from 'bilder' field, uploads to 'bilder_attachments'
"""

import os
import sys
from pyairtable import Api

# Get credentials from environment
AT_TOKEN = os.getenv('AIRTABLE_TOKEN')
AT_BASE = os.getenv('AIRTABLE_BASE_PLUGIN')
AT_TABLE = os.getenv('AIRTABLE_TABLE_PLUGIN')

if not all([AT_TOKEN, AT_BASE, AT_TABLE]):
    print("❌ Missing environment variables!")
    print("Required: AIRTABLE_TOKEN, AIRTABLE_BASE_PLUGIN, AIRTABLE_TABLE_PLUGIN")
    sys.exit(1)

def main():
    print("🔄 Starting image upload to Airtable...")
    
    api = Api(AT_TOKEN)
    table = api.table(AT_BASE, AT_TABLE)
    
    # Get all records
    print("📥 Fetching records...")
    records = table.all()
    print(f"Found {len(records)} records")
    
    updated_count = 0
    skipped_count = 0
    error_count = 0
    
    for record in records:
        fields = record['fields']
        expose_id = fields.get('expose_id', record['id'])
        
        # Skip if already has attachments
        if fields.get('bilder_attachments'):
            print(f"⏭️  {expose_id} - already has attachments")
            skipped_count += 1
            continue
        
        # Get image URLs from bilder field (newline-separated)
        bilder_text = fields.get('bilder', '')
        if not bilder_text:
            print(f"⏭️  {expose_id} - no images")
            skipped_count += 1
            continue
        
        # Parse URLs
        image_urls = [url.strip() for url in bilder_text.split('\n') if url.strip()]
        
        if not image_urls:
            print(f"⏭️  {expose_id} - no valid URLs")
            skipped_count += 1
            continue
        
        print(f"📸 {expose_id} - uploading {len(image_urls)} images")
        
        # Create attachment objects
        # Airtable will download from these URLs and host them
        attachments = []
        for url in image_urls[:10]:  # Max 10 images
            try:
                attachments.append({"url": url})
            except Exception as e:
                print(f"   ⚠️  Error with URL {url}: {e}")
        
        if not attachments:
            print(f"❌ {expose_id} - no valid attachments")
            error_count += 1
            continue
        
        try:
            # Update record with attachments
            table.update(record['id'], {
                'bilder_attachments': attachments
            })
            print(f"✅ {expose_id} - uploaded {len(attachments)} images")
            updated_count += 1
            
        except Exception as e:
            print(f"❌ {expose_id} - error updating: {e}")
            error_count += 1
    
    print("\n" + "="*50)
    print(f"✅ Updated: {updated_count}")
    print(f"⏭️  Skipped: {skipped_count}")
    print(f"❌ Errors: {error_count}")
    print("="*50)

if __name__ == '__main__':
    main()
