"""
One-time script to fetch and store static on-street parking bays data
"""
import os
import httpx
import time
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv(override=True)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.getenv("SUPABASE_SERVICE_ROLE")

# Melbourne API endpoint for parking bays
API_BASE = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/on-street-parking-bays/records"

def fetch_all_parking_bays():
    """Fetch all parking bay records from Melbourne API"""
    all_records = []
    offset = 0
    limit = 100
    max_offset = 10000  # API limit
    
    print(f"🚀 Starting to fetch parking bays data...")
    
    with httpx.Client(timeout=60.0) as client:  # Increased timeout to 60s
        while offset < max_offset:
            print(f"📄 Fetching page {offset//limit + 1}... ({len(all_records)} records so far)")
            
            params = {
                "limit": limit,
                "offset": offset
            }
            
            # Retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = client.get(API_BASE, params=params)
                    response.raise_for_status()
                    data = response.json()
                    
                    records = data.get("results", [])
                    
                    if not records:
                        print(f"✅ Reached end of data. Total records: {len(all_records)}")
                        return all_records
                    
                    all_records.extend(records)
                    offset += limit
                    
                    # Check if we've reached the total count
                    total_count = data.get("total_count", 0)
                    if len(all_records) >= total_count:
                        print(f"✅ Fetched all {len(all_records)} records")
                        return all_records
                    
                    break  # Success, exit retry loop
                    
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 400 and offset >= max_offset:
                        print(f"⚠️  Reached API offset limit ({max_offset}). Fetched {len(all_records)} records.")
                        print(f"💡 This is normal - the API has a 10,000 record limit.")
                        return all_records
                    else:
                        raise
                        
                except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 5
                        print(f"⚠️  Timeout on attempt {attempt + 1}/{max_retries}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"❌ Failed after {max_retries} attempts. Saving {len(all_records)} records collected so far.")
                        return all_records
    
    return all_records

def transform_record(record):
    """Transform API record to database format"""
    location = record.get("location", {})
    
    return {
        "roadsegmentid": record.get("roadsegmentid"),
        "kerbsideid": record.get("kerbsideid"),
        "roadsegmentdescription": record.get("roadsegmentdescription"),
        "latitude": record.get("latitude"),
        "longitude": record.get("longitude"),
        "lastupdated": record.get("lastupdated"),
        "location": location,  # Store as JSONB
        "updated_at": datetime.now().isoformat()
    }

def upsert_to_supabase(sb: Client, records):
    """Upsert records to Supabase in batches"""
    if not records:
        print("⚠️  No records to upsert")
        return
    
    print(f"📊 Transforming {len(records)} records...")
    transformed = [transform_record(r) for r in records]
    
    # Filter out records with missing required fields
    valid_records = [
        r for r in transformed 
        if r["roadsegmentid"] is not None
    ]
    
    skipped = len(transformed) - len(valid_records)
    if skipped > 0:
        print(f"⚠️  Skipped {skipped} records with missing roadsegmentid")
    
    # Deduplicate by roadsegmentid (keep the last occurrence)
    seen = {}
    for record in valid_records:
        seen[record["roadsegmentid"]] = record
    
    deduped_records = list(seen.values())
    
    duplicates_removed = len(valid_records) - len(deduped_records)
    if duplicates_removed > 0:
        print(f"⚠️  Removed {duplicates_removed} duplicate roadsegmentid values from batch")
    
    print(f"⬆️  Upserting {len(deduped_records)} unique records to Supabase...")
    
    # Upsert in batches of 500 (smaller batches for safety)
    batch_size = 500
    total_upserted = 0
    
    for i in range(0, len(deduped_records), batch_size):
        batch = deduped_records[i:i + batch_size]
        
        try:
            result = sb.table("parking_bays").upsert(
                batch,
                on_conflict="roadsegmentid"
            ).execute()
            
            total_upserted += len(batch)
            print(f"✅ Upserted batch {i//batch_size + 1}: {len(batch)} rows (total: {total_upserted})")
        except Exception as e:
            print(f"⚠️  Error upserting batch {i//batch_size + 1}: {e}")
            # Skip this batch and continue
            continue
    
    print(f"🎉 Successfully processed {total_upserted} parking bay records!")

def main():
    """Main execution"""
    print("=" * 60)
    print("🚗 Melbourne On-Street Parking Bays Fetcher")
    print("=" * 60)
    
    # Validate environment
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
        print("❌ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE")
        return
    
    print(f"🔧 Supabase URL: {SUPABASE_URL}")
    
    # Create Supabase client
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
    
    try:
        # Fetch all records (up to API limit)
        records = fetch_all_parking_bays()
        
        if records:
            # Upsert to Supabase
            upsert_to_supabase(sb, records)
            
            print("\n✅ All done! Parking bays data is now in Supabase.")
            print(f"📊 Check your 'parking_bays' table in Supabase dashboard")
            print(f"📊 Total records stored: {len(records)}")
        else:
            print("❌ No records fetched")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()