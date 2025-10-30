"""
One-time script to fetch and store pedestrian sensor locations data
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

# Melbourne API endpoint for sensor locations
API_BASE = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/pedestrian-counting-system-sensor-locations/records"

def fetch_all_sensor_locations():
    """Fetch all sensor location records from Melbourne API"""
    all_records = []
    offset = 0
    limit = 100
    max_offset = 10000  # API limit
    
    print(f"🚀 Starting to fetch sensor locations data...")
    
    with httpx.Client(timeout=60.0) as client:
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
        "location_id": record.get("location_id"),
        "sensor_description": record.get("sensor_description"),
        "sensor_name": record.get("sensor_name"),
        "installation_date": record.get("installation_date"),
        "note": record.get("note"),
        "location_type": record.get("location_type"),
        "status": record.get("status"),
        "direction_1": record.get("direction_1"),
        "direction_2": record.get("direction_2"),
        "latitude": record.get("latitude"),
        "longitude": record.get("longitude"),
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
        if r["location_id"] is not None
    ]
    
    skipped = len(transformed) - len(valid_records)
    if skipped > 0:
        print(f"⚠️  Skipped {skipped} records with missing location_id")
    
    # Deduplicate by location_id (keep the last occurrence)
    seen = {}
    for record in valid_records:
        seen[record["location_id"]] = record
    
    deduped_records = list(seen.values())
    
    duplicates_removed = len(valid_records) - len(deduped_records)
    if duplicates_removed > 0:
        print(f"⚠️  Removed {duplicates_removed} duplicate location_id values from batch")
    
    print(f"⬆️  Upserting {len(deduped_records)} unique records to Supabase...")
    
    # Upsert in batches of 100
    batch_size = 100
    total_upserted = 0
    
    for i in range(0, len(deduped_records), batch_size):
        batch = deduped_records[i:i + batch_size]
        
        try:
            result = sb.table("pedestrian_sensor_locations").upsert(
                batch,
                on_conflict="location_id"
            ).execute()
            
            total_upserted += len(batch)
            print(f"✅ Upserted batch {i//batch_size + 1}: {len(batch)} rows (total: {total_upserted})")
        except Exception as e:
            print(f"⚠️  Error upserting batch {i//batch_size + 1}: {e}")
            continue
    
    print(f"🎉 Successfully processed {total_upserted} sensor location records!")

def main():
    """Main execution"""
    print("=" * 60)
    print("🚶 Melbourne Pedestrian Sensor Locations Fetcher")
    print("=" * 60)
    
    # Validate environment
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
        print("❌ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE")
        return
    
    print(f"🔧 Supabase URL: {SUPABASE_URL}")
    
    # Create Supabase client
    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
    
    try:
        # Fetch all records
        records = fetch_all_sensor_locations()
        
        if records:
            # Upsert to Supabase
            upsert_to_supabase(sb, records)
            
            print("\n✅ All done! Sensor locations data is now in Supabase.")
            print(f"📊 Check your 'pedestrian_sensor_locations' table in Supabase dashboard")
            print(f"📊 Total records stored: {len(records)}")
        else:
            print("❌ No records fetched")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()