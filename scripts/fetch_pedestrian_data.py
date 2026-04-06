"""
Fetch pedestrian counting data from Melbourne Open Data API
and store in Supabase with pagination support
Based on working runner.py implementation
"""
import requests
from supabase import create_client, Client
from datetime import datetime, timezone
import sys
import os

# Ensure scripts/ directory is on path so config.py can be found
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import (
    SUPABASE_URL, SUPABASE_KEY, get_timestamp
)

# Correct API endpoint from runner.py
BASE = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets"
DATASET_ID = "pedestrian-counting-system-past-hour-counts-per-minute"
PEDESTRIAN_API_URL = f"{BASE}/{DATASET_ID}/records"

def _to_utc_iso(dt_str: str) -> str:
    """Normalize datetime to UTC ISO format"""
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).isoformat()

def _row_from_fields(fields: dict) -> dict:
    """
    Transform API fields to match Supabase table schema
    Expected fields from API:
    - location_id
    - sensing_datetime
    - sensing_date
    - sensing_time
    - direction_1
    - direction_2
    - total_of_directions
    """
    location_id = int(fields["location_id"])
    sensing_datetime_iso = _to_utc_iso(fields["sensing_datetime"])
    sensing_date = fields["sensing_date"]
    sensing_time = fields["sensing_time"]
    direction_1 = int(fields["direction_1"])
    direction_2 = int(fields["direction_2"])
    total_of_directions = int(fields["total_of_directions"])

    return {
        "location_id": location_id,
        "sensing_datetime": sensing_datetime_iso,
        "sensing_date": sensing_date,
        "sensing_time": sensing_time,
        "direction_1": direction_1,
        "direction_2": direction_2,
        "total_of_directions": total_of_directions,
    }

def fetch_pedestrian_data():
    """Fetch latest pedestrian counting data from Melbourne API with pagination"""
    print(f"🚶 [{get_timestamp()}] Starting pedestrian data fetch...")
    
    # Check environment variables
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Missing SUPABASE_URL or SUPABASE_KEY")
        print(f"   SUPABASE_URL: {'✅ Set' if SUPABASE_URL else '❌ Missing'}")
        print(f"   SUPABASE_KEY: {'✅ Set' if SUPABASE_KEY else '❌ Missing'}")
        sys.exit(1)
    
    try:
        # Initialize Supabase client
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Connected to Supabase")
        
        # CORRECT TABLE NAME from runner.py
        table_name = 'ped_counts_minute'
        
        # Get the last fetched timestamp (using correct field name)
        last_record = supabase.table(table_name) \
            .select('sensing_datetime') \
            .order('sensing_datetime', desc=True) \
            .limit(1) \
            .execute()
        
        if last_record.data:
            last_timestamp = last_record.data[0]['sensing_datetime']
            print(f"📅 Last record timestamp: {last_timestamp}")
        else:
            last_timestamp = None
            print("📅 No previous records found, fetching all available data")
        
        # Fetch ALL pages from Melbourne API
        all_rows = []
        offset = 0
        batch_size = 100
        max_offset = 10000  # API limit from runner.py
        total_fetched = 0
        
        print(f"🌐 Fetching from API: {PEDESTRIAN_API_URL}")
        
        while True:
            # Stop if we've reached the API's max offset
            if offset >= max_offset:
                print(f"⚠️  Reached API offset limit ({max_offset}). Stopping pagination.")
                print(f"💡 Subsequent runs will use timestamp filtering.")
                break
            
            params = {
                'limit': batch_size,
                'offset': offset,
            }
            
            # Add timestamp filter if we have a last seen timestamp
            if last_timestamp:
                params['where'] = f"sensing_datetime > '{last_timestamp}'"
            
            if offset == 0:
                print(f"📋 Query parameters: {params}")
            
            try:
                response = requests.get(PEDESTRIAN_API_URL, params=params, timeout=30)
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 400 and offset >= max_offset:
                    print(f"⚠️  Hit API limit at offset {offset}")
                    break
                print(f"⚠️  API request failed on page {offset // batch_size + 1}: {e}")
                if offset == 0:
                    raise
                else:
                    break
            except requests.exceptions.RequestException as e:
                print(f"⚠️  Request failed: {e}")
                if offset == 0:
                    raise
                else:
                    break
            
            payload = response.json()
            results = payload.get('results', [])
            
            if not results:
                if offset == 0:
                    print(f"ℹ️  No new records available")
                else:
                    print(f"📄 Page {offset // batch_size + 1}: No more records (reached end)")
                break
            
            if offset == 0:
                print(f"🔍 API returned {len(results)} results on first page")
                total_records = payload.get("total_count", "unknown")
                print(f"📊 Total records available: {total_records}")
                if isinstance(total_records, int) and total_records > max_offset:
                    print(f"⚠️  Note: Total records ({total_records}) exceeds API limit ({max_offset})")
                    print(f"💡 Will fetch first {max_offset} records, then use timestamp filtering")
                
                # Print first record structure for debugging
                if results:
                    print(f"🔍 First record fields: {list(results[0].keys())}")
            
            # Process each result
            for i, fields in enumerate(results):
                try:
                    row = _row_from_fields(fields)
                    
                    # Only include rows newer than our last seen timestamp
                    if last_timestamp is None or row["sensing_datetime"] > last_timestamp:
                        all_rows.append(row)
                        
                except KeyError as e:
                    if offset == 0 and i == 0:
                        print(f"⚠️  Missing required field {e} in first record")
                        print(f"📄 Available fields: {list(fields.keys())}")
                except ValueError as e:
                    pass  # Skip records with invalid data types
                except Exception as e:
                    if offset == 0 and i < 3:
                        print(f"⚠️  Error parsing record {i}: {e}")
            
            total_fetched += len(results)
            
            # Check if we got fewer results than the limit (last page)
            if len(results) < batch_size:
                print(f"✅ Fetched all available records (last page had {len(results)} records)")
                break
            
            # Progress reporting
            if offset % 1000 == 0 or offset < 1000:
                print(f"📄 Page {offset // batch_size + 1}: Fetched {len(results)} records (Total: {total_fetched})")
            
            offset += batch_size
        
        print(f"📊 Total rows fetched from API: {total_fetched}")
        print(f"📊 Total rows to upsert (after filtering): {len(all_rows)}")
        
        if not all_rows:
            print("ℹ️ No new records to process")
            return 0
        
        # Add created_at timestamp to all rows
        now = datetime.now(timezone.utc).isoformat()
        for row in all_rows:
            row['created_at'] = now
        
        print(f"📊 Upserting {len(all_rows)} records to Supabase...")
        
        # Upsert into Supabase in batches (matching runner.py approach)
        upsert_batch_size = 1000
        total_upserted = 0
        
        for i in range(0, len(all_rows), upsert_batch_size):
            batch = all_rows[i:i + upsert_batch_size]
            batch_num = i // upsert_batch_size + 1
            
            try:
                # Use upsert with correct conflict resolution (from runner.py)
                result = supabase.table(table_name).upsert(
                    batch,
                    on_conflict='location_id,sensing_datetime'
                ).execute()
                
                batch_count = len(result.data) if result.data else len(batch)
                total_upserted += batch_count
                
                if len(all_rows) > upsert_batch_size:
                    print(f"   ✅ Batch {batch_num}: Upserted {batch_count} records (Total: {total_upserted})")
                    
            except Exception as e:
                print(f"   ❌ Upsert failed on batch {batch_num}: {e}")
                
                # Fallback to insert (from runner.py)
                try:
                    result = supabase.table(table_name).insert(batch).execute()
                    batch_count = len(result.data) if result.data else len(batch)
                    total_upserted += batch_count
                    print(f"   ✅ Batch {batch_num}: Inserted {batch_count} records")
                except Exception as e2:
                    print(f"   ❌ Insert also failed on batch {batch_num}: {e2}")
                    continue
        
        print(f"🎉 Successfully upserted {total_upserted} pedestrian records!")
        
        return total_upserted
        
    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"📄 Response status: {e.response.status_code}")
            print(f"📄 Response content: {e.response.text[:500]}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    try:
        count = fetch_pedestrian_data()
        print(f"\n{'='*60}")
        print(f"✅ PEDESTRIAN DATA FETCH COMPLETED")
        print(f"📊 Total records upserted: {count}")
        print(f"⏰ Timestamp: {get_timestamp()}")
        print(f"{'='*60}")
    except KeyboardInterrupt:
        print("\n⚠️ Fetch interrupted by user")
        sys.exit(0)