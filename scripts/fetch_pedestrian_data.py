"""
Fetch pedestrian counting data from Melbourne Open Data API
and store in Supabase with pagination support
"""
import requests
from supabase import create_client, Client
from datetime import datetime, timezone
import sys
from config import (
    SUPABASE_URL, SUPABASE_KEY, PEDESTRIAN_API_URL,
    get_timestamp
)

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
        
        # Get the last fetched timestamp
        last_record = supabase.table('pedestrian_counts') \
            .select('melbourne_time') \
            .order('melbourne_time', desc=True) \
            .limit(1) \
            .execute()
        
        if last_record.data:
            last_timestamp = last_record.data[0]['melbourne_time']
            print(f"📅 Last record timestamp: {last_timestamp}")
        else:
            last_timestamp = None
            print("📅 No previous records found, fetching all available data")
        
        # Fetch ALL pages from Melbourne API
        all_records = []
        offset = 0
        batch_size = 100
        max_pages = 50  # Safety limit (5000 records max)
        
        print(f"🌐 Fetching from API: {PEDESTRIAN_API_URL}")
        
        for page in range(max_pages):
            params = {
                'limit': batch_size,
                'offset': offset,
            }
            
            # Add timestamp filter if we have a last seen timestamp
            if last_timestamp:
                params['where'] = f"date_time > '{last_timestamp}'"
            
            if page == 0:
                print(f"📋 Query parameters: {params}")
            
            try:
                response = requests.get(PEDESTRIAN_API_URL, params=params, timeout=30)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"⚠️  API request failed on page {page + 1}: {e}")
                if page == 0:
                    # If first page fails, exit with error
                    raise
                else:
                    # If later page fails, use what we got
                    break
            
            api_data = response.json()
            records = api_data.get('results', [])
            
            if not records:
                if page == 0:
                    print(f"ℹ️  No new records available")
                else:
                    print(f"📄 Page {page + 1}: No more records (reached end)")
                break
            
            all_records.extend(records)
            print(f"📄 Page {page + 1}: Fetched {len(records)} records (Total so far: {len(all_records)})")
            
            # If we got fewer records than batch_size, we've reached the end
            if len(records) < batch_size:
                print(f"✅ Reached end of available data (last page had {len(records)} records)")
                break
            
            offset += batch_size
        
        if not all_records:
            print("ℹ️ No new records to process")
            return 0
        
        print(f"📦 Total fetched: {len(all_records)} records from API")
        
        # Transform data for Supabase
        transformed_records = []
        skipped_count = 0
        
        for item in all_records:
            try:
                # Skip records without required fields
                if not item.get('sensor_id') or not item.get('date_time'):
                    skipped_count += 1
                    continue
                
                transformed_records.append({
                    'location_id': item.get('sensor_id'),
                    'sensor_name': item.get('sensor_name'),
                    'melbourne_time': item.get('date_time'),
                    'pedestrian_count': item.get('hourly_counts'),
                    'year': item.get('year'),
                    'month': item.get('month'),
                    'day': item.get('day'),
                    'hour': item.get('time'),
                    'latitude': item.get('latitude'),
                    'longitude': item.get('longitude'),
                    'created_at': datetime.now(timezone.utc).isoformat()
                })
            except Exception as e:
                skipped_count += 1
                continue
        
        if skipped_count > 0:
            print(f"⚠️  Skipped {skipped_count} records with missing/invalid data")
        
        if not transformed_records:
            print("⚠️  No valid records to insert after filtering")
            return 0
        
        print(f"📊 Upserting {len(transformed_records)} valid records to Supabase...")
        
        # Upsert into Supabase in batches (max 1000 per batch for performance)
        upsert_batch_size = 1000
        total_upserted = 0
        
        for i in range(0, len(transformed_records), upsert_batch_size):
            batch = transformed_records[i:i + upsert_batch_size]
            batch_num = i // upsert_batch_size + 1
            
            try:
                result = supabase.table('pedestrian_counts').upsert(
                    batch,
                    on_conflict='location_id,melbourne_time'
                ).execute()
                
                batch_count = len(result.data) if result.data else len(batch)
                total_upserted += batch_count
                
                if len(transformed_records) > upsert_batch_size:
                    print(f"   ✅ Batch {batch_num}: Upserted {batch_count} records")
            except Exception as e:
                print(f"   ❌ Batch {batch_num} failed: {e}")
                # Continue with next batch
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