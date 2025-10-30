"""
Fetch parking bay sensor data from Melbourne Open Data API
and store in Supabase
"""
import requests
from supabase import create_client, Client
from datetime import datetime, timezone
import sys
from config import (
    SUPABASE_URL, SUPABASE_KEY, PARKING_API_URL, 
    RECORDS_LIMIT, get_timestamp
)

def fetch_parking_data():
    """Fetch latest parking data from Melbourne API"""
    print(f"🅿️ [{get_timestamp()}] Starting parking data fetch...")
    
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
        
        # Get the last fetched timestamp from database
        last_record = supabase.table('parking_bay_sensors') \
            .select('status_timestamp') \
            .order('status_timestamp', desc=True) \
            .limit(1) \
            .execute()
        
        if last_record.data:
            last_timestamp = last_record.data[0]['status_timestamp']
            print(f"📅 Last record timestamp: {last_timestamp}")
        else:
            last_timestamp = None
            print("📅 No previous records found, fetching all available data")
        
        # Fetch from Melbourne API (API v2.1 format)
        params = {
            'limit': 100,  # API v2.1 max limit
        }
        
        # Add timestamp filter if we have a last seen timestamp
        if last_timestamp:
            params['where'] = f"status_timestamp > '{last_timestamp}'"
        
        print(f"🌐 Fetching from API: {PARKING_API_URL}")
        print(f"📋 Parameters: {params}")
        
        response = requests.get(PARKING_API_URL, params=params, timeout=30)
        response.raise_for_status()
        
        api_data = response.json()
        records = api_data.get('results', [])
        
        if not records:
            print("ℹ️ No new records to fetch")
            return 0
        
        print(f"📦 Received {len(records)} records from API")
        
        # Transform data for Supabase (matching runner.py format)
        transformed_records = []
        skipped_count = 0
        
        for item in records:
            try:
                # Skip records without required fields
                if not item.get('zone_number'):
                    skipped_count += 1
                    continue
                
                location = item.get('location', {})
                
                transformed_records.append({
                    'zone_number': item.get('zone_number'),
                    'kerbsideid': item.get('kerbsideid'),
                    'status_description': item.get('status_description'),
                    'status_timestamp': item.get('status_timestamp'),
                    'latitude': location.get('lat') if location else None,
                    'longitude': location.get('lon') if location else None,
                    'lastupdated': item.get('lastupdated'),
                    'created_at': datetime.now(timezone.utc).isoformat()
                })
            except Exception as e:
                skipped_count += 1
                continue
        
        if skipped_count > 0:
            print(f"⚠️  Skipped {skipped_count} records with missing data")
        
        if not transformed_records:
            print("ℹ️ No valid records to insert")
            return 0
        
        print(f"📊 Upserting {len(transformed_records)} records...")
        
        # Upsert into Supabase (batch insert)
        result = supabase.table('parking_bay_sensors').upsert(
            transformed_records,
            on_conflict='zone_number,kerbsideid,status_timestamp'
        ).execute()
        
        inserted_count = len(result.data) if result.data else len(transformed_records)
        print(f"✅ Successfully upserted {inserted_count} parking records")
        
        return inserted_count
        
    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"📄 Response content: {e.response.text[:500]}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    try:
        count = fetch_parking_data()
        print(f"🎉 Parking data fetch completed! Upserted {count} records")
    except KeyboardInterrupt:
        print("\n⚠️ Fetch interrupted by user")
        sys.exit(0)