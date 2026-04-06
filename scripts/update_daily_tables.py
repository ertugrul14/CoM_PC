#!/usr/bin/env python3
"""
update_daily_tables.py

Aggregates raw data from ped_melbourne and parking_melbourne tables
into the daily summary tables (ped_melbourne_daily, parking_melbourne_daily).

Run this script periodically to keep the daily tables up to date.
"""

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client
from pathlib import Path

# Load environment variables from parent directory
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY') or os.getenv('SUPABASE_SERVICE_ROLE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment or .env file")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_last_daily_date(table_name: str) -> str:
    """Get the last date in the daily table."""
    try:
        response = supabase.table(table_name).select('date').order('date', desc=True).limit(1).execute()
        if response.data:
            return response.data[0]['date']
    except Exception as e:
        print(f"Error getting last date from {table_name}: {e}")
    return None


def aggregate_ped_daily(start_date: str, end_date: str):
    """Aggregate pedestrian data from ped_melbourne into ped_melbourne_daily."""
    print(f"\n📊 Aggregating pedestrian data from {start_date} to {end_date}...")
    
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    records_added = 0
    
    while current_date <= end:
        date_str = current_date.strftime('%Y-%m-%d')
        
        try:
            # Fetch all records for this date
            start_ts = f"{date_str}T00:00:00"
            end_ts = f"{date_str}T23:59:59"
            
            all_records = []
            offset = 0
            limit = 1000
            
            while True:
                response = supabase.table('ped_melbourne').select(
                    'total_of_directions'
                ).gte('local_datetime', start_ts).lte('local_datetime', end_ts).range(offset, offset + limit - 1).execute()
                
                if response.data:
                    all_records.extend(response.data)
                    if len(response.data) < limit:
                        break
                    offset += limit
                else:
                    break
            
            if all_records:
                daily_total = sum(r.get('total_of_directions', 0) or 0 for r in all_records)
                
                # Upsert into daily table
                supabase.table('ped_melbourne_daily').upsert({
                    'date': date_str,
                    'daily_total': daily_total,
                    'record_count': len(all_records)
                }, on_conflict='date').execute()
                
                records_added += 1
                print(f"  ✓ {date_str}: {daily_total:,} movements ({len(all_records)} records)")
            else:
                print(f"  - {date_str}: No data")
        
        except Exception as e:
            print(f"  ✗ {date_str}: Error - {e}")
        
        current_date += timedelta(days=1)
    
    print(f"\n✓ Added/updated {records_added} daily pedestrian records")
    return records_added


def aggregate_parking_daily(start_date: str, end_date: str):
    """Aggregate parking data from parking_melbourne into parking_melbourne_daily."""
    print(f"\n📊 Aggregating parking data from {start_date} to {end_date}...")
    
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    records_added = 0
    
    while current_date <= end:
        date_str = current_date.strftime('%Y-%m-%d')
        
        try:
            # Fetch count for this date using limit=1 with range header
            start_ts = f"{date_str}T00:00:00"
            end_ts = f"{date_str}T23:59:59"
            
            # Count records by fetching with range
            all_records = []
            offset = 0
            limit = 1000
            
            while True:
                response = supabase.table('parking_melbourne').select(
                    'id'
                ).gte('local_datetime', start_ts).lte('local_datetime', end_ts).range(offset, offset + limit - 1).execute()
                
                if response.data:
                    all_records.extend(response.data)
                    if len(response.data) < limit:
                        break
                    offset += limit
                else:
                    break
            
            record_count = len(all_records)
            
            if record_count > 0:
                # Upsert into daily table
                supabase.table('parking_melbourne_daily').upsert({
                    'date': date_str,
                    'total_status_changes': record_count
                }, on_conflict='date').execute()
                
                records_added += 1
                print(f"  ✓ {date_str}: {record_count:,} status changes")
            else:
                print(f"  - {date_str}: No data")
        
        except Exception as e:
            print(f"  ✗ {date_str}: Error - {e}")
        
        current_date += timedelta(days=1)
    
    print(f"\n✓ Added/updated {records_added} daily parking records")
    return records_added


def main():
    print("=" * 60)
    print("Melbourne Data Daily Aggregation")
    print("=" * 60)
    
    # Get last dates in daily tables
    last_ped_date = get_last_daily_date('ped_melbourne_daily')
    last_parking_date = get_last_daily_date('parking_melbourne_daily')
    
    print(f"\nCurrent daily table coverage:")
    print(f"  - Pedestrian: up to {last_ped_date or 'N/A'}")
    print(f"  - Parking: up to {last_parking_date or 'N/A'}")
    
    # Calculate dates to aggregate (from last date + 1 to yesterday)
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Aggregate pedestrian data
    if last_ped_date:
        start_date = (datetime.strptime(last_ped_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        if start_date <= yesterday:
            aggregate_ped_daily(start_date, yesterday)
        else:
            print("\n✓ Pedestrian daily table is up to date")
    else:
        # Start from a reasonable date
        aggregate_ped_daily('2025-10-28', yesterday)
    
    # Aggregate parking data
    if last_parking_date:
        start_date = (datetime.strptime(last_parking_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        if start_date <= yesterday:
            aggregate_parking_daily(start_date, yesterday)
        else:
            print("\n✓ Parking daily table is up to date")
    else:
        aggregate_parking_daily('2025-10-28', yesterday)
    
    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == '__main__':
    main()
