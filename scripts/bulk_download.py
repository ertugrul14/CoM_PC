"""
Bulk Download Script - Downloads all data in chunks to bypass 500K limit.
Saves to local parquet files for fast loading.
"""
import os
from pathlib import Path
import pandas as pd
from supabase import create_client
from datetime import datetime
import time
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv(Path(__file__).resolve().parent.parent / '.env')
SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']

OUTPUT_DIR = str(Path(__file__).resolve().parent.parent / 'data' / 'bulk_download')
os.makedirs(OUTPUT_DIR, exist_ok=True)

client = create_client(SUPABASE_URL, SUPABASE_KEY)


def download_table_chunked(table_name: str, batch_size: int = 10000, max_retries: int = 5):
    """
    Download entire table using ID-based pagination.
    Saves progress incrementally so you can resume if interrupted.
    """
    print(f"\n{'='*60}")
    print(f"Downloading: {table_name}")
    print(f"{'='*60}")
    
    output_file = os.path.join(OUTPUT_DIR, f"{table_name}_full.parquet")
    progress_file = os.path.join(OUTPUT_DIR, f"{table_name}_progress.txt")
    
    # Check for resume
    last_id = 0
    existing_data = []
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            last_id = int(f.read().strip())
        print(f"Resuming from ID: {last_id:,}")
        if os.path.exists(output_file):
            existing_data = [pd.read_parquet(output_file)]
            print(f"Loaded {len(existing_data[0]):,} existing rows")
    
    all_data = existing_data
    total_fetched = sum(len(d) for d in all_data)
    start_time = time.time()
    
    while True:
        retries = 0
        batch_df = None
        
        while retries < max_retries:
            try:
                response = client.table(table_name) \
                    .select('*') \
                    .gt('id', last_id) \
                    .order('id', desc=False) \
                    .limit(batch_size) \
                    .execute()
                
                if not response.data:
                    print(f"\nDownload complete! Total: {total_fetched:,} rows")
                    break
                
                batch_df = pd.DataFrame(response.data)
                break
                
            except Exception as e:
                retries += 1
                print(f"\nError (retry {retries}/{max_retries}): {e}")
                time.sleep(2 ** retries)  # Exponential backoff
        
        if batch_df is None or batch_df.empty:
            break
        
        all_data.append(batch_df)
        last_id = batch_df['id'].max()
        total_fetched += len(batch_df)
        
        # Save progress every 100K rows
        if total_fetched % 100000 < batch_size:
            with open(progress_file, 'w') as f:
                f.write(str(last_id))
            
            # Save intermediate parquet
            combined = pd.concat(all_data, ignore_index=True)
            combined.to_parquet(output_file, index=False)
            
            elapsed = time.time() - start_time
            rate = total_fetched / elapsed if elapsed > 0 else 0
            print(f"  Saved checkpoint: {total_fetched:,} rows | {rate:.0f} rows/sec")
        else:
            # Progress indicator
            if total_fetched % 10000 == 0:
                elapsed = time.time() - start_time
                rate = total_fetched / elapsed if elapsed > 0 else 0
                print(f"\r  Downloaded: {total_fetched:,} rows | {rate:.0f} rows/sec", end='', flush=True)
    
    # Final save
    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_parquet(output_file, index=False)
        
        # Cleanup progress file
        if os.path.exists(progress_file):
            os.remove(progress_file)
        
        elapsed = time.time() - start_time
        print(f"\n\nSaved to: {output_file}")
        print(f"Total rows: {len(combined):,}")
        print(f"File size: {os.path.getsize(output_file) / 1024 / 1024:.1f} MB")
        print(f"Time: {elapsed:.1f} seconds")
        
        return combined
    
    return pd.DataFrame()


def main():
    print("="*60)
    print("BULK DOWNLOAD - Melbourne Street Data")
    print("="*60)
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Batch size: 10,000 rows per request")
    print(f"Progress saved after every 100K rows (resumable)")
    
    # Download pedestrian data
    ped_df = download_table_chunked('ped_melbourne', batch_size=10000)
    
    # Download parking data
    parking_df = download_table_chunked('parking_melbourne', batch_size=10000)
    
    print("\n" + "="*60)
    print("DOWNLOAD COMPLETE")
    print("="*60)
    print(f"\nFiles saved to: {OUTPUT_DIR}")
    print(f"  - ped_melbourne_full.parquet: {len(ped_df):,} rows")
    print(f"  - parking_melbourne_full.parquet: {len(parking_df):,} rows")
    print("\nYou can now run the pipeline with local data!")


if __name__ == '__main__':
    main()
