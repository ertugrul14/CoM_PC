"""
Configuration for Melbourne data fetchers
"""
import os
from datetime import datetime

# Supabase Configuration
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY') or os.environ.get('SUPABASE_SERVICE_ROLE')

# Melbourne Open Data API Endpoints
PARKING_API_URL = 'https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/on-street-parking-bay-sensors/records'
PEDESTRIAN_API_URL = 'https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/pedestrian-counting-system-monthly-counts-per-hour/records'

# Fetch Configuration
RECORDS_LIMIT = 1000
TIMEZONE = 'Australia/Melbourne'

def get_timestamp():
    """Get current timestamp in Melbourne timezone"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')