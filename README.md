# CoM_PC - City of Melbourne Parking & Pedestrian Counter

🚗 🚶 Automated data collection and spatiotemporal analysis of Melbourne's parking and pedestrian sensors.

## 🎯 Project Overview

This project automatically collects, stores, and analyzes real-time data from Melbourne's urban sensors:
- **Parking Bay Sensors**: On-street parking occupancy (1,886 bays)
- **Pedestrian Counters**: Foot traffic patterns (65+ locations)

Data is collected hourly via GitHub Actions and stored in Supabase for analysis.

---

## 🚀 Features

- ✅ **Automated hourly data fetching** via GitHub Actions (runs 24/7)
- ✅ **Parking bay sensor data** (occupancy status, GPS coordinates)
- ✅ **Pedestrian counting data** (hourly foot traffic by location)
- ✅ **Supabase PostgreSQL storage** (scalable cloud database)
- ✅ **Interactive Jupyter notebooks** for temporal analysis
- ✅ **Mapbox visualizations** (heatmaps, animated maps)
- ✅ **Zero-cost infrastructure** (GitHub Actions free tier)

---

## 📊 Data Sources

| Dataset | API Endpoint | Update Frequency |
|---------|-------------|------------------|
| [On-Street Parking Bay Sensors](https://data.melbourne.vic.gov.au/Transport/On-street-Parking-Bay-Sensors/vh2v-4nfs) | OpenDataSoft API v2.1 | Real-time |
| [Pedestrian Counting System](https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-Monthly-counts-per-hour/b2ak-trbp) | OpenDataSoft API v2.1 | Hourly |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     GitHub Actions                          │
│              (Scheduled every hour at :00)                  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              Python Fetcher Scripts                         │
│   • fetch_parking_data.py                                   │
│   • fetch_pedestrian_data.py                                │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                 Supabase (PostgreSQL)                       │
│   Tables:                                                   │
│   • parking_bay_sensors (56K+ records)                      │
│   • pedestrian_counts (400K+ records)                       │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│            Jupyter Notebooks (Analysis)                     │
│   • Temporal patterns                                       │
│   • Heatmaps & visualizations                               │
│   • Interactive Mapbox maps                                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 📂 Repository Structure

```
CoM_PC/
├── .github/
│   └── workflows/
│       └── fetch_data.yml          # GitHub Actions workflow (hourly cron)
├── scripts/
│   ├── config.py                   # Configuration & API endpoints
│   ├── fetch_parking_data.py       # Parking data fetcher
│   └── fetch_pedestrian_data.py    # Pedestrian data fetcher
├── analysis/
│   ├── 28-29.10.25.ipynb           # Pedestrian temporal analysis
│   ├── 28-29.10.25_parking.ipynb   # Parking occupancy analysis
│   ├── *.html                      # Interactive Mapbox maps
│   └── *.png                       # Static visualizations
├── logs/                           # Execution logs (auto-generated)
├── requirements.txt                # Python dependencies
├── .gitignore                      # Git ignore rules
└── README.md                       # This file
```

---

## 🛠️ Setup & Installation

### Prerequisites

- **GitHub account** with Actions enabled (3,000 free minutes/month)
- **Supabase account** (free tier is sufficient)
- **Python 3.11+** (for local testing)

### Step 1: Clone Repository

```bash
git clone https://github.com/ertugrul14/CoM_PC.git
cd CoM_PC
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Configure GitHub Secrets

1. Go to: **Settings → Secrets and variables → Actions**
2. Click: **"New repository secret"**
3. Add the following secrets:

| Secret Name | Description | Where to Find |
|------------|-------------|---------------|
| `SUPABASE_URL` | Your Supabase project URL | Supabase Dashboard → Settings → API → Project URL |
| `SUPABASE_KEY` | Your Supabase anon/public key | Supabase Dashboard → Settings → API → `anon` `public` key |

### Step 4: Enable GitHub Actions

1. Go to: **Actions** tab
2. Click: **"I understand my workflows, go ahead and enable them"**
3. The workflow will now run automatically every hour!

### Step 5: Manual Test Run (Optional)

1. Go to: **Actions → "Fetch Melbourne Data"**
2. Click: **"Run workflow"** dropdown → **"Run workflow"**
3. Watch the live logs to verify everything works

---

## 🤖 Automated Data Collection

### How It Works

The GitHub Actions workflow (`.github/workflows/fetch_data.yml`) runs automatically:

- **Schedule**: Every hour at `:00` (e.g., 10:00, 11:00, 12:00...)
- **Duration**: ~2-3 minutes per run
- **Monthly usage**: ~2,160 Actions minutes (within free 3,000 limit)
- **Cost**: **$0** (covered by GitHub Pro free tier)

### What Happens Each Hour

1. ✅ GitHub Actions spins up an Ubuntu container
2. ✅ Installs Python dependencies
3. ✅ Runs `fetch_parking_data.py`:
   - Fetches latest parking sensor readings from Melbourne API
   - Checks last timestamp in Supabase to avoid duplicates
   - Inserts new records into `parking_bay_sensors` table
4. ✅ Runs `fetch_pedestrian_data.py`:
   - Fetches latest pedestrian counts from Melbourne API
   - Inserts new records into `pedestrian_counts` table
5. ✅ Logs success/failure and exits

### Monitoring

- **View runs**: [Actions tab](https://github.com/ertugrul14/CoM_PC/actions)
- **Check logs**: Click any workflow run to see detailed output
- **Usage stats**: Settings → Billing → Plans and usage

---

## 📊 Analysis Notebooks

### Pedestrian Analysis (`28-29.10.25.ipynb`)

**Key Insights:**
- 📈 Peak hours: 12 PM - 1 PM (lunch rush)
- 🌃 Evening peak: 5 PM - 6 PM
- 🏆 Busiest location: Bourke Street Mall
- 📉 Lowest activity: 4 AM - 6 AM

**Visualizations:**
- Hourly pedestrian patterns
- Heatmap: Hour vs Day
- Top 15 busiest sensors
- Time of day analysis
- Interactive Mapbox map

### Parking Analysis (`28-29.10.25_parking.ipynb`)

**Key Insights:**
- 🅿️ Average occupancy: ~48%
- 🔴 Peak occupancy: 9 AM - 11 AM, 2 PM - 5 PM
- 🟢 Best time to find parking: 4 AM - 7 AM
- 🏆 Most occupied zones: CBD core streets
- 📊 1,886 unique parking bays tracked

**Visualizations:**
- Hourly occupancy patterns
- Peak hours comparison
- Top 15 busiest parking zones
- Time of day parking demand
- Interactive animated occupancy map

### Correlation Analysis

- 📉 Pedestrian vs Parking correlation: **0.65** (moderate positive)
- When foot traffic increases, parking occupancy also rises
- Peak hours align between both datasets

---

## 🗺️ Interactive Maps

All analysis notebooks generate interactive Mapbox maps:

- **Dark theme** for better visibility
- **Hover tooltips** with detailed info
- **Animated hourly playback** (parking occupancy over 24 hours)
- **Color-coded heatmaps** (green = low, red = high)
- **Responsive** (zoom, pan, search)

**Mapbox Token**: Maps use a personal access token (included in notebooks)

---

## 📈 Sample Queries

### Parking Data

```sql
-- Get current parking occupancy rate
SELECT 
  COUNT(*) FILTER (WHERE status_description = 'Present') * 100.0 / COUNT(*) as occupancy_rate
FROM parking_bay_sensors
WHERE status_timestamp > NOW() - INTERVAL '1 hour';

-- Top 10 most occupied parking zones today
SELECT 
  zone_number,
  COUNT(*) FILTER (WHERE status_description = 'Present') as occupied_count,
  COUNT(*) as total_readings
FROM parking_bay_sensors
WHERE DATE(status_timestamp AT TIME ZONE 'Australia/Melbourne') = CURRENT_DATE
GROUP BY zone_number
ORDER BY occupied_count DESC
LIMIT 10;
```

### Pedestrian Data

```sql
-- Busiest pedestrian locations this week
SELECT 
  sensor_name,
  SUM(pedestrian_count) as total_pedestrians
FROM pedestrian_counts
WHERE melbourne_time > NOW() - INTERVAL '7 days'
GROUP BY sensor_name
ORDER BY total_pedestrians DESC
LIMIT 10;

-- Hourly pedestrian pattern (average by hour)
SELECT 
  hour,
  AVG(pedestrian_count) as avg_count
FROM pedestrian_counts
GROUP BY hour
ORDER BY hour;
```

---

## 🔧 Local Development

### Test Fetchers Locally

```bash
# Set environment variables
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_KEY="your-anon-key"

# Run parking fetcher
cd scripts
python fetch_parking_data.py

# Run pedestrian fetcher
python fetch_pedestrian_data.py
```

### Run Jupyter Notebooks

```bash
# Install Jupyter
pip install jupyter

# Start Jupyter server
jupyter notebook

# Open analysis/*.ipynb files
```

---

## 📊 Data Schema

### `parking_bay_sensors`

| Column | Type | Description |
|--------|------|-------------|
| `id` | bigint | Primary key (auto-increment) |
| `zone_number` | text | Parking zone identifier |
| `kerbsideid` | text | Unique bay ID |
| `status_description` | text | "Present" or "Absent" |
| `status_timestamp` | timestamptz | Sensor reading time (UTC) |
| `latitude` | float | GPS latitude |
| `longitude` | float | GPS longitude |
| `lastupdated` | timestamptz | Last API update |
| `fetched_at` | timestamptz | When we fetched it |

### `pedestrian_counts`

| Column | Type | Description |
|--------|------|-------------|
| `id` | bigint | Primary key (auto-increment) |
| `location_id` | int | Sensor ID |
| `sensor_name` | text | Location name |
| `melbourne_time` | timestamptz | Timestamp (Melbourne TZ) |
| `pedestrian_count` | int | Hourly pedestrian count |
| `year` | int | Year |
| `month` | int | Month (1-12) |
| `day` | int | Day of month |
| `hour` | int | Hour (0-23) |
| `latitude` | float | GPS latitude |
| `longitude` | float | GPS longitude |
| `fetched_at` | timestamptz | When we fetched it |

---

## 🐛 Troubleshooting

### Workflow Fails

1. **Check secrets**: Settings → Secrets → Verify `SUPABASE_URL` and `SUPABASE_KEY`
2. **Check logs**: Actions → Click failed run → Expand steps
3. **Test API**: Visit Melbourne Open Data API URLs in browser
4. **Check Supabase**: Verify tables exist and RLS policies allow inserts

### No New Data

- Melbourne API might be temporarily down
- Check if API endpoints changed
- Verify Supabase connection (test locally)

### Actions Usage Limit

- Monitor: Settings → Billing → Plans and usage
- Current usage: ~2,160 minutes/month
- Limit: 3,000 minutes/month (GitHub Pro)
- If exceeded: Workflow pauses until next month

---

## 📧 Contact

**Ertugrul Akdemir**
- GitHub: [@ertugrul14](https://github.com/ertugrul14)
- Repository: [CoM_PC](https://github.com/ertugrul14/CoM_PC)

---

## 📜 License

This project is open source and available under the MIT License.

---

## 🙏 Acknowledgments

- **City of Melbourne** - Open Data Platform
- **Supabase** - PostgreSQL database hosting
- **GitHub Actions** - Free CI/CD automation
- **Mapbox** - Interactive map visualizations

---

⭐ **Star this repo if you find it useful!**

🐛 **Found a bug? Open an issue!**

🤝 **Want to contribute? Pull requests welcome!**