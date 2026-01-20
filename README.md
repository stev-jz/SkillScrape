# Tech Stack Crawler

Automatically scrapes tech job postings from GitHub repos (like [SimplifyJobs/Summer2026-Internships](https://github.com/SimplifyJobs/Summer2026-Internships)) and extracts tech stack information using AI.

## Features

- 📥 **GitHub README Scraper** - Extracts job URLs from GitHub internship repos
- ⚡ **Batch Processing** - Processes multiple jobs concurrently with asyncio
- 🔄 **Job Tracking** - Avoids re-processing jobs already in the database
- ⏰ **24-Hour Scheduler** - Automatically runs scraping on a schedule
- 🤖 **AI Parsing** - Uses Google Gemini to extract tech skills from job descriptions
- 📊 **PostgreSQL Storage** - Stores jobs and skills in a relational database

## Quick Start

### 1. Setup

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your GOOGLE_API_KEY and DB_CONNECTION_STRING
```

### 2. Start PostgreSQL

```bash
docker-compose up -d
```

### 3. Run the Scraper

```bash
# One-time run (processes all new jobs)
python src/scheduler.py

# Limit to 20 jobs
python src/scheduler.py --max-jobs 20

# Run as a daemon (every 24 hours)
python src/scheduler.py --daemon

# Custom interval (every 12 hours)
python src/scheduler.py --daemon --interval 12

# View stats only
python src/scheduler.py --stats
```

## CLI Options

```
python src/scheduler.py [OPTIONS]

Options:
  -d, --daemon              Run as a daemon (continuous mode)
  -i, --interval HOURS      Hours between runs (default: 24)
  -b, --batch-size N        Jobs per batch (default: 10)
  -c, --max-concurrent N    Max concurrent scraping tasks (default: 5)
  -m, --max-jobs N          Max jobs to process per run (default: unlimited)
  -s, --stats               Just print stats and exit
```

## Architecture

```
src/
├── github_scraper.py   # Fetches job URLs from GitHub README
├── scraper.py          # Web scraper using Crawl4AI
├── parser.py           # AI parsing with Google Gemini
├── db.py               # PostgreSQL database operations
├── job_tracker.py      # Tracks processed jobs, provides stats
├── batch_processor.py  # Concurrent batch processing
└── scheduler.py        # CLI and scheduling logic
```

## Database Schema

```sql
-- Jobs table
jobs (id, title, company, url, raw_skills_data JSONB, created_at)

-- Skills table (normalized)
skills (id, name, category)

-- Junction table
job_skills (job_id, skill_id)
```

## Environment Variables

```bash
GOOGLE_API_KEY=your_gemini_api_key
DB_CONNECTION_STRING=postgresql://myuser:mypassword@localhost:5433/job_market
```

## Running with Cron (Alternative to Daemon)

Instead of running as a daemon, you can use cron:

```bash
# Edit crontab
crontab -e

# Add this line to run daily at 2 AM
0 2 * * * cd /path/to/Tech-Stack-Crawler && ./venv/bin/python src/scheduler.py >> logs/cron.log 2>&1
```

## Sample Output

```
📥 Fetching jobs from GitHub...
✓ Extracted 881 job postings
📊 Found 50 already processed jobs in database
✓ Filtered: 831 new jobs to process

🚀 Processing batch of 10 jobs (max 5 concurrent)...
✓ Batch complete in 45.2s - 10/10 succeeded

📊 JOB DATABASE STATISTICS
   Total jobs: 60
   Jobs added today: 10
   Unique skills tracked: 250

🔥 Top Skills (by job frequency):
   Python (languages): 45 jobs
   Java (languages): 38 jobs
   SQL (databases): 32 jobs
```

## License

MIT
