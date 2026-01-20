"""
Job Tracker

Tracks which jobs have already been processed to avoid duplicates.
Uses the database to store processed job URLs.
"""
from typing import List, Set
from db import get_db_connection, get_failed_urls
from github_scraper import JobPosting


def get_processed_urls() -> Set[str]:
    """
    Get all URLs that have already been processed and saved to the database.
    
    Returns:
        Set of URLs that are already in the database
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT url FROM jobs WHERE url IS NOT NULL")
            rows = cur.fetchall()
            return {row['url'] for row in rows}


def filter_new_jobs(jobs: List[JobPosting], skip_failed: bool = True) -> List[JobPosting]:
    """
    Filter out jobs that have already been processed or have failed.
    
    Args:
        jobs: List of job postings from GitHub
        skip_failed: If True, also skip URLs that previously failed to scrape
        
    Returns:
        List of jobs that haven't been processed yet
    """
    processed_urls = get_processed_urls()
    print(f"📊 Found {len(processed_urls)} already processed jobs in database")
    
    failed_urls = set()
    if skip_failed:
        failed_urls = get_failed_urls()
        if failed_urls:
            print(f"⚠️  Found {len(failed_urls)} previously failed URLs (skipping)")
    
    # Combine URLs to skip
    skip_urls = processed_urls | failed_urls
    
    # Filter out jobs whose URLs are already in the database or have failed
    new_jobs = []
    for job in jobs:
        # Check both with and without query params
        url_clean = job.apply_url.split('?')[0]
        
        if job.apply_url not in skip_urls and url_clean not in skip_urls:
            new_jobs.append(job)
    
    skipped_processed = len([j for j in jobs if j.apply_url in processed_urls or j.apply_url.split('?')[0] in processed_urls])
    skipped_failed = len(jobs) - len(new_jobs) - skipped_processed
    print(f"✓ Filtered: {len(new_jobs)} new jobs to process ({skipped_processed} processed, {skipped_failed} failed)")
    
    return new_jobs


def get_job_stats() -> dict:
    """
    Get statistics about processed jobs.
    
    Returns:
        Dictionary with job statistics
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Total jobs
            cur.execute("SELECT COUNT(*) as count FROM jobs")
            total_jobs = cur.fetchone()['count']
            
            # Jobs by company (top 10)
            cur.execute("""
                SELECT company, COUNT(*) as count 
                FROM jobs 
                WHERE company IS NOT NULL
                GROUP BY company 
                ORDER BY count DESC 
                LIMIT 10
            """)
            top_companies = cur.fetchall()
            
            # Total unique skills
            cur.execute("SELECT COUNT(*) as count FROM skills")
            total_skills = cur.fetchone()['count']
            
            # Top skills
            cur.execute("""
                SELECT s.name, s.category, COUNT(js.job_id) as job_count
                FROM skills s
                JOIN job_skills js ON s.id = js.skill_id
                GROUP BY s.id, s.name, s.category
                ORDER BY job_count DESC
                LIMIT 15
            """)
            top_skills = cur.fetchall()
            
            # Jobs added today
            cur.execute("""
                SELECT COUNT(*) as count 
                FROM jobs 
                WHERE created_at >= CURRENT_DATE
            """)
            jobs_today = cur.fetchone()['count']
            
            return {
                'total_jobs': total_jobs,
                'jobs_today': jobs_today,
                'total_skills': total_skills,
                'top_companies': [
                    {'name': r['company'], 'count': r['count']} 
                    for r in top_companies
                ],
                'top_skills': [
                    {'name': r['name'], 'category': r['category'], 'job_count': r['job_count']}
                    for r in top_skills
                ]
            }


def print_stats():
    """Print formatted job statistics."""
    stats = get_job_stats()
    
    print("\n" + "=" * 60)
    print("JOB DATABASE STATISTICS")
    print("=" * 60)
    
    print(f"\nTotal jobs: {stats['total_jobs']}")
    print(f"Jobs added today: {stats['jobs_today']}")
    print(f"Unique skills tracked: {stats['total_skills']}")
    
    print("\nTop Companies:")
    for c in stats['top_companies'][:5]:
        print(f"   {c['name']}: {c['count']} jobs")
    
    print("\nTop Skills (by job frequency):")
    for s in stats['top_skills'][:10]:
        print(f"   {s['name']} ({s['category']}): {s['job_count']} jobs")
    
    print("=" * 60)


if __name__ == "__main__":
    from db import init_db
    init_db()
    print_stats()
