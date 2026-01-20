"""
Batch Job Processor

Processes multiple job postings concurrently using asyncio.
Configurable batch size and concurrency limits to balance speed vs rate limiting.
"""
import asyncio
from typing import List, Optional
from dataclasses import dataclass
import time

from github_scraper import JobPosting, get_job_urls
from scraper import scrape_page
from parser import parse_job_text, parse_job_texts_batch
from db import save_job_data, init_db, save_failed_url
from job_tracker import filter_new_jobs, print_stats


@dataclass
class ProcessResult:
    """Result of processing a single job."""
    job: JobPosting
    success: bool
    error: Optional[str] = None
    parsed_data: Optional[dict] = None


class BatchProcessor:
    """
    Processes job postings in batches with configurable concurrency.
    
    Args:
        max_concurrent: Maximum number of concurrent scraping tasks (default: 5)
        delay_between_batches: Seconds to wait between batches (default: 2)
    """
    
    def __init__(self, max_concurrent: int = 5, delay_between_batches: float = 2.0):
        self.max_concurrent = max_concurrent
        self.delay_between_batches = delay_between_batches
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Stats
        self.processed = 0
        self.succeeded = 0
        self.failed = 0
    
    async def process_single_job(self, job: JobPosting) -> ProcessResult:
        """
        Process a single job posting: scrape -> parse -> save.
        Uses semaphore to limit concurrency.
        Failed URLs are saved to avoid retrying them.
        """
        async with self.semaphore:
            try:
                # 1. Scrape the job page
                html_content = await scrape_page(job.apply_url)
                
                if not html_content or len(html_content) < 500:
                    error_msg = f"Scraping failed or content too short ({len(html_content) if html_content else 0} chars)"
                    save_failed_url(job.apply_url, error_msg)
                    return ProcessResult(
                        job=job,
                        success=False,
                        error=error_msg
                    )
                
                # 2. Parse with Gemini
                parsed = parse_job_text(html_content)
                
                if not parsed:
                    error_msg = "Parsing failed"
                    save_failed_url(job.apply_url, error_msg)
                    return ProcessResult(
                        job=job,
                        success=False,
                        error=error_msg
                    )
                
                # 3. Enrich with data from GitHub (in case Gemini missed it)
                if not parsed.get('job_title') or parsed.get('job_title') == 'null':
                    parsed['job_title'] = job.role
                if not parsed.get('company') or parsed.get('company') == 'null':
                    parsed['company'] = job.company
                
                # Add the URL
                parsed['url'] = job.apply_url
                parsed['location'] = job.location
                
                # 4. Save to database
                save_job_data(parsed)
                
                return ProcessResult(
                    job=job,
                    success=True,
                    parsed_data=parsed
                )
                
            except Exception as e:
                error_msg = str(e)
                save_failed_url(job.apply_url, error_msg)
                return ProcessResult(
                    job=job,
                    success=False,
                    error=error_msg
                )
    
    async def scrape_single_job(self, job: JobPosting) -> tuple[JobPosting, Optional[str], Optional[str]]:
        """
        Scrape a single job page (no parsing).
        Returns: (job, html_content, error)
        """
        async with self.semaphore:
            try:
                html_content = await scrape_page(job.apply_url)
                
                if not html_content or len(html_content) < 500:
                    error_msg = f"Scraping failed or content too short ({len(html_content) if html_content else 0} chars)"
                    return (job, None, error_msg)
                
                return (job, html_content, None)
                
            except Exception as e:
                return (job, None, str(e))

    async def process_batch(self, jobs: List[JobPosting]) -> List[ProcessResult]:
        """
        Process a batch of jobs efficiently:
        1. Scrape all jobs concurrently
        2. Parse ALL scraped content in ONE API call (saves requests!)
        3. Save results to database
        
        This uses 1 API request per batch instead of 1 per job.
        """
        print(f"\n🔄 Processing batch of {len(jobs)} jobs...")
        print(f"   (Scraping: max {self.max_concurrent} concurrent, Parsing: 1 API call for all)")
        start_time = time.time()
        
        # Step 1: Scrape all jobs concurrently
        scrape_tasks = [self.scrape_single_job(job) for job in jobs]
        scrape_results = await asyncio.gather(*scrape_tasks, return_exceptions=True)
        
        # Separate successful scrapes from failures
        to_parse = []  # (job, content) tuples
        results = []   # Final results
        
        for i, result in enumerate(scrape_results):
            if isinstance(result, Exception):
                save_failed_url(jobs[i].apply_url, str(result))
                results.append(ProcessResult(job=jobs[i], success=False, error=str(result)))
            else:
                job, content, error = result
                if error:
                    save_failed_url(job.apply_url, error)
                    results.append(ProcessResult(job=job, success=False, error=error))
                else:
                    to_parse.append((job, content))
        
        scrape_success = len(to_parse)
        print(f"   ✓ Scraped: {scrape_success}/{len(jobs)} succeeded")
        
        # Step 2: Parse all scraped content in ONE API call
        if to_parse:
            # Build input for batch parser: (job_id, text)
            parse_input = [(str(i), content) for i, (job, content) in enumerate(to_parse)]
            
            print(f"   🤖 Parsing {len(parse_input)} jobs in single API call...")
            parsed_results = parse_job_texts_batch(parse_input)
            
            # Create a lookup by job_id
            parsed_lookup = {str(p.get('job_id', i)): p for i, p in enumerate(parsed_results)}
            
            # Step 3: Save results
            for i, (job, content) in enumerate(to_parse):
                parsed = parsed_lookup.get(str(i))
                
                if not parsed:
                    save_failed_url(job.apply_url, "Parsing failed - no result returned")
                    results.append(ProcessResult(job=job, success=False, error="Parsing failed"))
                    continue
                
                # Enrich with GitHub data
                if not parsed.get('job_title') or parsed.get('job_title') == 'null':
                    parsed['job_title'] = job.role
                if not parsed.get('company') or parsed.get('company') == 'null':
                    parsed['company'] = job.company
                
                parsed['url'] = job.apply_url
                parsed['location'] = job.location
                
                # Remove job_id field before saving (it was just for matching)
                parsed.pop('job_id', None)
                
                # Save to database
                save_job_data(parsed)
                results.append(ProcessResult(job=job, success=True, parsed_data=parsed))
        
        # Update stats
        for result in results:
            self.processed += 1
            if result.success:
                self.succeeded += 1
            else:
                self.failed += 1
        
        elapsed = time.time() - start_time
        success_count = sum(1 for r in results if r.success)
        print(f"   ✓ Batch complete in {elapsed:.1f}s - {success_count}/{len(jobs)} succeeded")
        
        return results
    
    async def process_all(self, jobs: List[JobPosting], batch_size: int = 10) -> List[ProcessResult]:
        """
        Process all jobs in batches.
        
        Args:
            jobs: List of all job postings
            batch_size: Number of jobs per batch
            
        Returns:
            List of all ProcessResult objects
        """
        print(f"\n{'='*60}")
        print(f"Starting batch processing of {len(jobs)} jobs")
        print(f"   Batch size: {batch_size}, Max concurrent: {self.max_concurrent}")
        print(f"{'='*60}")
        
        all_results = []
        total_batches = (len(jobs) + batch_size - 1) // batch_size
        
        for i in range(0, len(jobs), batch_size):
            batch_num = i // batch_size + 1
            batch = jobs[i:i + batch_size]
            
            print(f"\nBatch {batch_num}/{total_batches}")
            results = await self.process_batch(batch)
            all_results.extend(results)
            
            # Delay between batches (except for the last one)
            if i + batch_size < len(jobs) and self.delay_between_batches > 0:
                print(f"   Waiting {self.delay_between_batches}s before next batch...")
                await asyncio.sleep(self.delay_between_batches)
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"FINAL RESULTS")
        print(f"{'='*60}")
        print(f"   Total processed: {self.processed}")
        print(f"   Succeeded: {self.succeeded}")
        print(f"   Failed: {self.failed}")
        print(f"   Success rate: {self.succeeded/self.processed*100:.1f}%" if self.processed > 0 else "N/A")
        
        return all_results


async def run_batch_pipeline(
    limit: int = None,
    batch_size: int = 10,
    max_concurrent: int = 5,
    skip_existing: bool = True,
    skip_failed: bool = True
):
    """
    Main entry point for batch processing jobs from GitHub.
    
    Args:
        limit: Max number of jobs to process (None = all)
        batch_size: Jobs per batch
        max_concurrent: Max concurrent scraping tasks
        skip_existing: If True, skip jobs already in the database
        skip_failed: If True, skip URLs that previously failed to scrape
    """
    # Initialize database
    init_db()
    
    # Fetch ALL job URLs from GitHub (don't limit here)
    print("📥 Fetching jobs from GitHub...")
    jobs = get_job_urls(limit=None)
    
    if not jobs:
        print("No jobs found!")
        return []
    
    print(f"✓ Found {len(jobs)} total job postings")
    
    # Filter out already-processed and failed jobs
    if skip_existing:
        jobs = filter_new_jobs(jobs, skip_failed=skip_failed)
        
        if not jobs:
            print("All jobs have already been processed!")
            print_stats()
            return []
    
    # Apply limit AFTER filtering (so we get `limit` unique new jobs)
    if limit and len(jobs) > limit:
        print(f"📊 Limiting to {limit} new jobs (out of {len(jobs)} available)")
        jobs = jobs[:limit]
    
    # Process in batches
    processor = BatchProcessor(
        max_concurrent=max_concurrent,
        delay_between_batches=2.0
    )
    
    results = await processor.process_all(jobs, batch_size=batch_size)
    
    # Print final stats
    print_stats()
    
    return results


# Test the batch processor
if __name__ == "__main__":
    print("=" * 60)
    print("Batch Processor - Test Run")
    print("=" * 60)
    
    # Run with small limits for testing
    results = asyncio.run(run_batch_pipeline(
        limit=5,  # Only process 5 jobs for testing
        batch_size=3,  # 3 jobs per batch
        max_concurrent=2  # 2 concurrent at a time
    ))
    
    print("\nDetailed Results:")
    for r in results:
        status = "SUCCESS" if r.success else "FAILED"
        print(f"{status} {r.job.company} - {r.job.role}")
        if not r.success:
            print(f"   Error: {r.error}")
        elif r.parsed_data:
            skills = r.parsed_data.get('skills', {})
            total_skills = sum(len(v) for v in skills.values() if isinstance(v, list))
            print(f"   Extracted {total_skills} skills")
