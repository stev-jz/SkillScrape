"""
GitHub README Job Scraper

Extracts job posting URLs from the SimplifyJobs Summer2026-Internships repo.
Uses the raw GitHub URL to fetch the README directly (no JavaScript needed!).
"""
import re
import requests
from typing import List, Dict
from dataclasses import dataclass

# Raw GitHub URL for the README (bypasses the web interface)
GITHUB_RAW_URL = "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/dev/README.md"


@dataclass
class JobPosting:
    """Represents a job posting extracted from the GitHub README."""
    company: str
    role: str
    location: str
    apply_url: str


def fetch_readme(url: str = GITHUB_RAW_URL) -> str:
    """
    Fetches the raw README content from GitHub.
    
    Returns:
        The raw markdown content of the README.
    """
    print(f"Fetching README from: {url}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    print(f"Fetched {len(response.text):,} characters")
    return response.text


def extract_job_urls(readme_content: str) -> List[JobPosting]:
    """
    Extracts job posting information from the README.
    
    The README uses HTML tables with this format:
    <tr>
      <td><strong><a href="company_url">Company Name</a></strong></td>
      <td>Role</td>
      <td>Location</td>
      <td><div align="center"><a href="apply_url"><img...></a></div></td>
      <td>Age</td>
    </tr>
    
    Returns:
        List of JobPosting objects with company, role, location, and apply URL.
    """
    jobs = []
    current_company = None
    
    # Pattern to match each table row
    # Captures: company name, role, location, and apply URL
    row_pattern = re.compile(
        r'<tr>\s*'
        r'<td>(?:<strong>)?(?:<a[^>]*>)?([^<]+)(?:</a>)?(?:</strong>)?</td>\s*'  # Company or ↳
        r'<td>([^<]+)</td>\s*'  # Role
        r'<td>([^<]+)</td>\s*'  # Location
        r'<td>.*?<a\s+href="([^"]+)".*?</td>',  # Apply URL
        re.DOTALL
    )
    
    # Also match rows where company cell might contain HTML link
    for match in row_pattern.finditer(readme_content):
        company_cell = match.group(1).strip()
        role = match.group(2).strip()
        location = match.group(3).strip()
        apply_url = match.group(4).strip()
        
        # Handle sub-listings (↳ means same company as previous)
        if company_cell == '↳':
            company = current_company or "Unknown"
        else:
            company = company_cell
            current_company = company
        
        # Clean up role (remove emoji badges)
        role = re.sub(r'[🎓🔥🛂🇺🇸🔒]+', '', role).strip()
        
        # Skip if URL doesn't look like a job application
        if 'simplify.jobs' in apply_url or 'github.com' in apply_url:
            continue
        
        jobs.append(JobPosting(
            company=company,
            role=role,
            location=location,
            apply_url=apply_url
        ))
    
    return jobs


def get_job_urls(limit: int = None) -> List[JobPosting]:
    """
    Main function to fetch and extract job URLs from GitHub.
    
    Args:
        limit: Optional limit on number of jobs to return (for testing)
    
    Returns:
        List of JobPosting objects
    """
    readme = fetch_readme()
    jobs = extract_job_urls(readme)
    
    print(f"Extracted {len(jobs)} job postings")
    
    if limit:
        jobs = jobs[:limit]
        print(f"  (Limited to first {limit} for testing)")
    
    return jobs


# Test the scraper
if __name__ == "__main__":
    print("=" * 60)
    print("GitHub Job URL Extractor - Test Run")
    print("=" * 60)
    
    # Fetch and extract (limit to 10 for testing)
    jobs = get_job_urls(limit=10)
    
    print("\nSample Job Postings:")
    print("-" * 60)
    for i, job in enumerate(jobs, 1):
        print(f"{i}. {job.company}")
        print(f"   Role: {job.role}")
        print(f"   Location: {job.location}")
        print(f"   URL: {job.apply_url}")
        print()
