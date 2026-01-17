import asyncio
import os
from crawl4ai import AsyncWebCrawler

async def scrape_page(url):
    print(f"Scraping: {url}")
    
    # Initialize crawler
    async with AsyncWebCrawler(verbose=True) as crawler:
        result = await crawler.arun(url=url)
        
        # We want the "markdown" version because it's easier for AI to read later
        return result.markdown

def save_raw_data(filename, data):
    # Ensure the directory exists
    os.makedirs("data/raw", exist_ok=True)
    
    filepath = os.path.join("data/raw", filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(data)
    print(f"Saved raw data to: {filepath}")

async def main():
    # TEST URL: We'll test with a real YC startup job or a stable page
    # You can change this URL to any specific job posting later
    target_url = "https://www.ycombinator.com/jobs" 
    
    raw_text = await scrape_page(target_url)
    
    # Save it with a timestamp or ID (using 'test_run' for now)
    save_raw_data("yc_jobs_test.md", raw_text)

if __name__ == "__main__":
    asyncio.run(main())