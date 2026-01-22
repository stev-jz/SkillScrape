import os
import json
from dotenv import load_dotenv
from google import genai


# 1. Load environment variables
load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")

if not api_key:
    raise ValueError("GOOGLE_API_KEY not found in .env file!")

# 2. Configure Gemini
client = genai.Client(api_key=api_key)

def parse_job_text(raw_text):
    """
    Sends raw job description text to Gemini and extracts structured skills.
    """


    prompt = f"""
    You are an expert Tech Recruiter extracting SPECIFIC technical skills.

    RULES:
    1. Only extract CONCRETE, SPECIFIC skills - not vague descriptions.
    2. Split combined skills: "C/C++" should become ["C", "C++"], "React/Vue" becomes ["React", "Vue"].
    3. Use STANDARD names: "Python" not "python programming", "AWS" not "Amazon Web Services".
    4. For concepts: ONLY include well-known methodologies (Agile, Scrum, CI/CD, OOP, REST, GraphQL).
       DO NOT include vague terms like "problem solving", "communication", "teamwork", "fast-paced".
    5. Limit concepts to MAX 5 most important ones.

    Return ONLY a JSON object with this exact schema:
    {{
      "job_title": "Extract the likely job title",
      "company": "Extract company name if present, else null",
      "skills": {{
        "languages": ["Programming languages: Python, Java, C++, JavaScript, etc."],
        "frameworks": ["Frameworks/Libraries: React, Spring Boot, Django, .NET, etc."],
        "databases": ["Databases: PostgreSQL, MongoDB, Redis, MySQL, etc."],
        "tools": ["Tools: Git, Docker, AWS, Kubernetes, Jenkins, Jira, etc."],
        "concepts": ["ONLY well-known methodologies: Agile, CI/CD, OOP, REST, etc. MAX 5"]
      }}
    }}

    JOB DESCRIPTION:
    {raw_text}
    """

    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash-lite', 
            contents=prompt
        )
        
        cleaned_text = response.text.strip()
        
        # Handle cases where Gemini might still add markdown backticks
        if cleaned_text.startswith("```json"):
            cleaned_text = cleaned_text[7:]
        if cleaned_text.endswith("```"):
            cleaned_text = cleaned_text[:-3]
            
        return json.loads(cleaned_text)
        
    except Exception as e:
        print(f"Error parsing with Gemini: {e}")
        return None


def parse_job_texts_batch(job_texts: list[tuple[str, str]]) -> list[dict]:
    """
    Parse multiple job descriptions in a SINGLE API call.
    This maximizes your requests-per-day limit.
    
    Args:
        job_texts: List of tuples (job_id, raw_text) where job_id is used to match results
        
    Returns:
        List of parsed job dicts with 'job_id' field to match back to original
    """
    if not job_texts:
        return []
    
    # Build the batch prompt
    jobs_section = ""
    for i, (job_id, raw_text) in enumerate(job_texts):
        # Truncate very long descriptions to avoid token limits
        truncated = raw_text[:8000] if len(raw_text) > 8000 else raw_text
        jobs_section += f"""
---JOB {i+1} (ID: {job_id})---
{truncated}
"""

    prompt = f"""
You are an expert Tech Recruiter extracting SPECIFIC technical skills from multiple job postings.

RULES:
1. Only extract CONCRETE, SPECIFIC skills - not vague descriptions.
2. Split combined skills: "C/C++" should become ["C", "C++"], "React/Vue" becomes ["React", "Vue"].
3. Use STANDARD names: "Python" not "python programming", "AWS" not "Amazon Web Services".
4. For concepts: ONLY include well-known methodologies (Agile, Scrum, CI/CD, OOP, REST, GraphQL).
   DO NOT include vague terms like "problem solving", "communication", "teamwork", "fast-paced".
5. Limit concepts to MAX 5 most important ones per job.

Return ONLY a JSON array with one object per job, in the same order as input:
[
  {{
    "job_id": "the ID from the input",
    "job_title": "Extract the likely job title",
    "company": "Extract company name if present, else null",
    "skills": {{
      "languages": ["Programming languages"],
      "frameworks": ["Frameworks/Libraries"],
      "databases": ["Databases"],
      "tools": ["Tools"],
      "concepts": ["Methodologies, MAX 5"]
    }}
  }},
  ...
]

HERE ARE {len(job_texts)} JOB DESCRIPTIONS TO PARSE:
{jobs_section}
"""

    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash-lite', 
            contents=prompt
        )
        
        cleaned_text = response.text.strip()
        
        # Handle markdown backticks
        if cleaned_text.startswith("```json"):
            cleaned_text = cleaned_text[7:]
        elif cleaned_text.startswith("```"):
            cleaned_text = cleaned_text[3:]
        if cleaned_text.endswith("```"):
            cleaned_text = cleaned_text[:-3]
        
        results = json.loads(cleaned_text.strip())
        
        # Ensure it's a list
        if isinstance(results, dict):
            results = [results]
            
        return results
        
    except Exception as e:
        print(f"Error batch parsing with Gemini: {e}")
        return []


# TEST BLOCK
if __name__ == "__main__":
    # Load the data we scraped in the previous step
    # Make sure this file actually exists from your scraper run!
    test_file_path = "data/raw/TD_test_posting.md"
    
    if os.path.exists(test_file_path):
        with open(test_file_path, "r", encoding="utf-8") as f:
            raw_data = f.read()
            
        print("Sending data to Gemini...")
        result = parse_job_text(raw_data)
        
        print("\nEXTRACTED SKILLS JSON:")
        print(json.dumps(result, indent=2))
    else:
        print("No raw data found. Run src/scraper.py first, or create a dummy file to test.")