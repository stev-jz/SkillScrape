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
    You are an expert Tech Recruiter. Your goal is to be EXHAUSTIVE. 
    If a tool or skill is mentioned in the text, it MUST be extracted.

    Strategy:
    1. Look specifically for sections labeled "Requirements", "Qualifications", or "Stack".
    2. "Tools" includes: DevOps (Docker), Source Control (Git, GitHub), Project Management (Jira, Confluence), and Cloud (AWS).
    3. Do not ignore "Soft Technical" skills like Agile, Scrum, or SDLC.

    Return ONLY a JSON object with this exact schema:
    {{
      "job_title": "Extract the likely job title",
      "company": "Extract company name if present, else null",
      "skills": {{
        "languages": ["Programming languages e.g. Python, Java, TypeScript"],
        "frameworks": ["Frameworks/Libraries e.g. React, Spring Boot, .NET"],
        "databases": ["Databases e.g. PostgreSQL, MongoDB, Redis"],
        "tools": ["ALL tools: Git, Jira, Docker, AWS, Kubernetes, Jenkins, Excel"],
        "concepts": ["Methodologies e.g. Agile, OOP, CI/CD, Distributed Systems"]
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