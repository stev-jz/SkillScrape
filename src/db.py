import os
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_URL = os.getenv("DB_CONNECTION_STRING")

# Skill normalization mappings
SKILL_ALIASES = {
    'javascript': 'JavaScript',
    'typescript': 'TypeScript',
    'python': 'Python',
    'java': 'Java',
    'c#': 'C#',
    'c++': 'C++',
    'golang': 'Go',
    'nodejs': 'Node.js',
    'node.js': 'Node.js',
    'react.js': 'React',
    'reactjs': 'React',
    'vue.js': 'Vue',
    'vuejs': 'Vue',
    'angular.js': 'Angular',
    'angularjs': 'Angular',
    'postgresql': 'PostgreSQL',
    'postgres': 'PostgreSQL',
    'mongodb': 'MongoDB',
    'mysql': 'MySQL',
    'amazon web services': 'AWS',
    'google cloud platform': 'GCP',
    'google cloud': 'GCP',
    'microsoft azure': 'Azure',
    'ci/cd': 'CI/CD',
    'continuous integration': 'CI/CD',
}

def normalize_skill(skill_name: str) -> list:
    """
    Normalizes a skill name and splits combined skills.
    Returns a list of normalized skill names.
    """
    skill = skill_name.strip()
    
    # Skip empty or very short skills
    if len(skill) < 2:
        return []
    
    # Check for known aliases FIRST (before splitting)
    # This preserves skills like "CI/CD" as a single unit
    if skill.lower() in SKILL_ALIASES:
        return [SKILL_ALIASES[skill.lower()]]
    
    # Skip vague/non-technical skills
    skip_terms = ['problem solving', 'communication', 'teamwork', 'fast-paced', 
                  'self-starter', 'detail-oriented', 'passionate', 'motivated',
                  'excellent', 'strong', 'good', 'ability to', 'experience with']
    if any(term in skill.lower() for term in skip_terms):
        return []
    
    # Keep compound technical terms as single skills
    keep_as_single = ['data structures', 'algorithms', 'data structures & algorithms',
                      'data structures and algorithms', 'object oriented', 
                      'machine learning', 'deep learning', 'computer vision',
                      'natural language processing', 'distributed systems']
    if any(term in skill.lower() for term in keep_as_single):
        return [skill]
    
    # Split combined skills like "C/C++", "React/Vue", "Python/Java"
    if '/' in skill and len(skill) < 20:  # Only split short combined skills
        parts = [p.strip() for p in skill.split('/')]
        result = []
        for part in parts:
            if len(part) >= 2:
                normalized = SKILL_ALIASES.get(part.lower(), part)
                result.append(normalized)
        return result if result else [skill]
    
    # Return as-is if no special handling needed
    return [skill]

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg.connect(DB_URL, row_factory=dict_row)
        return conn
    except Exception as e:
        print(f"DB Connection Failed {e}")
        raise e

def init_db():
    """Creates the necessary tables in PostgreSQL."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 1. JOBS TABLE (With JSONB support)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id SERIAL PRIMARY KEY,
                    title TEXT,
                    company TEXT,
                    url TEXT UNIQUE,
                    raw_skills_data JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                
                # 2. SKILLS TABLE
                cur.execute("""
                CREATE TABLE IF NOT EXISTS skills (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE,
                    category TEXT
                );
                """)
                
                # 3. JOB_SKILLS (Junction Table)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS job_skills (
                    job_id INTEGER REFERENCES jobs(id) ON DELETE CASCADE,
                    skill_id INTEGER REFERENCES skills(id) ON DELETE CASCADE,
                    PRIMARY KEY (job_id, skill_id)
                );
                """)
                
                # 4. FAILED_URLS (Track URLs that failed to scrape)
                cur.execute("""
                CREATE TABLE IF NOT EXISTS failed_urls (
                    id SERIAL PRIMARY KEY,
                    url TEXT UNIQUE,
                    error TEXT,
                    attempts INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """)
                
                # Index for performance
                cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_skills_gin ON jobs USING GIN (raw_skills_data);")
                
                conn.commit()
        print("PostgreSQL Database initialized successfully.")
    except Exception as e:
        print(f"Init failed: {e}")

def save_job_data(job_data):
    """
    Saves a parsed job to Postgres.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                # 1. Insert Job (Using ON CONFLICT to ignore duplicates)
                cur.execute("""
                INSERT INTO jobs (title, company, url, raw_skills_data) 
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (url) DO UPDATE 
                    SET raw_skills_data = EXCLUDED.raw_skills_data
                RETURNING id;
                """, (
                    job_data.get('job_title'), 
                    job_data.get('company'), 
                    job_data.get('url'),
                    psycopg.types.json.Json(job_data) # Store full JSONB
                ))
                
                # Handle case where job already existed and we just updated it
                result = cur.fetchone()
                if not result:
                    # If no row returned, we need to fetch the ID manually
                    cur.execute("SELECT id FROM jobs WHERE url = %s", (job_data['url'],))
                    job_id = cur.fetchone()['id']
                else:
                    job_id = result['id']

                # 2. Process Relational Skills (For Clustering)
                all_skills = job_data.get('skills', {})
                
                for category, skill_list in all_skills.items():
                    for skill_name in skill_list:
                        # Normalize and split combined skills
                        normalized_skills = normalize_skill(skill_name)
                        
                        for clean_name in normalized_skills:
                            # Insert Skill if new
                            cur.execute("""
                            INSERT INTO skills (name, category) 
                            VALUES (%s, %s)
                            ON CONFLICT (name) DO NOTHING
                            RETURNING id;
                            """, (clean_name, category))
                            
                            skill_res = cur.fetchone()
                            if skill_res:
                                skill_id = skill_res['id']
                            else:
                                cur.execute("SELECT id FROM skills WHERE name = %s", (clean_name,))
                                skill_id = cur.fetchone()['id']
                            
                            # Link Job <-> Skill
                            cur.execute("""
                            INSERT INTO job_skills (job_id, skill_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING;
                            """, (job_id, skill_id))
                
                conn.commit()
                print(f"💾 Saved job '{job_data.get('job_title')}' to Postgres.")
                
            except Exception as e:
                conn.rollback()
                print(f"Database Error: {e}")

def save_failed_url(url: str, error: str):
    """
    Save a URL that failed to scrape so we can skip it in future runs.
    If the URL already exists, increment the attempt counter.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO failed_urls (url, error) 
                VALUES (%s, %s)
                ON CONFLICT (url) DO UPDATE 
                    SET attempts = failed_urls.attempts + 1,
                        error = EXCLUDED.error,
                        last_attempt = CURRENT_TIMESTAMP
            """, (url, error))
            conn.commit()


def get_failed_urls() -> set:
    """
    Get all URLs that have failed to scrape.
    
    Returns:
        Set of URLs that failed
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT url FROM failed_urls")
            rows = cur.fetchall()
            return {row['url'] for row in rows}


def clear_failed_urls():
    """Clear all failed URLs (useful for retrying everything)."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM failed_urls")
            conn.commit()
            print("Cleared all failed URLs")


if __name__ == "__main__":
    init_db()