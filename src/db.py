import os
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

# Load environment variables (for local development)
load_dotenv()

# Try Streamlit secrets first (for Streamlit Cloud), fallback to env var (for local)
try:
    import streamlit as st
    DB_URL = st.secrets.get("DB_CONNECTION_STRING", os.getenv("DB_CONNECTION_STRING"))
except Exception:
    DB_URL = os.getenv("DB_CONNECTION_STRING")

# Skill normalization mappings
SKILL_ALIASES = {
    # Languages
    'javascript': 'JavaScript',
    'typescript': 'TypeScript',
    'python': 'Python',
    'java': 'Java',
    'c#': 'C#',
    'c': 'C/C++',
    'c++': 'C/C++',
    'golang': 'Go',
    'go': 'Go',
    'r': 'R',
    # Frameworks/Libraries
    'nodejs': 'Node.js',
    'node.js': 'Node.js',
    'node': 'Node.js',
    'react.js': 'React',
    'reactjs': 'React',
    'vue.js': 'Vue',
    'vuejs': 'Vue',
    'angular.js': 'Angular',
    'angularjs': 'Angular',
    'pytorch': 'PyTorch',
    'tensorflow': 'TensorFlow',
    'scikit-learn': 'scikit-learn',
    'numpy': 'NumPy',
    'pandas': 'pandas',
    # Databases
    'postgresql': 'PostgreSQL',
    'postgres': 'PostgreSQL',
    'mongodb': 'MongoDB',
    'mysql': 'MySQL',
    # Cloud
    'amazon web services': 'AWS',
    'aws': 'AWS',
    'google cloud platform': 'GCP',
    'google cloud': 'GCP',
    'gcp': 'GCP',
    'microsoft azure': 'Azure',
    'azure': 'Azure',
    # Tools
    'git': 'Git',
    'github': 'GitHub',
    'gitlab': 'GitLab',
    'docker': 'Docker',
    'kubernetes': 'Kubernetes',
    'k8s': 'Kubernetes',
    'jira': 'Jira',
    'ci/cd': 'CI/CD',
    'continuous integration': 'CI/CD',
    # OS/Systems
    'linux': 'Linux',
    'unix': 'Unix',
    'bash': 'Bash',
    'powershell': 'PowerShell',
    # Methodologies
    'scrum': 'Scrum',
    'agile': 'Agile',
    # Data Science
    'matlab': 'MATLAB',
}

# Job category classification based on title keywords
# Order matters - first match wins
JOB_CATEGORIES = [
    ('Machine Learning / AI', ['machine learning', 'ml', ' ai ', 'artificial intelligence', 
                               'deep learning', 'llm', 'neural', 'nlp', 'computer vision',
                               'genai', 'gen ai', 'ai agent', 'ai sw', 'ai intern',
                               'computational']),
    ('Data Science', ['data science', 'data scientist', 'data analyst', 'business intelligence', 
                      'analytics', 'data engineering', 'data intern', 'data platform', 
                      'data fabric', 'data management', 'failure analysis data', 'pricing data',
                      'risk analysis']),
    ('Research', ['research', 'scientist', 'phd', 'r&d', 'bell labs']),
    ('Software Engineering', ['software', 'developer', 'swe', 'full stack', 'fullstack', 
                              'frontend', 'backend', 'web', 'mobile', 'ios', 'android', 
                              'engineer', 'engineering', 'programmer', 'coder', 'technology',
                              'digital', 'automation', 'gis', 'gaming', 'video algorithm',
                              'implementation', 'product development', 'product manager',
                              'simulation', 'robotics', 'rpa', 'it ', 'systems', 'wireless',
                              'mes ', 'manufacturing execution', 'industry 4.0',
                              'digitalization', 'dimensional', 'innovation', 'predictive',
                              'language models', 'algorithms', '6g', 'digital twin',
                              'platform', 'adtech', 'd365', 'consulting', 'euv', 'agile',
                              'product associate', 'commerce', 'devops', 'cloud', 'sre', 
                              'infrastructure', 'platform engineer', 'reliability', 
                              'kubernetes', 'aws', 'azure', 'gcp', 'network']),
]

def categorize_job_title(title: str) -> str:
    """
    Categorize a job title based on keywords.
    Returns the category name or 'Other' if no match.
    """
    if not title:
        return 'Other'
    
    title_lower = title.lower()
    
    for category, keywords in JOB_CATEGORIES:
        for keyword in keywords:
            if keyword in title_lower:
                return category
    
    return 'Other'

def normalize_skill(skill_name: str) -> list:
    """
    Normalizes a skill name and splits combined skills.
    Returns a list of normalized skill names.
    """
    skill = skill_name.strip()
    
    # Skip empty skills
    if not skill:
        return []
    
    # Check for known aliases FIRST (handles single-char like C, R)
    if skill.lower() in SKILL_ALIASES:
        return [SKILL_ALIASES[skill.lower()]]
    
    # Skip very short skills that aren't in aliases
    if len(skill) < 2:
        return []
    
    # Note: alias check already done above, this is for the split case
    
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
                
                # Add category column if it doesn't exist (for existing databases)
                cur.execute("""
                    ALTER TABLE jobs ADD COLUMN IF NOT EXISTS category TEXT;
                """)
                
                conn.commit()
        print("PostgreSQL Database initialized successfully.")
    except Exception as e:
        print(f"Init failed: {e}")

def save_job_data(job_data):
    """
    Saves a parsed job to Postgres.
    Skips non-tech jobs based on title analysis.
    """
    job_title = job_data.get('job_title')
    
    # Skip non-tech jobs
    if not is_tech_related_job(job_title):
        print(f"⏭️  Skipping non-tech job: '{job_title}'")
        return
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                # Categorize the job before saving
                category = categorize_job_title(job_title)
                
                # 1. Insert Job (Using ON CONFLICT to ignore duplicates)
                cur.execute("""
                INSERT INTO jobs (title, company, url, raw_skills_data, category) 
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (url) DO UPDATE 
                    SET raw_skills_data = EXCLUDED.raw_skills_data,
                        category = EXCLUDED.category
                RETURNING id;
                """, (
                    job_title, 
                    job_data.get('company'), 
                    job_data.get('url'),
                    psycopg.types.json.Json(job_data),  # Store full JSONB
                    category
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


def categorize_all_jobs():
    """
    Categorize all existing jobs based on their titles.
    This updates the category column for all jobs without re-scraping.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Get all jobs
            cur.execute("SELECT id, title FROM jobs")
            jobs = cur.fetchall()
            
            updated = 0
            for job in jobs:
                category = categorize_job_title(job['title'])
                cur.execute(
                    "UPDATE jobs SET category = %s WHERE id = %s",
                    (category, job['id'])
                )
                updated += 1
            
            conn.commit()
            print(f"✅ Categorized {updated} jobs")
            return updated


def get_job_categories():
    """Get job counts grouped by category."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT category, COUNT(*) as count
                FROM jobs
                WHERE category IS NOT NULL
                GROUP BY category
                ORDER BY count DESC
            """)
            return cur.fetchall()


def get_top_skills_by_job_category(job_category: str, limit: int = 15):
    """
    Get top skills for jobs in a specific job category.
    This filters skills based on which jobs they appear in.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.name, s.category, COUNT(js.job_id) as job_count
                FROM skills s
                JOIN job_skills js ON s.id = js.skill_id
                JOIN jobs j ON js.job_id = j.id
                WHERE j.category = %s
                GROUP BY s.id, s.name, s.category
                ORDER BY job_count DESC
                LIMIT %s
            """, (job_category, limit))
            return cur.fetchall()


def get_top_skills_filtered(limit: int = 15, skill_category: str = None, job_category: str = None):
    """
    Get top skills with optional filtering by both skill category AND job category.
    Both filters work simultaneously when provided.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Build query dynamically based on filters
            query = """
                SELECT s.name, s.category, COUNT(js.job_id) as job_count
                FROM skills s
                JOIN job_skills js ON s.id = js.skill_id
                JOIN jobs j ON js.job_id = j.id
                WHERE 1=1
            """
            params = []
            
            if skill_category:
                query += " AND s.category = %s"
                params.append(skill_category)
            
            if job_category:
                query += " AND j.category = %s"
                params.append(job_category)
            
            query += """
                GROUP BY s.id, s.name, s.category
                ORDER BY job_count DESC
                LIMIT %s
            """
            params.append(limit)
            
            cur.execute(query, params)
            return cur.fetchall()


# Non-tech job patterns to exclude from scraping
NON_TECH_JOB_PATTERNS = [
    'meteorologist', 'weather', 'clinical', 'nurse', 'nursing', 'medical', 'physician',
    'pharmacist', 'environmental permitting', 'storm water', 'wastewater', 
    'grid planning', 'renewable energy', 'power generation', 'nuclear', 
    'earth science', 'geologist', 'chemistry', 'biologist', 'ecology',
    'marketing', 'sales', 'accounting', 'finance analyst', 'hr ', 'human resources',
    'legal', 'attorney', 'paralegal', 'recruiter', 'recruiting',
    'public affairs', 'policy', 'security investigator'
]


def is_tech_related_job(title: str) -> bool:
    """
    Check if a job title is tech-related.
    Returns True for tech jobs, False for non-tech jobs that should be excluded.
    """
    if not title:
        return False
    
    title_lower = title.lower()
    
    # Check if title matches non-tech patterns
    for pattern in NON_TECH_JOB_PATTERNS:
        if pattern in title_lower:
            return False
    
    return True


def delete_non_tech_jobs():
    """
    Delete jobs that are not related to software/tech from the database.
    Returns the count of deleted jobs.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # First, get all jobs to check
            cur.execute("SELECT id, title FROM jobs")
            jobs = cur.fetchall()
            
            deleted_count = 0
            deleted_jobs = []
            
            for job in jobs:
                if not is_tech_related_job(job['title']):
                    deleted_jobs.append(f"  - {job['title']}")
                    cur.execute("DELETE FROM jobs WHERE id = %s", (job['id'],))
                    deleted_count += 1
            
            conn.commit()
            
            if deleted_jobs:
                print(f"🗑️  Deleted {deleted_count} non-tech jobs:")
                for job in deleted_jobs:
                    print(job)
            else:
                print("✅ No non-tech jobs found to delete.")
            
            return deleted_count


def fix_skill_categories():
    """
    Fix miscategorized skills in the database.
    Moves skills to their correct categories.
    """
    # Define corrections: skill_name -> correct_category
    SKILL_CORRECTIONS = {
        # These should be frameworks, not languages
        'Node.js': 'frameworks',
        'node.js': 'frameworks',
        'NodeJS': 'frameworks',
        'Express': 'frameworks',
        'Express.js': 'frameworks',
        '.NET': 'frameworks',
        # These should be tools, not languages
        'Linux': 'tools',
        'Unix': 'tools',
        'Windows': 'tools',
        'Bash': 'tools',
        'Shell': 'tools',
        'PowerShell': 'tools',
    }
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            updated = 0
            for skill_name, correct_category in SKILL_CORRECTIONS.items():
                cur.execute(
                    "UPDATE skills SET category = %s WHERE LOWER(name) = LOWER(%s) AND category != %s",
                    (correct_category, skill_name, correct_category)
                )
                if cur.rowcount > 0:
                    print(f"  ✓ {skill_name} → {correct_category}")
                    updated += cur.rowcount
            
            conn.commit()
            print(f"✅ Fixed {updated} skill categories")
            return updated


if __name__ == "__main__":
    init_db()
    # Also categorize existing jobs when running db.py directly
    categorize_all_jobs()