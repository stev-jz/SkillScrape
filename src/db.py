import os
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_URL = os.getenv("DB_CONNECTION_STRING")

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
                        clean_name = skill_name.strip()
                        
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

if __name__ == "__main__":
    init_db()