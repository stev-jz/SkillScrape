"""
Streamlit Dashboard for Tech Stack Crawler

Displays job market insights and skill trends from scraped job postings.
"""
import streamlit as st
import pandas as pd
import altair as alt
import sys
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, '.')
from db import get_db_connection, init_db, get_job_categories, categorize_all_jobs, get_top_skills_by_job_category, get_top_skills_filtered

# Page config
st.set_page_config(
    page_title="Tech Stack Crawler",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for Times New Roman font, hide sidebar, and narrow content
st.markdown("""
<style>
    /* Apply Times New Roman to main content only, not popover menus */
    .main, .main *, h1, h2, h3, p, span, div, label {
        font-family: "Times New Roman", Times, serif !important;
    }
    /* Reset font for table column menus/popovers */
    [data-baseweb="popover"], 
    [data-baseweb="popover"] *,
    [data-baseweb="menu"],
    [data-baseweb="menu"] *,
    .stDataFrameGlideDataEditor [role="menu"],
    .stDataFrameGlideDataEditor [role="menu"] * {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif !important;
    }
    [data-testid="stSidebar"] {
        display: none;
    }
    .main .block-container {
        max-width: 800px;
        padding-left: 2rem;
        padding-right: 2rem;
    }
    /* Bold title */
    h1 {
        font-weight: bold !important;
    }
    /* Tighter spacing between metrics only */
    [data-testid="stVerticalBlock"] > [data-testid="stVerticalBlock"] {
        gap: 0.5rem !important;
    }
    /* Code blocks - larger font and all white text */
    pre, code, [data-testid="stCodeBlock"], [data-testid="stCodeBlock"] pre {
        font-size: 1.1rem !important;
    }
    /* Override syntax highlighting to make everything white */
    pre code, pre code span, code span, .highlight, .hljs,
    [data-testid="stCodeBlock"] code, [data-testid="stCodeBlock"] code span,
    [data-testid="stCodeBlock"] pre code, [data-testid="stCodeBlock"] pre code span {
        color: white !important;
    }
    /* Override specific syntax highlighting colors */
    .hljs-keyword, .hljs-string, .hljs-number, .hljs-comment,
    .hljs-function, .hljs-variable, .hljs-operator, .hljs-built_in {
        color: white !important;
    }
    /* Pointer cursor for selectbox dropdown */
    [data-baseweb="select"],
    [data-baseweb="select"] *,
    [data-baseweb="select"] input,
    [data-testid="stSelectbox"],
    [data-testid="stSelectbox"] *,
    div[data-baseweb="select"] > div,
    div[data-baseweb="select"] > div > div {
        cursor: pointer !important;
    }
    /* Force metric labels to wrap into 2 lines */
    [data-testid="stMetricLabel"] {
        white-space: normal !important;
        word-wrap: break-word !important;
        text-overflow: clip !important;
        overflow: visible !important;
        font-size: 1.2rem !important;
    }
    [data-testid="stMetricLabel"] > div {
        text-overflow: clip !important;
        overflow: visible !important;
        white-space: normal !important;
        font-size: 1.2rem !important;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================
# DATABASE QUERY FUNCTIONS
# ============================================================

@st.cache_data(ttl=60)  # Cache for 60 seconds
def get_overview_stats():
    """Get high-level statistics."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as count FROM jobs")
            total_jobs = cur.fetchone()['count']
            
            # Count total occurrences of skills/concepts (from junction table)
            cur.execute("SELECT COUNT(*) as count FROM job_skills")
            total_skills_concepts = cur.fetchone()['count']
            
            cur.execute("""
                SELECT COUNT(*) as count FROM jobs 
                WHERE created_at >= CURRENT_DATE
            """)
            jobs_today = cur.fetchone()['count']
            
            cur.execute("SELECT COUNT(DISTINCT company) as count FROM jobs WHERE company IS NOT NULL")
            total_companies = cur.fetchone()['count']
            
            return {
                'total_jobs': total_jobs,
                'total_skills_concepts': total_skills_concepts,
                'jobs_today': jobs_today,
                'total_companies': total_companies
            }


@st.cache_data(ttl=60)
def get_top_skills(limit=15, category=None):
    """Get most in-demand skills, optionally filtered by category."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if category:
                cur.execute("""
                    SELECT s.name, s.category, COUNT(js.job_id) as job_count
                    FROM skills s
                    JOIN job_skills js ON s.id = js.skill_id
                    WHERE s.category = %s
                    GROUP BY s.id, s.name, s.category
                    ORDER BY job_count DESC
                    LIMIT %s
                """, (category, limit))
            else:
                cur.execute("""
                    SELECT s.name, s.category, COUNT(js.job_id) as job_count
                    FROM skills s
                    JOIN job_skills js ON s.id = js.skill_id
                    GROUP BY s.id, s.name, s.category
                    ORDER BY job_count DESC
                    LIMIT %s
                """, (limit,))
            return cur.fetchall()


@st.cache_data(ttl=60)
def get_skills_by_category():
    """Get skill counts grouped by category."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.category, COUNT(DISTINCT js.job_id) as job_count
                FROM skills s
                JOIN job_skills js ON s.id = js.skill_id
                GROUP BY s.category
                ORDER BY job_count DESC
            """)
            return cur.fetchall()


@st.cache_data(ttl=60)
def get_top_companies(limit=10):
    """Get companies with most job postings."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT company, COUNT(*) as job_count
                FROM jobs
                WHERE company IS NOT NULL
                GROUP BY company
                ORDER BY job_count DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()


@st.cache_data(ttl=60)
def get_recent_jobs(limit=20):
    """Get recently scraped jobs."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT j.id, j.title, j.company, j.url, j.created_at,
                       COALESCE(
                           (SELECT string_agg(s.name, ', ' ORDER BY s.name)
                            FROM job_skills js
                            JOIN skills s ON js.skill_id = s.id
                            WHERE js.job_id = j.id
                            LIMIT 10),
                           'None'
                       ) as skills
                FROM jobs j
                ORDER BY j.created_at DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()


# ============================================================
# DASHBOARD UI
# ============================================================

def main():
    # Initialize database
    init_db()
    
    # Show all content on one page
    show_overview()
    st.markdown("---")
    show_pipeline_info()


def show_overview():
    """Overview dashboard section."""
    st.markdown('<h1><strong>Tech Stack</strong> Crawler</h1>', unsafe_allow_html=True)
    
    # Stats cards
    stats = get_overview_stats()
    
    # Two metrics side by side (not spread across full width)
    metric_col1, metric_col2, spacer = st.columns([1, 1, 2])
    
    with metric_col1:
        st.metric("Jobs scraped", stats['total_jobs'])
    with metric_col2:
        st.metric("Total Skills Scraped", stats['total_skills_concepts'])
    
    # Refresh button below metrics
    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    st.markdown("---")
    
    # Skills Overview section (combined chart and table)
    st.header("Skills Overview")
    
    # Get available skill type categories for filter
    categories_data = get_skills_by_category()
    available_categories = [cat['category'] for cat in categories_data] if categories_data else []
    
    # Create capitalized display options for skill types
    display_options = ["All"] + [cat.capitalize() for cat in available_categories]
    # Map display back to original category (for database queries)
    category_map = {display: orig for display, orig in zip(display_options, ["All"] + available_categories)}
    
    # Get job categories for filter
    job_categories_data = get_job_categories()
    job_category_options = ["All Jobs"] + [cat['category'] for cat in job_categories_data] if job_categories_data else ["All Jobs"]
    
    # Two filter dropdowns side by side
    filter_col1, filter_col2, _ = st.columns([1, 1, 2])
    with filter_col1:
        selected_display = st.selectbox(
            "Filter by skill type:",
            options=display_options,
            index=0,
            key="skills_category_filter"
        )
        selected_category = category_map[selected_display]
    
    with filter_col2:
        selected_job_category = st.selectbox(
            "Filter by job category:",
            options=job_category_options,
            index=0,
            key="job_category_filter"
        )
    
    # Get skills based on BOTH filters for chart (top 10)
    top_skills_chart = get_top_skills_filtered(
        limit=10,
        skill_category=None if selected_category == "All" else selected_category,
        job_category=None if selected_job_category == "All Jobs" else selected_job_category
    )
    
    # Display chart
    if top_skills_chart:
        df_chart = pd.DataFrame(top_skills_chart)
        # Ensure descending order (highest to lowest, left to right)
        df_chart = df_chart.sort_values('job_count', ascending=False, ignore_index=True)
        # Use Altair for custom sort order (descending by job_count)
        chart = alt.Chart(df_chart).mark_bar(color='#8ab4f8').encode(
            x=alt.X('name:N', sort='-y', title=None, axis=alt.Axis(labelAngle=-45, labelOverlap=False, labelLimit=200, labelFontSize=15)),
            y=alt.Y('job_count:Q', title='Job Count', axis=alt.Axis(labelFontSize=14, titleFontSize=16))
        ).properties(
            height=450
        )
        # Make chart narrower and left-aligned
        col1, col2 = st.columns([2, 1])
        with col1:
            st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No skill data yet. Run the pipeline to scrape jobs.")
    
    st.markdown("---")
    
    # Skills list (top 25, same filters)
    top_skills_table = get_top_skills_filtered(
        limit=25,
        skill_category=None if selected_category == "All" else selected_category,
        job_category=None if selected_job_category == "All Jobs" else selected_job_category
    )
    
    if top_skills_table:
        df_table = pd.DataFrame(top_skills_table)
        df_table.columns = ['Skill', 'Category', 'Job Count']
        df_table.index = range(1, len(df_table) + 1)
        # Configure column widths - smaller for index and Job Count
        st.dataframe(
            df_table,
            use_container_width=False,
            column_config={
                "Skill": st.column_config.TextColumn("Skill", width="medium"),
                "Category": st.column_config.TextColumn("Category", width="small"),
                "Job Count": st.column_config.NumberColumn("Job Count", width="small"),
            },
            width=600
        )
    else:
        st.info("No skill data yet.")


def show_pipeline_info():
    """Pipeline information section."""
    st.header("How the Pipeline Works")
    
    st.markdown("""
    The scraping pipeline:
    1. Fetches job URLs from GitHub
    2. Filters out already processed jobs
    3. Scrapes new job postings
    4. Extracts skills using AI
    5. Saves to database
    """)
    
    st.subheader("Terminal Commands")
    
    # Use columns to make code boxes narrower
    cmd_col, _ = st.columns([2, 1])
    
    with cmd_col:
        st.markdown("**Run once** (scrape new jobs):")
        st.code("python src/scheduler.py --max-jobs 10", language="bash")
        
        st.markdown("**View database stats**:")
        st.code("python src/scheduler.py --stats", language="bash")


if __name__ == "__main__":
    main()
