import json
import os
import psycopg2
from datetime import datetime
from dateutil import parser as date_parser
import openai
from openai import OpenAI
from dotenv import load_dotenv
import decimal

# Load environment variables from .env file
load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=openai_api_key)

# PostgreSQL connection parameters from environment variables
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# -----------------------
# Custom JSON Encoder for Decimal
# -----------------------
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# -----------------------
# DATABASE CONNECTION
# -----------------------
def get_pg_connection():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )
    return conn

# -----------------------
# SCHEMA CREATION
# -----------------------
def create_schema(conn):
    """
    Creates the posts and comments tables in PostgreSQL if they do not exist.
    """
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        id SERIAL PRIMARY KEY,
        source TEXT,
        title TEXT,
        created_at TEXT,
        asin TEXT,
        subreddit TEXT,
        url TEXT,
        description TEXT,
        channel_name TEXT,
        country_of_origin TEXT,
        price REAL,
        currency TEXT,
        star_ratings TEXT,
        total_rating INTEGER,
        raw_json TEXT
    );
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comments (
        id SERIAL PRIMARY KEY,
        post_id INTEGER REFERENCES posts(id),
        author_name TEXT,
        content TEXT,
        rating REAL,
        helpful_votes TEXT,
        karma INTEGER,
        created_at TEXT,
        age_group TEXT,
        gender TEXT,
        income_band TEXT
    );
    """)
    
    conn.commit()
    cursor.close()

# -----------------------
# HELPER FUNCTIONS
# -----------------------
def parse_float(val):
    if val is None:
        return None
    try:
        val_clean = str(val).replace(",", "")
        return float(val_clean)
    except ValueError:
        return None

def parse_int(val):
    if val is None:
        return None
    try:
        return int(val)
    except ValueError:
        return None

def parse_date(date_str):
    if not date_str:
        return None
    try:
        if " on " in date_str:
            parts = date_str.split(" on ", 1)
            date_str = parts[-1]
        dt = date_parser.parse(date_str.strip(), fuzzy=True)
        return dt.strftime('%Y-%m-%d')
    except Exception:
        return date_str

# -----------------------
# DATABASE INSERTION FUNCTIONS
# -----------------------
def insert_post(conn, source, title, created_at, asin, subreddit, url, description,
                channel_name, country_of_origin, price, currency, star_ratings, 
                total_rating, raw_json):
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO posts (source, title, created_at, asin, subreddit, url, description,
                       channel_name, country_of_origin, price, currency, star_ratings,
                       total_rating, raw_json)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id;
    """, (
        source, title, created_at, asin, subreddit, url, description, channel_name,
        country_of_origin, price, currency, star_ratings, total_rating, raw_json
    ))
    post_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    return post_id

def insert_comment(conn, post_id, author_name, content, rating, helpful_votes,
                   karma, created_at, age_group, gender, income_band):
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO comments (post_id, author_name, content, rating, helpful_votes,
                          karma, created_at, age_group, gender, income_band)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, (
        post_id, author_name, content, rating, helpful_votes, karma, created_at,
        age_group, gender, income_band
    ))
    conn.commit()
    cursor.close()

# -----------------------
# INGESTION LOGIC
# -----------------------
def ingest_record(conn, record):
    source = record.get("source", "unknown")
    raw_json_str = json.dumps(record, ensure_ascii=False)
    
    title = None
    created_at = None
    asin = None
    subreddit = None
    url = None
    description = None
    channel_name = None
    country_of_origin = None
    price = None
    currency = None
    star_ratings = None
    total_rating = None
    
    if "amazon" in source.lower():
        asin = record.get("asin")
        product_details = record.get("product_details", {})
        title = product_details.get("title")
        country_of_origin = product_details.get("Country of Origin")
        price = parse_float(product_details.get("price"))
        currency = product_details.get("currency")
        star_ratings = product_details.get("star_ratings")
        total_rating = parse_int(product_details.get("total_rating"))
    
    elif "reddit" in source.lower():
        subreddit = record.get("subreddit")
        created_at = parse_date(record.get("created_at"))
        title = record.get("content")
    
    elif "youtube" in source.lower():
        title = record.get("title")
        url = record.get("url")
        channel_name = record.get("channel_name")
        created_at = parse_date(record.get("published_at"))
        description = record.get("description")
    
    post_id = insert_post(
        conn,
        source=source,
        title=title,
        created_at=created_at,
        asin=asin,
        subreddit=subreddit,
        url=url,
        description=description,
        channel_name=channel_name,
        country_of_origin=country_of_origin,
        price=price,
        currency=currency,
        star_ratings=star_ratings,
        total_rating=total_rating,
        raw_json=raw_json_str
    )
    
    comments = []
    if "reviews" in record:
        comments = record["reviews"]
    elif "comments" in record:
        comments = record["comments"]
    
    for c in comments:
        author_name = None
        content = None
        rating = None
        helpful_votes = None
        karma = None
        comment_created_at = None
        user_info = c.get("user_info") or {}
        age_group = user_info.get("age_group")
        gender = user_info.get("gender")
        income_band = user_info.get("income_band")
        
        if "review_author" in c:
            author_name = c.get("review_author")
            content = c.get("content")
            rating = parse_float(c.get("review_star_rating"))
            helpful_votes = c.get("helpful_vote_statement")
            comment_created_at = parse_date(c.get("review_date"))
        elif "body" in c:
            content = c.get("body")
            author_name = c.get("author")
            karma = parse_int(c.get("karma"))
            comment_created_at = parse_date(c.get("created_at"))
        elif "text" in c:
            content = c.get("text")
            author_name = c.get("author_name")
            comment_created_at = c.get("time")
        
        insert_comment(
            conn,
            post_id=post_id,
            author_name=author_name,
            content=content,
            rating=rating,
            helpful_votes=helpful_votes,
            karma=karma,
            created_at=comment_created_at,
            age_group=age_group,
            gender=gender,
            income_band=income_band
        )

def ingest_json_file(conn, json_file_path):
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"JSON file not found: {json_file_path}")
    
    with open(json_file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    if not isinstance(data, list):
        raise ValueError("Top-level JSON structure must be a list of records.")
    
    for record in data:
        ingest_record(conn, record)

# -----------------------
# SQL & LLM FUNCTIONS
# -----------------------
def execute_sql_query(query):
    conn = get_pg_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    conn.close()
    results = [dict(zip(columns, row)) for row in rows]
    return results

def generate_sql_from_nl(user_query, schema_description):
    prompt = f"""
You are an SQL expert. Given the following database schema:

TABLE posts:
  id (SERIAL, primary key)
  source (TEXT) -- Example: 'amazon_reviews_1', 'reddit_1', 'youtube_1'
  title (TEXT)
  created_at (TEXT) -- Stored as a string in the format 'YYYY-MM-DD'
  asin (TEXT)
  subreddit (TEXT)
  url (TEXT)
  description (TEXT)
  channel_name (TEXT)
  country_of_origin (TEXT)
  price (REAL)
  currency (TEXT)
  star_ratings (TEXT)
  total_rating (INTEGER)
  raw_json (TEXT)

TABLE comments:
  id (SERIAL, primary key)
  post_id (INTEGER, foreign key referencing posts(id))
  author_name (TEXT)
  content (TEXT)
  rating (REAL)
  helpful_votes (TEXT)
  karma (INTEGER)
  created_at (TEXT)
  age_group (TEXT)
  gender (TEXT)
  income_band (TEXT)

Additional Instructions:
- When generating queries referring to source, use pattern matching (e.g., "WHERE source LIKE '%amazon%'") rather than exact equality.
- Note: The 'created_at' field is stored as a string in the format 'YYYY-MM-DD'. Please cast it using posts.created_at::date.

Now, generate a SQL query that answers the following question:
\"{user_query}\"

Only output the SQL query.
"""
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=300,
    )
    sql_query = response.choices[0].message.content.strip()
    return sql_query

def explain_results(results, user_query):
    prompt = f"""
The following are the results from a database query:

{json.dumps(results, indent=2, cls=DecimalEncoder)}

Based on the user question:
\"{user_query}\"

Provide a clear and concise explanation of these results. Be as specific as possible.
"""
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
        max_tokens=300,
    )
    explanation = response.choices[0].message.content.strip()
    return explanation

# -----------------------
# MAIN EXECUTION
# -----------------------
def main():
    # Connect to PostgreSQL database and create schema
    conn = get_pg_connection()
    create_schema(conn)
    
    # Path to your JSON data file (adjust as needed)
    json_file_path = "sample_data.json"
    
    # Ingest the JSON data into the database
    ingest_json_file(conn, json_file_path)
    conn.close()  # Close ingestion connection
    
    # Accept a user query to analyze the data
    user_query = ("What is the distribution of review counts by country of origin for Amazon products?").strip()
    
    # Schema description for context
    schema_description = """
TABLE posts:
  id, source, title, created_at, asin, subreddit, url, description, channel_name, country_of_origin, price, currency, star_ratings, total_rating, raw_json

TABLE comments:
  id, post_id, author_name, content, rating, helpful_votes, karma, created_at, age_group, gender, income_band
    """
    
    # Generate SQL query from natural language using the LLM
    print("\nGenerating SQL query from your natural language input...")
    sql_query = generate_sql_from_nl(user_query, schema_description)
    print("\nGenerated SQL Query:")
    print(sql_query)
    
    # Execute the SQL query
    results = execute_sql_query(sql_query)
    print("\nQuery Results:")
    print(json.dumps(results, indent=2, cls=DecimalEncoder))
    
    # Ask the LLM to explain the results
    explanation = explain_results(results, user_query)
    print("\nExplanation:")
    print(explanation)

if __name__ == "__main__":
    main()
