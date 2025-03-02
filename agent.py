"""
Data Agent: ETL and Query System using PostgreSQL and Generative AI

This module implements an intelligent ETL (Extract, Transform, Load) Data Agent that:
  - Reads a JSON file containing social media posts from multiple platforms.
  - Ingests and transforms the data into a standardized schema.
  - Stores the data in a PostgreSQL database.
  - Uses an LLM (via OpenAI API) to generate SQL queries from natural language and to explain query results.

Key Features:
  - Selection: Only extracts fields present in the source JSON.
  - Projection: Maps platform-specific fields (e.g., Amazon product details, Reddit content, YouTube video details) to a common schema.
  - Transformation: Standardizes dates and numeric values using helper functions.
  - LLM Integration: Generates SQL queries and explanations without transforming individual records.
  - Adaptability: Modular design that facilitates changes if new data sources or schema changes occur.

Note: The 'created_at' field is stored as a string in the format 'YYYY-MM-DD'.

Dependencies:
  - psycopg2-binary: For PostgreSQL database connectivity.
  - python-dateutil: For flexible date parsing.
  - openai: For interacting with the OpenAI API.
  - python-dotenv: For loading environment variables from a .env file.
  - decimal: To handle Decimal objects returned by PostgreSQL.

Author: [Your Name]
Date: [Current Date]
"""

import json
import os
import psycopg2
from datetime import datetime
from dateutil import parser as date_parser
import openai
from openai import OpenAI
from dotenv import load_dotenv
import decimal

# -----------------------
# Environment & OpenAI Setup
# -----------------------
# Load environment variables from the .env file.
load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=openai_api_key)

# PostgreSQL connection parameters are loaded from environment variables.
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# -----------------------
# Custom JSON Encoder for Decimal Objects
# -----------------------
class DecimalEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that converts Decimal objects (returned by PostgreSQL)
    into floats so that they can be serialized to JSON.
    """
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# -----------------------
# DATABASE CONNECTION
# -----------------------
def get_pg_connection():
    """
    Establishes and returns a connection to the PostgreSQL database using
    connection parameters from the environment variables.
    
    Returns:
        psycopg2.extensions.connection: The PostgreSQL connection object.
    """
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
    Creates the necessary tables in the PostgreSQL database if they do not already exist.
    
    Tables:
      - posts: Stores top-level post/product data.
      - comments: Stores reviews/comments associated with posts.
    
    Args:
        conn (psycopg2.extensions.connection): The database connection.
    """
    cursor = conn.cursor()
    
    # Create posts table with columns for each required field.
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
    
    # Create comments table with a foreign key referencing posts.
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
# HELPER FUNCTIONS FOR DATA TRANSFORMATION
# -----------------------
def parse_float(val):
    """
    Converts a given value to float after removing commas.
    
    Args:
        val (str or number): The value to convert.
    
    Returns:
        float or None: The converted float value, or None if conversion fails.
    """
    if val is None:
        return None
    try:
        val_clean = str(val).replace(",", "")
        return float(val_clean)
    except ValueError:
        return None

def parse_int(val):
    """
    Converts a given value to an integer.
    
    Args:
        val (str or number): The value to convert.
    
    Returns:
        int or None: The converted integer, or None if conversion fails.
    """
    if val is None:
        return None
    try:
        return int(val)
    except ValueError:
        return None

def parse_date(date_str):
    """
    Parses a date/time string using dateutil and returns the date in the format 'YYYY-MM-DD'.
    
    Args:
        date_str (str): The date string to parse.
    
    Returns:
        str or None: The formatted date string, or the original string if parsing fails.
    """
    if not date_str:
        return None
    try:
        # Remove common leading text (e.g., "Reviewed in India on ")
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
    """
    Inserts a single post record into the posts table and returns its generated id.
    
    Args:
        conn (psycopg2.extensions.connection): Database connection.
        (Other parameters correspond to columns in the posts table.)
    
    Returns:
        int: The newly generated post id.
    """
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
    """
    Inserts a single comment/review record into the comments table.
    
    Args:
        conn (psycopg2.extensions.connection): Database connection.
        post_id (int): The id of the associated post.
        (Other parameters correspond to columns in the comments table.)
    """
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
    """
    Processes a single record from the JSON file. Based on the 'source' field,
    it maps the record to the standardized schema and inserts data into the posts
    table. It then processes nested reviews/comments and inserts them into the comments table.
    
    Args:
        conn (psycopg2.extensions.connection): Database connection.
        record (dict): A JSON object representing a social media post.
    """
    source = record.get("source", "unknown")
    raw_json_str = json.dumps(record, ensure_ascii=False)
    
    # Initialize variables for post fields.
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
    
    # Platform-specific mapping.
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
    
    # Insert the post record.
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
    
    # Determine which key holds nested reviews or comments.
    comments = []
    if "reviews" in record:
        comments = record["reviews"]
    elif "comments" in record:
        comments = record["comments"]
    
    # Process and insert each comment/review.
    for c in comments:
        author_name = None
        content = None
        rating = None
        helpful_votes = None
        karma = None
        comment_created_at = None
        
        # Safely extract user_info fields.
        user_info = c.get("user_info") or {}
        age_group = user_info.get("age_group")
        gender = user_info.get("gender")
        income_band = user_info.get("income_band")
        
        # Mapping for different platforms' comment fields.
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
        
        # Insert the comment record.
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
    """
    Loads the JSON file and ingests each record into the database.
    
    Args:
        conn (psycopg2.extensions.connection): Database connection.
        json_file_path (str): Path to the JSON file.
    
    Raises:
        FileNotFoundError: If the JSON file is not found.
        ValueError: If the JSON file's top-level structure is not a list.
    """
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
    """
    Executes the given SQL query on the PostgreSQL database and returns the results.
    
    Args:
        query (str): The SQL query to execute.
    
    Returns:
        list of dict: Query results with column names as keys.
    """
    conn = get_pg_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    conn.close()
    results = [dict(zip(columns, row)) for row in rows]
    return results

def generate_sql_from_nl(user_query, schema_description):
    """
    Uses the OpenAI API to generate a SQL query from a natural language query.
    
    Args:
        user_query (str): The natural language query.
        schema_description (str): Description of the database schema.
    
    Returns:
        str: The generated SQL query.
    
    Additional Instructions in the prompt:
      - Use pattern matching for the source field.
      - The 'created_at' field is stored as a string in the format 'YYYY-MM-DD'. Cast it using posts.created_at::date.
    """
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
    """
    Uses the OpenAI API to generate a human-friendly explanation of SQL query results.
    
    Args:
        results (list of dict): The results from executing the SQL query.
        user_query (str): The original natural language query.
    
    Returns:
        str: A clear and concise explanation of the query results.
    """
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
    # Connect to PostgreSQL database and create schema.
    conn = get_pg_connection()
    create_schema(conn)
    
    # Path to your JSON data file (update as needed).
    json_file_path = "sample_data.json"
    
    # Ingest the JSON data into the database.
    ingest_json_file(conn, json_file_path)
    conn.close()  # Close ingestion connection.
    
    # Accept a user query to analyze the data.
    user_query = ("What is the distribution of review counts by country of origin for Amazon products?").strip()
    
    # Schema description for context (optional for LLM prompt).
    schema_description = """
TABLE posts:
  id, source, title, created_at, asin, subreddit, url, description, channel_name, country_of_origin, price, currency, star_ratings, total_rating, raw_json

TABLE comments:
  id, post_id, author_name, content, rating, helpful_votes, karma, created_at, age_group, gender, income_band
    """
    
    # Generate SQL query from natural language using the LLM.
    print("\nGenerating SQL query from your natural language input...")
    sql_query = generate_sql_from_nl(user_query, schema_description)
    print("\nGenerated SQL Query:")
    print(sql_query)
    
    # Execute the SQL query and display results.
    results = execute_sql_query(sql_query)
    print("\nQuery Results:")
    print(json.dumps(results, indent=2, cls=DecimalEncoder))
    
    # Ask the LLM to explain the results.
    explanation = explain_results(results, user_query)
    print("\nExplanation:")
    print(explanation)

if __name__ == "__main__":
    main()
