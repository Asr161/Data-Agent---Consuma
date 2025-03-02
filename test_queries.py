# test_queries.py
import json
from agent import generate_sql_from_nl, execute_sql_query, explain_results

# Optional schema description used in the prompt.
SCHEMA_DESCRIPTION = """
TABLE posts:
  id, source, title, created_at, asin, subreddit, url, description, channel_name, country_of_origin, price, currency, star_ratings, total_rating, raw_json

TABLE comments:
  id, post_id, author_name, content, rating, helpful_votes, karma, created_at, age_group, gender, income_band
"""

def test_query_sources():
    user_query = "What are the distinct sources of data?"
    sql_query = generate_sql_from_nl(user_query, SCHEMA_DESCRIPTION)
    print("\nGenerated SQL Query for sources:")
    print(sql_query)
    results = execute_sql_query(sql_query)
    print("\nQuery Results:")
    print(json.dumps(results, indent=2))
    # Here you might assert something like the number of distinct sources is > 0.
    assert len(results) > 0

def test_query_platform_gender():
    user_query = "By platform and gender, how many reviews and what's the average star rating?"
    sql_query = generate_sql_from_nl(user_query, SCHEMA_DESCRIPTION)
    print("\nGenerated SQL Query for platform and gender:")
    print(sql_query)
    results = execute_sql_query(sql_query)
    print("\nQuery Results:")
    print(json.dumps(results, indent=2))
    explanation = explain_results(results, user_query)
    print("\nExplanation of Query Results:")
    print(explanation)
    # Add assertions if you have known expected values.
    
def test_query_reddit_posts_per_month():
    user_query = "For Reddit posts, show the number of posts per month."
    sql_query = generate_sql_from_nl(user_query, SCHEMA_DESCRIPTION)
    print("\nGenerated SQL Query for Reddit posts per month:")
    print(sql_query)
    results = execute_sql_query(sql_query)
    print("\nQuery Results:")
    print(json.dumps(results, indent=2))
    explanation = explain_results(results, user_query)
    print("\nExplanation of Query Results:")
    print(explanation)

if __name__ == "__main__":
    test_query_sources()
    test_query_platform_gender()
    test_query_reddit_posts_per_month()
