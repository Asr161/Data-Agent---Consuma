# test_queries.py
import json
from agent import generate_sql_from_nl, execute_sql_query, explain_results

SCHEMA_DESCRIPTION = """
TABLE posts:
  id, source, title, created_at, asin, subreddit, url, description, channel_name, country_of_origin, price, currency, star_ratings, total_rating, raw_json

TABLE comments:
  id, post_id, author_name, content, rating, helpful_votes, karma, created_at, age_group, gender, income_band
"""

import json

def test_query_sources():
    user_query = "What are the distinct sources of data?"
    sql_query = generate_sql_from_nl(user_query, SCHEMA_DESCRIPTION)
    # print("\nGenerated SQL Query for sources:")
    # print(sql_query)
    results = execute_sql_query(sql_query)
    # print("\nQuery Results:")
    # print(json.dumps(results, indent=2))
    explanation = explain_results(results, user_query)
    print("\nAnswer:")
    print(explanation)
    #Expected output: The distinct sources of data based on the query results are Amazon, Reddit, and YouTube.

def test_count_posts_by_platform():
    user_query = "How many posts are there for each platform?"
    sql_query = generate_sql_from_nl(user_query, SCHEMA_DESCRIPTION)
    # print("\nGenerated SQL Query for post count by platform:")
    # print(sql_query)
    results = execute_sql_query(sql_query)
    # print("\nQuery Results:")
    # print(json.dumps(results, indent=2))
    explanation = explain_results(results, user_query)
    print("\nAnswer:")
    print(explanation)
    #Expected output: Here are the number of posts for each platform:
    # - Amazon: 308 posts
    # - YouTube: 20 posts
    # - Reddit: 167 posts

def test_avg_amazon_star_rating():
    user_query = "What is the average rating for Amazon products?"
    sql_query = generate_sql_from_nl(user_query, SCHEMA_DESCRIPTION)
    # print("\nGenerated SQL Query for average Amazon star rating:")
    # print(sql_query)
    results = execute_sql_query(sql_query)
    # print("\nQuery Results:")
    # print(json.dumps(results, indent=2))
    explanation = explain_results(results, user_query)
    print("\nAnswer:")
    print(explanation)
    # Expected output: The average rating for Amazon products is approximately 4.11.

def test_top_amazon_product():
    user_query = "Which Amazon product has the highest number of reviews?"
    sql_query = generate_sql_from_nl(user_query, SCHEMA_DESCRIPTION)
    # print("\nGenerated SQL Query for top reviewed Amazon product:")
    # print(sql_query)
    results = execute_sql_query(sql_query)
    # print("\nQuery Results:")
    # print(json.dumps(results, indent=2))
    explanation = explain_results(results, user_query)
    print("\nAnswer:")
    print(explanation)
    #Expected output: The product "PILGRIM Korean 2% Alpha Arbutin & 3% Vitamin C Brightening Face Serum for glowing skin| Alpha arbutin face serum|All skin types 
    # | Men & Women| Korean Skin Care| Vegan & Cruelty-free | 30ml" has the highest number of reviews on Amazon, with a total of 2 reviews.
   

def test_most_active_subreddit():
    user_query = "Which subreddit has the highest number of posts?"
    sql_query = generate_sql_from_nl(user_query, SCHEMA_DESCRIPTION)
    # print("\nGenerated SQL Query for most active subreddit:")
    # print(sql_query)
    results = execute_sql_query(sql_query)
    # print("\nQuery Results:")
    # print(json.dumps(results, indent=2))
    explanation = explain_results(results, user_query)
    print("\nAnswer:")
    print(explanation)
    #Expected output: The subreddit "IndianSkincareAddicts" has the highest number of posts, with a total of 95 posts.



if __name__ == "__main__":
    test_query_sources()
    test_count_posts_by_platform()
    test_avg_amazon_star_rating()
    test_top_amazon_product()
    test_most_active_subreddit()
