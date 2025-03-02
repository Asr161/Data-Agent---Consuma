import json
import random
import string
from faker import Faker

fake = Faker()

def random_source():
    return random.choice(["amazon_reviews_1", "reddit_1", "youtube_1"])

def random_amazon_record():
    return {
        "source": "amazon_reviews_1",
        "asin": "".join(random.choices(string.ascii_uppercase + string.digits, k=10)),
        "product_details": {
            "title": fake.sentence(),
            "Country of Origin": fake.country(),
            "price": str(random.randint(100, 2000)),
            "currency": "USD",
            "star_ratings": str(random.randint(1, 5)),
            "total_rating": random.randint(1, 500),
        },
        "reviews": [
            {
                "review_author": fake.name(),
                "content": fake.paragraph(),
                "review_star_rating": str(random.randint(1, 5)),
                "review_date": str(fake.date_this_year()),
                "helpful_vote_statement": f"{random.randint(1, 5)} people found this helpful",
                "user_info": {
                    "age_group": random.choice(["18-24", "25-34", "35-44", "Unknown"]),
                    "gender": random.choice(["Male", "Female", "Unknown"]),
                    "income_band": random.choice(["Lower", "Middle", "Upper"])
                }
            }
            for _ in range(random.randint(1, 5))  # 1-5 reviews
        ]
    }

def random_reddit_record():
    return {
        "source": "reddit_1",
        "subreddit": fake.word(),
        "created_at": str(fake.date_this_year()),
        "content": fake.sentence(),
        "comments": [
            {
                "body": fake.paragraph(),
                "author": fake.user_name(),
                "karma": random.randint(-5, 50),
                "created_at": str(fake.date_this_year()),
                "user_info": {
                    "age_group": random.choice(["18-24", "25-34", "35-44", "Unknown"]),
                    "gender": random.choice(["Male", "Female", "Unknown"]),
                    "income_band": random.choice(["Lower", "Middle", "Upper"])
                }
            }
            for _ in range(random.randint(1, 3))  # 1-3 comments
        ]
    }

def random_youtube_record():
    return {
        "source": "youtube_1",
        "title": fake.sentence(),
        "published_at": str(fake.date_time_this_year()),
        "description": fake.text(),
        "channel_name": fake.company(),
        "comments": [
            {
                "text": fake.paragraph(),
                "author_name": fake.user_name(),
                "time": str(fake.date_time_this_year()),
                "user_info": {
                    "age_group": random.choice(["18-24", "25-34", "35-44", "Unknown"]),
                    "gender": random.choice(["Male", "Female", "Unknown"]),
                    "income_band": random.choice(["Lower", "Middle", "Upper"])
                }
            }
            for _ in range(random.randint(2, 6))  # 2-6 comments
        ]
    }

def create_large_json(num_records=100000):
    data = []
    for _ in range(num_records):
        src = random_source()
        if "amazon" in src:
            data.append(random_amazon_record())
        elif "reddit" in src:
            data.append(random_reddit_record())
        else:
            data.append(random_youtube_record())
    return data

if __name__ == "__main__":
    # Generate 100k records (this can be scaled to millions).
    large_data = create_large_json(num_records=10000)
    with open("sample_data_10K.json", "w", encoding="utf-8") as f:
        json.dump(large_data, f, ensure_ascii=False)
