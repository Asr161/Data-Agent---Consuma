
---

# Data Agent: Intelligent ETL and Query System

## Overview
This **Data Agent** is an intelligent ETL (Extract, Transform, Load) pipeline built in Python. It processes social media data from multiple platforms (e.g., Amazon, Reddit, and YouTube) by:
- Extracting data from a JSON file containing posts from these platforms.
- Transforming platform-specific fields into a standardized schema.
- Loading the transformed data into a PostgreSQL database.
- Using Generative AI (via the OpenAI API) to convert natural language queries into SQL and to provide human-friendly explanations of query results.

This solution is designed to scale to large datasets and is modular and adaptable to new data sources or schema changes.

---

## Key Features
- **Selection:**  
  Extracts only the fields that are present in the source JSON data.
  
- **Projection:**  
  Maps platform-specific fields (e.g., Amazon product details, Reddit post content, YouTube video details) into a common schema and removes unnecessary fields.
  
- **Transformation:**  
  Standardizes heterogeneous data formats (dates, numbers) using helper functions.
  
- **LLM Integration:**  
  - Generates SQL queries from natural language input using the OpenAI API.
  - Executes the SQL query against a PostgreSQL database.
  - Explains query results using LLM-powered analysis.
  
- **Adaptability:**  
  The modular design makes it easy to update the ingestion logic if new data sources or schema changes occur.
  
- **Scalability:**  
  Although the development version uses PostgreSQL, the architecture can be extended (with indexing, partitioning, etc.) to handle terabyte-scale datasets.

---

## Installation & Usage

### Prerequisites
- **Python 3.7+** 
- **PostgreSQL** 
- **Git** 
- An **OpenAI API key** 

---

### 1. Clone the Repository
```bash
git clone https://github.com/Asr161/Data-Agent-Consuma
cd Data-Agent
```

---

### 2. Set Up the Python Virtual Environment
1. Create a virtual environment:
   ```bash
   python3 -m venv venv
   ```
2. Activate the virtual environment:
   - **Mac/Linux:**
     ```bash
     source venv/bin/activate
     ```
   - **Windows:**
     ```bash
     venv\Scripts\activate
     ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

---

### 3. Download the Dataset
Download your JSON dataset files (e.g., `sample_data.json`, `sample_data_large.json`, `sample_data_largest.json`) and place them in the project root.  


---

### 4. Set Up PostgreSQL
#### A. Install & Start PostgreSQL
- **Local Installation:**  
  Download and install PostgreSQL from [postgresql.org](https://www.postgresql.org/download/).  
  On Mac, if using Homebrew:
  ```bash
  brew install postgresql
  brew services start postgresql
  ```
  
#### B. Create a PostgreSQL User and Database
1. **Open Terminal and connect as a superuser (usually `postgres`):**
   ```bash
   psql -U postgres
   ```
2. **Create a new role (user):**
   ```sql
   CREATE ROLE your_username WITH LOGIN PASSWORD 'Your_Str0ng_Password';
   ```
3. **Create a new database:**
   ```sql
   CREATE DATABASE social_data OWNER your_username;
   ```
4. **Exit psql:**
   ```sql
   \q
   ```

---

### 5. Configure Environment Variables
Create a `.env` file in the project root and add the following lines (update the placeholders with your actual values):
```
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=social_data
PG_USER=your_username
PG_PASSWORD=Your_Str0ng_Password
OPENAI_API_KEY=your_openai_api_key
```
**Important:** Ensure `.env` is added to your `.gitignore` to keep your credentials secure.

---

### 6. Running the Data Agent

In line 542 of agent.py, enter the prompt you wish to test.
Run the agent to ingest the dataset, generate SQL queries from natural language input, execute them, and explain the results:
```bash
python agent.py
```
The agent will:
- Connect to PostgreSQL and create the necessary schema.
- Ingest the JSON data from `sample_data.json`.
- Accept a natural language query (e.g., "What is the distribution of review counts by country of origin for Amazon products?").
- Generate a corresponding SQL query using the LLM.
- Execute the SQL query against the database.
- Display the query results and a human-friendly explanation of those results.

---

### 7. Automated Testing
Run the test queries to see the expected outputs:
```bash
pytest test_queries/
```

---

### 8. Performance Benchmarks
Run the benchmark script to measure ingestion and query performance across different dataset sizes:
```bash
python benchmarks.py
```
The benchmark script compares performance for `sample_data.json`, `sample_data_10K.json`, and `sample_data_50K.json` and prints a summary table. The first one is the original
data provided with the assignment. The second and third datasets are generated randomly using the faker library. 

---

## Data Schema

### posts Table
| Column              | Type    | Description |
|---------------------|---------|-------------|
| id                  | SERIAL  | Primary key |
| source              | TEXT    | Social media platform (e.g., "amazon", "reddit", "youtube") |
| title               | TEXT    | Post title or product name |
| created_at          | TEXT    | Creation date (formatted as 'YYYY-MM-DD') |
| asin                | TEXT    | Amazon product identifier (if applicable) |
| subreddit           | TEXT    | Subreddit name (if applicable) |
| url                 | TEXT    | URL of the post/video |
| description         | TEXT    | Post/video description |
| channel_name        | TEXT    | YouTube channel name (if applicable) |
| country_of_origin   | TEXT    | Country of origin for Amazon products |
| price               | REAL    | Price of the product (if available) |
| currency            | TEXT    | Currency of the product price |
| star_ratings        | TEXT    | Star ratings (Amazon-specific) |
| total_rating        | INTEGER | Total number of reviews |
| raw_json            | TEXT    | Full raw JSON record |

### comments Table
| Column         | Type    | Description |
|----------------|---------|-------------|
| id             | SERIAL  | Primary key |
| post_id        | INTEGER | Foreign key referencing `posts.id` |
| author_name    | TEXT    | Comment/review author |
| content        | TEXT    | Text content of the comment/review |
| rating         | REAL    | Rating value (if applicable) |
| helpful_votes  | TEXT    | Helpful vote information (Amazon-specific) |
| karma          | INTEGER | Reddit comment karma (if applicable) |
| created_at     | TEXT    | Comment creation date ('YYYY-MM-DD') |
| age_group      | TEXT    | Age group from user info |
| gender         | TEXT    | Gender from user info |
| income_band    | TEXT    | Income band from user info |


---

