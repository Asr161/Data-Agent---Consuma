# benchmarks.py

import time
import json
from agent import get_pg_connection, create_schema, ingest_json_file, execute_sql_query

DATASETS = [
    "sample_data.json",
    "sample_data_10K.json",
    "sample_data_50K.json"
]

def benchmark_ingestion_for_file(json_file, iterations=3):
    """
    Ingests the given JSON file multiple times and returns the average ingestion time.
    
    Args:
        json_file (str): Path to the JSON file.
        iterations (int): Number of iterations for averaging.
        
    Returns:
        float: Average ingestion time in seconds.
    """
    total_time = 0
    for i in range(iterations):
        conn = get_pg_connection()
        create_schema(conn)
        start = time.time()
        ingest_json_file(conn, json_file)
        end = time.time()
        elapsed = end - start
        total_time += elapsed
        
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE comments, posts RESTART IDENTITY CASCADE;")
        conn.commit()
        conn.close()
    return total_time / iterations

def benchmark_query_for_file(query, iterations=3):
    """
    Executes the given SQL query multiple times and returns the average query execution time.
    
    Args:
        query (str): SQL query to execute.
        iterations (int): Number of iterations for averaging.
        
    Returns:
        tuple: Average query time in seconds and the query results from the final iteration.
    """
    total_time = 0
    results = None
    for i in range(iterations):
        start = time.time()
        results = execute_sql_query(query)
        end = time.time()
        total_time += (end - start)
    return total_time / iterations, results

def main():

    sample_query = "SELECT source, COUNT(*) AS total_posts FROM posts GROUP BY source;"
    
    # Dictionaries to store the benchmark results.
    ingestion_results = {}
    query_results = {}
    
    # Benchmark each dataset.
    for ds in DATASETS:
        print(f"\nBenchmarking dataset: {ds}")
        
        # Benchmark ingestion
        avg_ingestion_time = benchmark_ingestion_for_file(ds, iterations=3)
        ingestion_results[ds] = avg_ingestion_time
        print(f"  Average ingestion time: {avg_ingestion_time:.2f} seconds")
        
        # Ingest the dataset once to have data for query execution.
        conn = get_pg_connection()
        create_schema(conn)
        ingest_json_file(conn, ds)
        conn.close()
        
        # Benchmark query execution
        avg_query_time, _ = benchmark_query_for_file(sample_query, iterations=3)
        query_results[ds] = avg_query_time
        print(f"  Average query time: {avg_query_time:.4f} seconds")
    
    # Print summary of results.
    print("\n=== Performance Benchmark Summary ===")
    print("{:<25} {:<25} {:<25}".format("Dataset", "Ingestion Time (s)", "Query Time (s)"))
    for ds in DATASETS:
        print("{:<25} {:<25.2f} {:<25.4f}".format(ds, ingestion_results[ds], query_results[ds]))

if __name__ == "__main__":
    main()
