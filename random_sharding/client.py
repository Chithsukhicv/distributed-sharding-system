import requests
import random
import time
import statistics
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

BASE_URL = "http://localhost:5000"  # Points to central server now

# Use a session to reuse TCP connections
session = requests.Session()

def put_student(student_data):
    response = session.post(f"{BASE_URL}/put", json=student_data)
    return response.json()

def get_student(student_id):
    response = session.get(f"{BASE_URL}/get/{student_id}")
    return response.json()

def get_student_timed(student_id):
    start = time.time()
    get_student(student_id)
    end = time.time()
    return (end - start) * 1000

def performance_test(num_queries=1000, max_workers=50):
    print(f"\nRunning CONCURRENT performance test with {num_queries} queries and {max_workers} workers...")
    print(f"Sending requests through central server → random sharded backends")

    query_times = []
    random_ids = [random.randint(1, 1_000_000) for _ in range(num_queries)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_student_timed, sid): sid for sid in random_ids}

        completed = 0
        for future in as_completed(futures):
            query_times.append(future.result())
            completed += 1
            if completed % 100 == 0:
                print(f"  Completed {completed}/{num_queries} queries...")

    print(f"\n{'='*40}")
    print(f"PERFORMANCE RESULTS - RANDOM SHARDING")
    print(f"{'='*40}")
    print(f"Total queries    : {num_queries}")
    print(f"Average time     : {statistics.mean(query_times):.4f} ms")
    print(f"Fastest query    : {min(query_times):.4f} ms")
    print(f"Slowest query    : {max(query_times):.4f} ms")
    print(f"Median time      : {statistics.median(query_times):.4f} ms")
    print(f"{'='*40}")

    return query_times

def compare_results(single_db_avg, sharded_avg):
    improvement = ((single_db_avg - sharded_avg) / single_db_avg) * 100
    print(f"\n{'='*40}")
    print(f"COMPARISON")
    print(f"{'='*40}")
    print(f"Single DB average  : {single_db_avg:.4f} ms")
    print(f"Random average     : {sharded_avg:.4f} ms")
    print(f"Improvement        : {improvement:.1f}%")
    print(f"{'='*40}")

if __name__ == '__main__':
    print("Testing PUT through central server...")
    result = put_student({
        "student_id": 1000002,
        "name": "Test Student",
        "department": "CSE",
        "age": 21,
        "cgpa": 9.0
    })
    print(f"PUT result: {result}")

    print("\nTesting GET through central server...")
    result = get_student(500000)
    print(f"GET result: {result}")

    print("\nChecking count across all shards...")
    count_response = session.get(f"{BASE_URL}/count")
    print(f"Count result: {count_response.json()}")

    sharded_times = performance_test(1000, max_workers=50)
    sharded_avg = sum(sharded_times) / len(sharded_times)

    # Read the dynamic single DB average instead of hardcoding
    single_db_avg = 0
    try:
        metrics_path = os.path.join(os.path.dirname(__file__), "..", "single_db_instance", "single_db_avg.txt")
        with open(metrics_path, "r") as f:
            single_db_avg = float(f.read().strip())
        
        compare_results(single_db_avg=single_db_avg, sharded_avg=sharded_avg)
    except FileNotFoundError:
        print("\nNote: single_db_avg.txt not found. Please run the single_db_instance/client.py first to generate comparison metrics.")
