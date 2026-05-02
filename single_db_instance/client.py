import requests
import random
import time
import statistics
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

BASE_URL = "http://localhost:5000"

# Use a session to reuse TCP connections, improving performance
session = requests.Session()

def put_student(student_data):
    response = session.post(f"{BASE_URL}/put", json=student_data)
    return response.json()

def get_student_timed(student_id):
    start = time.time()
    session.get(f"{BASE_URL}/get/{student_id}")
    end = time.time()
    return (end - start) * 1000

def get_student(student_id):
    response = session.get(f"{BASE_URL}/get/{student_id}")
    return response.json()

def performance_test(num_queries=1000, max_workers=50):
    print(f"\nRunning CONCURRENT performance test...")
    print(f"Total queries      : {num_queries}")
    print(f"Concurrent workers : {max_workers}")
    print(f"Total records in DB: 1,000,001")

    random_ids = [random.randint(1, 1_000_000) for _ in range(num_queries)]
    query_times = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_student_timed, sid): sid for sid in random_ids}

        completed = 0
        for future in as_completed(futures):
            query_times.append(future.result())
            completed += 1
            if completed % 100 == 0:
                print(f"  Completed {completed}/{num_queries} queries...")

    avg_time = statistics.mean(query_times)

    print(f"\n{'='*40}")
    print(f"PERFORMANCE RESULTS - SINGLE DB")
    print(f"{'='*40}")
    print(f"Total queries     : {num_queries}")
    print(f"Concurrent workers: {max_workers}")
    print(f"Average time      : {avg_time:.4f} ms")
    print(f"Fastest query     : {min(query_times):.4f} ms")
    print(f"Slowest query     : {max(query_times):.4f} ms")
    print(f"Median time       : {statistics.median(query_times):.4f} ms")
    print(f"{'='*40}")

    # Save the average dynamically so the sharded script can read it!
    # No more hardcoded values.
    try:
        with open(os.path.join(os.path.dirname(__file__), "..", "single_db_avg.txt"), "w") as f:
            f.write(str(avg_time))
    except Exception as e:
        print(f"Failed to save metrics: {e}")

    return query_times

if __name__ == '__main__':

    print("Testing PUT...")
    result = put_student({
        "student_id": 999999998,
        "name": "Test Student",
        "department": "CSE",
        "age": 21,
        "cgpa": 9.0
    })
    print(f"PUT result: {result}")

    print("\nTesting GET...")
    result = get_student(500000)
    print(f"GET result: {result}")

    performance_test(1000, max_workers=50)