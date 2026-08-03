"""
Benchmark Runner - Compares all database architectures at multiple data sizes.
Saves results to benchmark_results.json for plotting.

Tests: Single DB, Hash Sharded, Random Sharded, Round Robin, Directory Based
Sizes: 1M, 3M, 5M, 7M, 10M records
Queries: 500 per test, 10 concurrent workers
"""

import subprocess
import time
import os
import sys
import json
import random
import statistics
import socket
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_FILE = os.path.join(BASE_DIR, "benchmark_results.json")

ARCHITECTURES = [
    {
        "name": "single_db",
        "dir": "single_db_instance",
        "label": "Single DB",
    },
    {
        "name": "hash_sharded",
        "dir": "sharded_version",
        "label": "Hash Sharded",
    },
    {
        "name": "random_sharded",
        "dir": "random_sharding",
        "label": "Random Sharded",
    },
    {
        "name": "round_robin",
        "dir": "round_robin_sharding",
        "label": "Round Robin",
    },
    {
        "name": "directory_based",
        "dir": "directory_based_sharding",
        "label": "Directory Based",
    },
]

DATA_SIZES = [1_000_000, 3_000_000, 5_000_000, 7_000_000, 10_000_000]
NUM_QUERIES = 500
MAX_WORKERS = 10


# ──────────────────────────────────────────────
#  SAFETY CHECKS - No more port conflicts!
# ──────────────────────────────────────────────

def is_port_in_use(port):
    """Check if a port is currently in use"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex(('localhost', port)) == 0


def check_local_mongodb():
    """
    Check if local MongoDB is running on port 27017.
    This MUST be stopped or single_db benchmarks will be wrong.
    """
    try:
        from pymongo import MongoClient
        # Try connecting to local MongoDB (not Docker)
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        client.close()
        return True  # Local MongoDB IS running
    except:
        return False  # Not running or not reachable


def stop_all_containers():
    """Stop ALL architecture containers to free ALL ports"""
    log("Stopping all architecture containers...")
    for arch in ARCHITECTURES:
        full_path = os.path.join(BASE_DIR, arch["dir"])
        if os.path.exists(os.path.join(full_path, "docker-compose.yml")):
            subprocess.run(
                "docker-compose down",
                cwd=full_path, shell=True,
                capture_output=True, text=True
            )
    # Wait for ports to be freed
    time.sleep(5)


def verify_ports_free():
    """Verify all critical ports are free before starting"""
    critical_ports = [5000, 27017, 27018, 27019, 27020, 27021]
    blocked = []
    for port in critical_ports:
        if is_port_in_use(port):
            blocked.append(port)
    return blocked


# ──────────────────────────────────────────────
#  CORE FUNCTIONS
# ──────────────────────────────────────────────

def log(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def docker_compose_up(arch_dir):
    """Start docker containers with a fresh build"""
    full_path = os.path.join(BASE_DIR, arch_dir)
    log(f"  Starting containers ({arch_dir})...")
    result = subprocess.run(
        "docker-compose up -d --build",
        cwd=full_path, shell=True,
        capture_output=True, text=True,
        timeout=300
    )
    if result.returncode != 0:
        log(f"  ERROR starting containers: {result.stderr[-200:]}")
        return False
    return True


def docker_compose_down(arch_dir):
    """Stop containers"""
    full_path = os.path.join(BASE_DIR, arch_dir)
    subprocess.run(
        "docker-compose down",
        cwd=full_path, shell=True,
        capture_output=True, text=True
    )


def wait_for_health(timeout=120):
    """Wait for the central server to respond on port 5000"""
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get("http://localhost:5000/health", timeout=3)
            if r.status_code == 200:
                return True
        except:
            pass
        time.sleep(3)
    return False


def load_data(arch_dir, total):
    """Run data_loader.py to insert data"""
    full_path = os.path.join(BASE_DIR, arch_dir)
    log(f"  Loading {total:,} records...")
    start = time.time()

    result = subprocess.run(
        f'python -c "import data_loader; data_loader.load_data(total={total})"',
        cwd=full_path, shell=True,
        capture_output=True, text=True,
        timeout=1800  # 30 min max
    )

    elapsed = time.time() - start
    log(f"  Data loaded in {elapsed:.0f}s")

    if result.returncode != 0:
        log(f"  WARNING: data_loader error!")
        lines = result.stderr.strip().split('\n') if result.stderr else []
        for line in lines[-3:]:
            log(f"    {line[:150]}")
        return False

    # Print last few lines of stdout (shows shard breakdown)
    lines = result.stdout.strip().split('\n') if result.stdout else []
    for line in lines[-5:]:
        log(f"    {line[:150]}")

    return True


def run_benchmark(num_queries=500, max_workers=10):
    """Run benchmark queries against localhost:5000"""
    session = requests.Session()

    def get_student_timed(student_id):
        start = time.time()
        try:
            session.get(f"http://localhost:5000/get/{student_id}", timeout=300)
        except:
            pass
        end = time.time()
        return (end - start) * 1000

    # Get actual record count
    try:
        count_resp = session.get("http://localhost:5000/count", timeout=10)
        count_data = count_resp.json()
        actual_count = count_data.get("total_count", count_data.get("count", 0))
    except:
        actual_count = 0

    log(f"  DB reports {actual_count:,} records")

    if actual_count == 0:
        log("  ERROR: No records found in DB! Skipping benchmark.")
        return None

    max_id = actual_count if actual_count > 0 else 10_000_000
    random_ids = [random.randint(1, max_id) for _ in range(num_queries)]
    query_times = []

    log(f"  Querying {num_queries} times with {max_workers} workers...")
    start = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_student_timed, sid): sid for sid in random_ids}
        completed = 0
        for future in as_completed(futures):
            query_times.append(future.result())
            completed += 1
            if completed % 100 == 0:
                log(f"    {completed}/{num_queries} done...")

    elapsed = time.time() - start
    avg = statistics.mean(query_times)
    median = statistics.median(query_times)

    log(f"  Done in {elapsed:.0f}s | avg={avg:.1f}ms median={median:.1f}ms")

    return {
        "record_count": actual_count,
        "avg_ms": round(avg, 2),
        "median_ms": round(median, 2),
        "min_ms": round(min(query_times), 2),
        "max_ms": round(max(query_times), 2),
        "p95_ms": round(sorted(query_times)[int(len(query_times) * 0.95)], 2),
        "total_queries": num_queries,
        "workers": max_workers,
    }


def load_existing_results():
    """Load previously saved results (for resuming)"""
    if os.path.exists(RESULTS_FILE):
        try:
            with open(RESULTS_FILE, "r") as f:
                return json.load(f)
        except:
            pass
    return {
        "metadata": {
            "queries_per_test": NUM_QUERIES,
            "workers": MAX_WORKERS,
            "data_sizes": DATA_SIZES,
        },
        "results": {}
    }


def save_results(data):
    """Save results to JSON"""
    data["metadata"]["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(RESULTS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    log(f"Results saved to {RESULTS_FILE}")


def print_summary_table(data):
    """Print a nice comparison table"""
    results = data["results"]

    print("\n" + "=" * 110)
    print("  FINAL BENCHMARK RESULTS (Average Query Time in ms)")
    print("=" * 110)

    # Header
    print(f"{'Size':<8}", end="")
    for arch in ARCHITECTURES:
        print(f"| {arch['label']:^18}", end="")
    print("|")
    print("-" * 110)

    # Data rows
    for size in DATA_SIZES:
        size_label = f"{size // 1_000_000}M"
        print(f"{size_label:<8}", end="")
        for arch in ARCHITECTURES:
            key = arch["name"]
            size_key = str(size)
            if key in results and size_key in results[key]:
                avg = results[key][size_key]["avg_ms"]
                print(f"| {avg:>14.1f} ms", end="")
            else:
                print(f"|       {'N/A':>8}   ", end="")
        print("|")

    # Improvement rows
    print("-" * 110)
    print("  IMPROVEMENT vs Single DB:")
    print("-" * 110)

    for size in DATA_SIZES:
        size_label = f"{size // 1_000_000}M"
        size_key = str(size)
        single_avg = None
        if "single_db" in results and size_key in results["single_db"]:
            single_avg = results["single_db"][size_key]["avg_ms"]

        if single_avg is None:
            continue

        print(f"{size_label:<8}", end="")
        for arch in ARCHITECTURES:
            key = arch["name"]
            if key in results and size_key in results[key]:
                avg = results[key][size_key]["avg_ms"]
                if key == "single_db":
                    print(f"|     {'baseline':>10}   ", end="")
                else:
                    imp = ((single_avg - avg) / single_avg) * 100
                    print(f"| {imp:>+13.1f} %", end="")
            else:
                print(f"|       {'N/A':>8}   ", end="")
        print("|")

    print("=" * 110)


# ──────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────

def main():
    print("\n" + "=" * 60)
    print("  🚀 DATABASE ARCHITECTURE BENCHMARK RUNNER")
    print("=" * 60)
    print(f"  Data sizes     : {', '.join(f'{s//1_000_000}M' for s in DATA_SIZES)}")
    print(f"  Queries/test   : {NUM_QUERIES}")
    print(f"  Workers        : {MAX_WORKERS}")
    print(f"  Architectures  : {len(ARCHITECTURES)}")
    print(f"  Total tests    : {len(DATA_SIZES) * len(ARCHITECTURES)}")
    print("=" * 60)

    # SAFETY CHECK 1: Stop all containers first
    stop_all_containers()

    # SAFETY CHECK 2: Verify local MongoDB is stopped
    if check_local_mongodb():
        log("⚠️  LOCAL MONGODB IS RUNNING ON PORT 27017!")
        log("   This WILL corrupt single_db benchmarks.")
        log("   Run 'net stop MongoDB' in Admin PowerShell first!")
        log("   Press Enter to continue anyway, or Ctrl+C to abort...")
        input()
    else:
        log("✅ Local MongoDB is NOT running. Good!")

    # SAFETY CHECK 3: Verify all ports are free
    blocked = verify_ports_free()
    if blocked:
        log(f"⚠️  Ports still in use: {blocked}")
        log("   Waiting 10 more seconds...")
        time.sleep(10)
        blocked = verify_ports_free()
        if blocked:
            log(f"   Still blocked: {blocked}. Proceeding anyway...")

    # Load existing results (for resuming interrupted runs)
    all_data = load_existing_results()

    total_tests = len(DATA_SIZES) * len(ARCHITECTURES)
    test_num = 0
    start_time = time.time()

    for size in DATA_SIZES:
        size_label = f"{size // 1_000_000}M"
        size_key = str(size)

        print(f"\n{'━' * 60}")
        log(f"📊 TESTING WITH {size_label} RECORDS")
        print(f"{'━' * 60}")

        for arch in ARCHITECTURES:
            test_num += 1
            arch_name = arch["name"]

            # Skip if already tested (resume support)
            if arch_name in all_data["results"] and size_key in all_data["results"][arch_name]:
                log(f"[{test_num}/{total_tests}] {arch['label']} @ {size_label} - ALREADY DONE, skipping")
                continue

            print(f"\n--- [{test_num}/{total_tests}] {arch['label']} @ {size_label} ---")

            # Step 1: Stop everything
            stop_all_containers()

            # Step 2: Verify ports are free
            blocked = verify_ports_free()
            if blocked:
                log(f"  Ports {blocked} still busy, waiting 10s...")
                time.sleep(10)

            # Step 3: Start this architecture
            if not docker_compose_up(arch["dir"]):
                log("  FAILED to start. Skipping.")
                continue

            # Step 4: Wait for health
            log("  Waiting for services...")
            if not wait_for_health(timeout=120):
                log("  TIMEOUT: Services did not start. Skipping.")
                docker_compose_down(arch["dir"])
                continue

            log("  ✅ Services ready!")
            time.sleep(5)  # Let DB fully settle

            # Step 5: Load data
            if not load_data(arch["dir"], size):
                log("  Data load had issues. Trying benchmark anyway...")

            time.sleep(5)  # Let DB settle after load

            # Step 6: Run benchmark
            try:
                result = run_benchmark(NUM_QUERIES, MAX_WORKERS)
                if result:
                    if arch_name not in all_data["results"]:
                        all_data["results"][arch_name] = {}
                    all_data["results"][arch_name][size_key] = result

                    # Save after EVERY test (crash-safe)
                    save_results(all_data)
                    log(f"  ✅ {arch['label']} @ {size_label}: avg={result['avg_ms']:.1f}ms")
            except Exception as e:
                log(f"  ❌ ERROR: {e}")

            # Step 7: Stop containers
            docker_compose_down(arch["dir"])
            time.sleep(3)

    # Final output
    total_time = time.time() - start_time
    log(f"\n🏁 All tests complete! Total time: {total_time / 60:.1f} minutes")

    print_summary_table(all_data)
    save_results(all_data)

    # Clean up
    stop_all_containers()


if __name__ == "__main__":
    main()
