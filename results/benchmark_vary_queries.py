"""
Benchmark: GRAPH 1 — Fixed Data Load, Vary Query Count
=======================================================
Data sizes tested : 1M and 5M records (loaded once per arch)
Query counts      : 100, 250, 500, 1000, 2000, 5000
Workers           : 10 (constant)
Architectures     : All 5 (or single via --arch flag)

Results saved to  : results_vary_queries.json

Usage:
  python benchmark_vary_queries.py                      # run ALL architectures
  python benchmark_vary_queries.py --arch single_db     # run ONE architecture
  python benchmark_vary_queries.py --arch hash_sharded
  python benchmark_vary_queries.py --arch random_sharded
  python benchmark_vary_queries.py --arch round_robin
  python benchmark_vary_queries.py --arch directory_based
"""

import subprocess
import time
import os
import sys
import json
import random
import statistics
import socket
import argparse
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ──────────────────────────────────────────────
#  CONFIGURATION
# ──────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_FILE = os.path.join(BASE_DIR, "results_vary_queries.json")

ALL_ARCHITECTURES = [
    {"name": "single_db",        "dir": "single_db_instance",    "label": "Single DB"},
    {"name": "hash_sharded",     "dir": "sharded_version",        "label": "Hash Sharded"},
    {"name": "random_sharded",   "dir": "random_sharding",        "label": "Random Sharded"},
    {"name": "round_robin",      "dir": "round_robin_sharding",   "label": "Round Robin"},
    {"name": "directory_based",  "dir": "directory_based_sharding","label": "Directory Based"},
]

# Fixed data sizes for this experiment
DATA_SIZES = [1_000_000, 5_000_000]

# Vary query counts — this is the X-axis of Graph 1
QUERY_COUNTS = [100, 250, 500, 1000, 2000, 5000]

MAX_WORKERS = 10


# ──────────────────────────────────────────────
#  HELPERS
# ──────────────────────────────────────────────

def log(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex(('localhost', port)) == 0


def stop_all_containers():
    log("Stopping all architecture containers...")
    for arch in ALL_ARCHITECTURES:
        full_path = os.path.join(BASE_DIR, arch["dir"])
        if os.path.exists(os.path.join(full_path, "docker-compose.yml")):
            subprocess.run(
                "docker-compose down",
                cwd=full_path, shell=True,
                capture_output=True, text=True
            )
    time.sleep(5)


def verify_ports_free():
    critical_ports = [5000, 27017, 27018, 27019, 27020, 27021]
    return [p for p in critical_ports if is_port_in_use(p)]


def docker_compose_up(arch_dir):
    full_path = os.path.join(BASE_DIR, arch_dir)
    log(f"  Starting containers ({arch_dir})...")
    result = subprocess.run(
        "docker-compose up -d --build",
        cwd=full_path, shell=True,
        capture_output=True, text=True,
        timeout=300
    )
    if result.returncode != 0:
        log(f"  ERROR starting containers: {result.stderr[-300:]}")
        return False
    return True


def docker_compose_down(arch_dir):
    full_path = os.path.join(BASE_DIR, arch_dir)
    subprocess.run(
        "docker-compose down",
        cwd=full_path, shell=True,
        capture_output=True, text=True
    )


def wait_for_health(timeout=120):
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get("http://localhost:5000/health", timeout=3)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(3)
    return False


def load_data(arch_dir, total):
    full_path = os.path.join(BASE_DIR, arch_dir)
    log(f"  Loading {total:,} records...")
    start = time.time()
    result = subprocess.run(
        f'python -c "import data_loader; data_loader.load_data(total={total})"',
        cwd=full_path, shell=True,
        capture_output=True, text=True,
        timeout=3600   # 1 hour max for large loads
    )
    elapsed = time.time() - start
    log(f"  Data loaded in {elapsed:.0f}s")
    if result.returncode != 0:
        log(f"  WARNING: data_loader reported an error")
        for line in (result.stderr or "").strip().split("\n")[-3:]:
            log(f"    {line[:150]}")
        return False
    for line in (result.stdout or "").strip().split("\n")[-5:]:
        log(f"    {line[:150]}")
    return True


def run_queries(num_queries, max_workers, actual_count):
    """
    Fire num_queries GET requests against localhost:5000/get/<id>
    using random IDs within the loaded dataset.
    Returns a dict of timing stats, or None on failure.
    """
    session = requests.Session()

    def get_student_timed(student_id):
        t0 = time.time()
        try:
            session.get(f"http://localhost:5000/get/{student_id}", timeout=300)
        except Exception:
            pass
        return (time.time() - t0) * 1000   # ms

    if actual_count == 0:
        log("  ERROR: No records in DB — cannot benchmark.")
        return None

    max_id = actual_count
    random_ids = [random.randint(1, max_id) for _ in range(num_queries)]
    query_times = []

    log(f"  Firing {num_queries} queries with {max_workers} workers...")
    wall_start = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_student_timed, sid): sid for sid in random_ids}
        done = 0
        for future in as_completed(futures):
            query_times.append(future.result())
            done += 1
            if done % max(100, num_queries // 10) == 0:
                log(f"    {done}/{num_queries} done...")

    wall_elapsed = time.time() - wall_start
    avg    = statistics.mean(query_times)
    median = statistics.median(query_times)
    log(f"  Finished in {wall_elapsed:.0f}s | avg={avg:.1f}ms  median={median:.1f}ms")

    return {
        "num_queries":  num_queries,
        "avg_ms":       round(avg, 2),
        "median_ms":    round(median, 2),
        "min_ms":       round(min(query_times), 2),
        "max_ms":       round(max(query_times), 2),
        "p95_ms":       round(sorted(query_times)[int(len(query_times) * 0.95)], 2),
        "workers":      max_workers,
        "wall_time_s":  round(wall_elapsed, 2),
    }


def get_db_count():
    """Ask the central server how many records are currently loaded."""
    try:
        r = requests.get("http://localhost:5000/count", timeout=15)
        data = r.json()
        return data.get("total_count", data.get("count", 0))
    except Exception:
        return 0


def load_existing_results():
    if os.path.exists(RESULTS_FILE):
        try:
            with open(RESULTS_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "metadata": {
            "experiment":    "vary_queries",
            "data_sizes":    DATA_SIZES,
            "query_counts":  QUERY_COUNTS,
            "workers":       MAX_WORKERS,
        },
        "results": {}
    }


def save_results(data):
    data["metadata"]["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(RESULTS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    log(f"  Results saved → {RESULTS_FILE}")


def print_summary(all_data):
    results = all_data["results"]
    print("\n" + "=" * 100)
    print("  GRAPH-1 RESULTS: Avg query time (ms) — Fixed load, Varying query count")
    print("=" * 100)

    for size in DATA_SIZES:
        size_label = f"{size // 1_000_000}M records"
        size_key   = str(size)
        print(f"\n  Data size: {size_label}")
        print(f"  {'Queries':<10}", end="")
        for arch in ALL_ARCHITECTURES:
            print(f"| {arch['label']:^18}", end="")
        print("|")
        print("  " + "-" * 96)

        for q in QUERY_COUNTS:
            q_key = str(q)
            print(f"  {q:<10}", end="")
            for arch in ALL_ARCHITECTURES:
                a_key = arch["name"]
                try:
                    val = results[a_key][size_key][q_key]["avg_ms"]
                    print(f"| {val:>14.1f} ms", end="")
                except (KeyError, TypeError):
                    print(f"|       {'N/A':>8}   ", end="")
            print("|")

    print("=" * 100)


# ──────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark: fixed data load, vary query count (Graph 1)"
    )
    parser.add_argument(
        "--arch",
        choices=[a["name"] for a in ALL_ARCHITECTURES],
        default=None,
        help="Run only ONE architecture (omit to run all 5)"
    )
    args = parser.parse_args()

    # Decide which architectures to run
    if args.arch:
        architectures = [a for a in ALL_ARCHITECTURES if a["name"] == args.arch]
        print(f"\n  Running SINGLE architecture: {architectures[0]['label']}")
    else:
        architectures = ALL_ARCHITECTURES
        print(f"\n  Running ALL {len(architectures)} architectures")

    print("\n" + "=" * 60)
    print("  📊 BENCHMARK — Graph 1: Vary Query Count")
    print("=" * 60)
    print(f"  Data sizes  : {', '.join(f'{s//1_000_000}M' for s in DATA_SIZES)}")
    print(f"  Query counts: {QUERY_COUNTS}")
    print(f"  Workers     : {MAX_WORKERS}")
    print("=" * 60)

    # Step 1: Clean slate
    stop_all_containers()
    blocked = verify_ports_free()
    if blocked:
        log(f"⚠️  Ports still busy: {blocked} — waiting 10s...")
        time.sleep(10)

    all_data = load_existing_results()
    run_start = time.time()

    for arch in architectures:
        arch_name  = arch["name"]
        arch_label = arch["label"]

        for size in DATA_SIZES:
            size_key   = str(size)
            size_label = f"{size // 1_000_000}M"

            # Check if ALL query counts for this (arch, size) combo are already done
            existing_q = all_data["results"].get(arch_name, {}).get(size_key, {})
            remaining_q = [q for q in QUERY_COUNTS if str(q) not in existing_q]
            if not remaining_q:
                log(f"[SKIP] {arch_label} @ {size_label} — all query counts already done")
                continue

            print(f"\n{'━' * 60}")
            log(f"🏗  {arch_label}  |  data={size_label}  |  queries left={remaining_q}")
            print(f"{'━' * 60}")

            # Stop everything, verify ports, bring up this arch
            stop_all_containers()
            blocked = verify_ports_free()
            if blocked:
                log(f"  Ports {blocked} still busy — waiting 10s...")
                time.sleep(10)

            if not docker_compose_up(arch["dir"]):
                log("  FAILED to start containers — skipping this combo")
                continue

            log("  Waiting for central server health check...")
            if not wait_for_health(timeout=120):
                log("  TIMEOUT — services did not start. Skipping.")
                docker_compose_down(arch["dir"])
                continue

            log("  ✅ Services are up!")
            time.sleep(5)

            # Load data for this size
            load_data(arch["dir"], size)
            time.sleep(5)

            # Verify record count
            actual_count = get_db_count()
            log(f"  DB confirmed {actual_count:,} records")
            if actual_count == 0:
                log("  No records found — skipping benchmark for this combo")
                docker_compose_down(arch["dir"])
                continue

            # Now run each query count that still needs to be done
            for num_q in remaining_q:
                q_key = str(num_q)
                print(f"\n  --- {arch_label} @ {size_label} | {num_q} queries ---")

                try:
                    result = run_queries(num_q, MAX_WORKERS, actual_count)
                    if result:
                        all_data["results"].setdefault(arch_name, {}).setdefault(size_key, {})[q_key] = result
                        save_results(all_data)
                        log(f"  ✅  avg={result['avg_ms']:.1f}ms  median={result['median_ms']:.1f}ms")
                except Exception as exc:
                    log(f"  ❌ Error: {exc}")

            docker_compose_down(arch["dir"])
            time.sleep(3)

    total_time = time.time() - run_start
    log(f"\n🏁 Done! Total time: {total_time / 60:.1f} minutes")
    print_summary(all_data)
    save_results(all_data)
    stop_all_containers()


if __name__ == "__main__":
    main()
