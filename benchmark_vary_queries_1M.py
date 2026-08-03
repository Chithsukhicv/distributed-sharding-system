"""
benchmark_vary_queries_1M.py
============================
Graph 1A — Fixed Data Load: 1 Million Records, Vary Query Count
---------------------------------------------------------------
Query counts : 100, 250, 500, 1000, 2000, 5000
Workers      : 10
Result JSON  : results_vary_queries_1M.json
Output graph : graph1A_vary_queries_1M.png

Usage:
  python benchmark_vary_queries_1M.py --arch single_db
  python benchmark_vary_queries_1M.py --arch hash_sharded
  python benchmark_vary_queries_1M.py --arch random_sharded
  python benchmark_vary_queries_1M.py --arch round_robin
  python benchmark_vary_queries_1M.py --arch directory_based
  python benchmark_vary_queries_1M.py          # run ALL 5
"""

import subprocess, time, os, json, random, statistics, socket, argparse, requests
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
RESULTS_FILE = os.path.join(BASE_DIR, "results_vary_queries_1M.json")
OUT_GRAPH    = os.path.join(BASE_DIR, "graph1A_vary_queries_1M.png")

DATA_SIZE    = 1_000_000
QUERY_COUNTS = [100, 250, 500, 1000, 2000, 5000]
MAX_WORKERS  = 10

ALL_ARCHITECTURES = [
    {"name": "single_db",       "dir": "single_db_instance",     "label": "Single DB"},
    {"name": "hash_sharded",    "dir": "sharded_version",         "label": "Hash Sharded"},
    {"name": "random_sharded",  "dir": "random_sharding",         "label": "Random Sharded"},
    {"name": "round_robin",     "dir": "round_robin_sharding",    "label": "Round Robin"},
    {"name": "directory_based", "dir": "directory_based_sharding","label": "Directory Based"},
]

ARCH_STYLE = {
    "single_db":       {"color": "#EF4444", "marker": "o",  "ls": "-"},
    "hash_sharded":    {"color": "#3B82F6", "marker": "s",  "ls": "-"},
    "random_sharded":  {"color": "#F59E0B", "marker": "^",  "ls": "--"},
    "round_robin":     {"color": "#10B981", "marker": "D",  "ls": "--"},
    "directory_based": {"color": "#8B5CF6", "marker": "P",  "ls": "-."},
}

BG    = "#0F172A"
PANEL = "#1E293B"
GRID  = "#334155"
TXT   = "#E2E8F0"

# ── HELPERS ───────────────────────────────────────────────────────────────────
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex(('localhost', port)) == 0

def stop_all():
    log("Stopping all containers...")
    for a in ALL_ARCHITECTURES:
        p = os.path.join(BASE_DIR, a["dir"])
        if os.path.exists(os.path.join(p, "docker-compose.yml")):
            subprocess.run("docker-compose down", cwd=p, shell=True,
                           capture_output=True, encoding='utf-8', errors='replace')
    time.sleep(5)

def free_ports():
    return [p for p in [5000, 27017, 27018, 27019, 27020, 27021] if is_port_in_use(p)]

def compose_up(arch_dir):
    p = os.path.join(BASE_DIR, arch_dir)
    log(f"  docker-compose up --build  ({arch_dir})")
    r = subprocess.run("docker-compose up -d --build", cwd=p, shell=True,
                       capture_output=True, encoding='utf-8', errors='replace', timeout=300)
    if r.returncode != 0:
        log(f"  ERROR: {r.stderr[-200:]}")
        return False
    return True

def compose_down(arch_dir):
    subprocess.run("docker-compose down",
                   cwd=os.path.join(BASE_DIR, arch_dir),
                   shell=True, capture_output=True, encoding='utf-8', errors='replace')

def wait_healthy(timeout=120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if requests.get("http://localhost:5000/health", timeout=3).status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(3)
    return False

def load_data(arch_dir, total):
    log(f"  Loading {total:,} records...")
    t0 = time.time()
    r = subprocess.run(
        f'python -c "import data_loader; data_loader.load_data(total={total})"',
        cwd=os.path.join(BASE_DIR, arch_dir), shell=True,
        capture_output=True, encoding='utf-8', errors='replace', timeout=3600)
    log(f"  Loaded in {time.time()-t0:.0f}s")
    if r.returncode != 0:
        for ln in (r.stderr or "").strip().split("\n")[-3:]:
            log(f"    {ln[:150]}")
        return False
    return True

def get_count():
    try:
        d = requests.get("http://localhost:5000/count", timeout=15).json()
        return d.get("total_count", d.get("count", 0))
    except Exception:
        return 0

def run_queries(num_q, max_w, count):
    sess = requests.Session()
    def timed(sid):
        t = time.time()
        try: sess.get(f"http://localhost:5000/get/{sid}", timeout=300)
        except Exception: pass
        return (time.time() - t) * 1000

    ids = [random.randint(1, count) for _ in range(num_q)]
    times = []
    log(f"  Firing {num_q} queries ({max_w} workers)...")
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=max_w) as ex:
        futs = {ex.submit(timed, sid): sid for sid in ids}
        done = 0
        for f in as_completed(futs):
            times.append(f.result())
            done += 1
            if done % max(50, num_q // 5) == 0:
                log(f"    {done}/{num_q} done...")
    wall = time.time() - t0
    avg = statistics.mean(times)
    log(f"  Done {wall:.0f}s | avg={avg:.1f}ms")
    return {
        "num_queries": num_q,
        "avg_ms":      round(avg, 2),
        "median_ms":   round(statistics.median(times), 2),
        "min_ms":      round(min(times), 2),
        "max_ms":      round(max(times), 2),
        "p95_ms":      round(sorted(times)[int(len(times)*0.95)], 2),
    }

def load_existing():
    if os.path.exists(RESULTS_FILE):
        try:
            with open(RESULTS_FILE) as f: return json.load(f)
        except Exception: pass
    return {"metadata": {"data_size": DATA_SIZE, "query_counts": QUERY_COUNTS,
                          "workers": MAX_WORKERS}, "results": {}}

def save(data):
    data["metadata"]["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(RESULTS_FILE, "w") as f: json.dump(data, f, indent=2)
    log(f"  Saved → {RESULTS_FILE}")

# ── PLOT ─────────────────────────────────────────────────────────────────────
def plot(all_data):
    results = all_data["results"]
    fig, ax = plt.subplots(figsize=(11, 6))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(PANEL)

    for a in ALL_ARCHITECTURES:
        s   = ARCH_STYLE[a["name"]]
        pts = results.get(a["name"], {})
        xs  = sorted([int(k) for k in pts], key=lambda x: x)
        ys  = [pts[str(x)]["avg_ms"] for x in xs if str(x) in pts]
        if not xs: continue
        ax.plot(xs, ys, label=a["label"], color=s["color"],
                marker=s["marker"], linestyle=s["ls"],
                linewidth=2.2, markersize=8,
                markeredgecolor="white", markeredgewidth=0.5)
        for x, y in zip(xs, ys):
            ax.annotate(f"{y:.0f}ms", xy=(x, y), xytext=(0, 9),
                        textcoords="offset points", ha="center",
                        fontsize=7.5, color=s["color"])

    ax.set_title("Graph 1A — Query Count vs Avg Latency  (1M Records Loaded)",
                 color="#F8FAFC", fontsize=14, fontweight="bold", pad=14)
    ax.set_xlabel("Number of Queries Fired", color=TXT, fontsize=12)
    ax.set_ylabel("Average Query Time (ms)", color=TXT, fontsize=12)
    ax.tick_params(colors=TXT, labelsize=10)
    for sp in ax.spines.values(): sp.set_edgecolor(GRID)
    ax.grid(True, color=GRID, linestyle="--", linewidth=0.6, alpha=0.7)
    ax.set_axisbelow(True)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x,_: f"{int(x):,}"))
    ax.legend(facecolor=PANEL, edgecolor=GRID, labelcolor=TXT,
              fontsize=9, framealpha=0.9, loc="upper left")

    plt.tight_layout()
    plt.savefig(OUT_GRAPH, dpi=150, bbox_inches="tight", facecolor=BG)
    print(f"\n✅  Graph saved → {OUT_GRAPH}")
    plt.close()

# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--arch", choices=[a["name"] for a in ALL_ARCHITECTURES],
                        default=None)
    args = parser.parse_args()
    archs = [a for a in ALL_ARCHITECTURES if a["name"] == args.arch] if args.arch else ALL_ARCHITECTURES

    print(f"\n{'='*55}")
    print(f"  📊 BENCHMARK 1A — 1M records, vary query count")
    print(f"  Architectures : {[a['label'] for a in archs]}")
    print(f"  Query counts  : {QUERY_COUNTS}")
    print(f"{'='*55}\n")

    stop_all()
    if free_ports():
        log("Ports still busy — waiting 10s..."); time.sleep(10)

    all_data = load_existing()

    for arch in archs:
        name  = arch["name"]
        label = arch["label"]
        existing_q = set(all_data["results"].get(name, {}).keys())
        remaining  = [q for q in QUERY_COUNTS if str(q) not in existing_q]
        if not remaining:
            log(f"[SKIP] {label} — all query counts done"); continue

        print(f"\n{'━'*55}")
        log(f"🏗  {label} | queries left: {remaining}")

        stop_all()
        if free_ports(): log("Ports busy — waiting 10s..."); time.sleep(10)
        if not compose_up(arch["dir"]): continue
        if not wait_healthy(): compose_down(arch["dir"]); continue
        log("  ✅ Services up!"); time.sleep(5)

        load_data(arch["dir"], DATA_SIZE)
        time.sleep(5)
        count = get_count()
        log(f"  DB has {count:,} records")
        if count == 0: compose_down(arch["dir"]); continue

        for q in remaining:
            try:
                result = run_queries(q, MAX_WORKERS, count)
                all_data["results"].setdefault(name, {})[str(q)] = result
                save(all_data)
            except Exception as e:
                log(f"  ❌ {e}")

        compose_down(arch["dir"]); time.sleep(3)

    log("\n🏁 All done! Plotting graph...")
    plot(all_data)

if __name__ == "__main__":
    main()
