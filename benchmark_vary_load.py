"""
benchmark_vary_load.py
======================
Graph 2 — Vary Data Load, Fixed Query Count
-------------------------------------------
Data sizes   : 1M, 3M, 5M  records  (X-axis)
Query count  : 200  (fixed, fast but statistically solid)
Workers      : 10
Result JSON  : results_vary_load.json
Output graph : graph2_vary_load.png

⚡ TIME SAVING TRICK:
  Containers stay UP between data sizes for the SAME architecture.
  We only rebuild when switching to a new architecture.
  This saves ~4-5 min per data size transition.

Usage:
  python benchmark_vary_load.py --arch single_db
  python benchmark_vary_load.py --arch hash_sharded
  python benchmark_vary_load.py --arch random_sharded
  python benchmark_vary_load.py --arch round_robin
  python benchmark_vary_load.py --arch directory_based
  python benchmark_vary_load.py          # run ALL 5
"""

import subprocess, time, os, json, random, statistics, socket, argparse, requests
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
RESULTS_FILE = os.path.join(BASE_DIR, "results_vary_load.json")
OUT_GRAPH    = os.path.join(BASE_DIR, "graph2_vary_load.png")

# ✂️ Only 3 data sizes — drops the two heaviest (7M and 10M).
# The trend is clear with 3 points and saves ~40% of total run time.
DATA_SIZES  = [1_000_000, 3_000_000, 5_000_000]
NUM_QUERIES = 200    # reduced from 500 — still statistically meaningful
MAX_WORKERS = 10

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
    """
    Call data_loader directly — it drops and re-inserts data in the
    already-running containers. No docker restart needed!
    """
    log(f"  Reloading data: {total:,} records (containers stay UP)...")
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
    for ln in (r.stdout or "").strip().split("\n")[-4:]:
        log(f"    {ln[:150]}")
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

    ids   = [random.randint(1, count) for _ in range(num_q)]
    times = []
    log(f"  Firing {num_q} queries ({max_w} workers)...")
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=max_w) as ex:
        futs = {ex.submit(timed, sid): sid for sid in ids}
        done = 0
        for f in as_completed(futs):
            times.append(f.result())
            done += 1
            if done % 50 == 0:
                log(f"    {done}/{num_q} done...")
    wall = time.time() - t0
    avg  = statistics.mean(times)
    log(f"  Done {wall:.0f}s | avg={avg:.1f}ms")
    return {
        "record_count": count,
        "num_queries":  num_q,
        "avg_ms":       round(avg, 2),
        "median_ms":    round(statistics.median(times), 2),
        "min_ms":       round(min(times), 2),
        "max_ms":       round(max(times), 2),
        "p95_ms":       round(sorted(times)[int(len(times)*0.95)], 2),
    }

def load_existing():
    if os.path.exists(RESULTS_FILE):
        try:
            with open(RESULTS_FILE) as f: return json.load(f)
        except Exception: pass
    return {"metadata": {"data_sizes": DATA_SIZES, "num_queries": NUM_QUERIES,
                          "workers": MAX_WORKERS}, "results": {}}

def save(data):
    data["metadata"]["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(RESULTS_FILE, "w") as f: json.dump(data, f, indent=2)
    log(f"  Saved → {RESULTS_FILE}")

# ── PLOT ─────────────────────────────────────────────────────────────────────
def plot(all_data):
    results    = all_data["results"]
    data_sizes = sorted(all_data["metadata"].get("data_sizes", DATA_SIZES))
    x_labels   = [f"{s // 1_000_000}M" for s in data_sizes]
    x_pos      = np.arange(len(data_sizes))

    fig, ax = plt.subplots(figsize=(11, 6))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(PANEL)

    any_plotted = False
    for a in ALL_ARCHITECTURES:
        s    = ARCH_STYLE[a["name"]]
        arch = results.get(a["name"], {})
        xs, ys = [], []
        for i, size in enumerate(data_sizes):
            entry = arch.get(str(size))
            if entry and "avg_ms" in entry:
                xs.append(i); ys.append(entry["avg_ms"])
        if not xs: continue
        ax.plot(xs, ys, label=a["label"], color=s["color"],
                marker=s["marker"], linestyle=s["ls"],
                linewidth=2.2, markersize=8,
                markeredgecolor="white", markeredgewidth=0.5)
        for xi, yi in zip(xs, ys):
            ax.annotate(f"{yi:.0f}ms", xy=(xi, yi), xytext=(0, 9),
                        textcoords="offset points", ha="center",
                        fontsize=8, color=s["color"])
        any_plotted = True

    # Shaded gain region between Single DB and best sharded
    if any_plotted and "single_db" in results:
        single_ys, best_ys, shade_xs = [], [], []
        for i, size in enumerate(data_sizes):
            sv = results["single_db"].get(str(size), {}).get("avg_ms")
            candidates = [
                results.get(a["name"], {}).get(str(size), {}).get("avg_ms")
                for a in ALL_ARCHITECTURES if a["name"] != "single_db"
            ]
            candidates = [c for c in candidates if c is not None]
            if sv is not None and candidates:
                shade_xs.append(i); single_ys.append(sv); best_ys.append(min(candidates))
        if shade_xs:
            ax.fill_between(shade_xs, best_ys, single_ys,
                            alpha=0.09, color="#38BDF8", label="Sharding gain region")

    if not any_plotted:
        ax.text(0.5, 0.5, "No data yet\nRun benchmark_vary_load.py",
                transform=ax.transAxes, ha="center", va="center",
                color=TXT, fontsize=13)

    ax.set_title(f"Graph 2 — Data Load vs Avg Latency  ({NUM_QUERIES} queries, {MAX_WORKERS} workers)",
                 color="#F8FAFC", fontsize=14, fontweight="bold", pad=14)
    ax.set_xlabel("Data Loaded into Database", color=TXT, fontsize=12)
    ax.set_ylabel("Average Query Time (ms)", color=TXT, fontsize=12)
    ax.tick_params(colors=TXT, labelsize=11)
    ax.set_xticks(x_pos); ax.set_xticklabels(x_labels, fontsize=11)
    for sp in ax.spines.values(): sp.set_edgecolor(GRID)
    ax.grid(True, color=GRID, linestyle="--", linewidth=0.6, alpha=0.7)
    ax.set_axisbelow(True)
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
    args  = parser.parse_args()
    archs = [a for a in ALL_ARCHITECTURES if a["name"] == args.arch] if args.arch else ALL_ARCHITECTURES

    print(f"\n{'='*60}")
    print(f"  📊 BENCHMARK 2 — Vary data load, fixed {NUM_QUERIES} queries")
    print(f"  Architectures : {[a['label'] for a in archs]}")
    print(f"  Data sizes    : {[f'{s//1_000_000}M' for s in DATA_SIZES]}")
    print(f"  ⚡ Containers stay UP between sizes (faster!)")
    print(f"{'='*60}\n")

    stop_all()
    if free_ports():
        log("Ports still busy — waiting 10s..."); time.sleep(10)

    all_data = load_existing()

    for arch in archs:
        name  = arch["name"]
        label = arch["label"]

        existing_sizes = set(all_data["results"].get(name, {}).keys())
        remaining      = [s for s in DATA_SIZES if str(s) not in existing_sizes]
        if not remaining:
            log(f"[SKIP] {label} — all sizes done"); continue

        print(f"\n{'━'*60}")
        log(f"🏗  {label} | sizes left: {[s//1_000_000 for s in remaining]}M")

        # ── Start containers ONCE for this architecture ────────────────────
        stop_all()
        if free_ports(): log("Ports busy — waiting 10s..."); time.sleep(10)
        if not compose_up(arch["dir"]): continue
        if not wait_healthy():
            log("  TIMEOUT — skipping this architecture")
            compose_down(arch["dir"]); continue
        log("  ✅ Services up!")

        # ── Loop through data sizes WITHOUT restarting Docker ──────────────
        for size in remaining:
            size_key   = str(size)
            size_label = f"{size // 1_000_000}M"
            print(f"\n  --- {label} @ {size_label} records ---")

            time.sleep(3)
            # data_loader drops old data and reloads — containers stay running
            if not load_data(arch["dir"], size):
                log("  Data load issue — trying benchmark anyway")
            time.sleep(3)

            count = get_count()
            log(f"  DB confirmed {count:,} records")
            if count == 0:
                log("  No records — skipping this size")
                continue

            try:
                result = run_queries(NUM_QUERIES, MAX_WORKERS, count)
                all_data["results"].setdefault(name, {})[size_key] = result
                save(all_data)
                log(f"  ✅ {label} @ {size_label}: avg={result['avg_ms']:.1f}ms")
            except Exception as e:
                log(f"  ❌ {e}")

        # ── Tear down only when moving to the next architecture ────────────
        compose_down(arch["dir"])
        log(f"  Containers stopped for {label}")
        time.sleep(3)

    log("\n🏁 All done! Plotting graph...")
    plot(all_data)

if __name__ == "__main__":
    main()
