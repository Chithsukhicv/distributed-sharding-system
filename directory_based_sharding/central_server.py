import os
import itertools
import threading
from flask import Flask, request, jsonify
import aiohttp
import asyncio
from pymongo import MongoClient

app = Flask(__name__)

BACKEND_SERVERS = {
    0: os.environ.get('BACKEND_1', "http://localhost:5001"),
    1: os.environ.get('BACKEND_2', "http://localhost:5002"),
    2: os.environ.get('BACKEND_3', "http://localhost:5003"),
}

NUM_SHARDS = 3

# Lookup DB ("phonebook") — stores {student_id -> shard} mappings
LOOKUP_URI = os.environ.get('MONGO_URI_LOOKUP', 'mongodb://localhost:27021/')
lookup_client = MongoClient(LOOKUP_URI)
lookup_collection = lookup_client["directory_db"]["lookup"]

# Round-robin assignment for new students
_cycle = itertools.cycle([0, 1, 2])
_cycle_lock = threading.Lock()


def _next_shard():
    with _cycle_lock:
        return next(_cycle)


@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "running", "role": "central_server_directory", "port": 5000})


@app.route('/put', methods=['POST'])
async def put_student():
    data = request.get_json()
    if not data or "student_id" not in data:
        return jsonify({"status": "error", "message": "student_id required"}), 400

    student_id = data["student_id"]
    shard = _next_shard()

    # Store the mapping in the directory
    try:
        lookup_collection.update_one(
            {"student_id": student_id},
            {"$set": {"shard": shard}},
            upsert=True,
        )
    except Exception as e:
        return jsonify({"status": "error", "message": f"lookup db error: {e}"}), 500

    server = BACKEND_SERVERS[shard]
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{server}/put", json=data) as response:
                result = await response.json()
                return jsonify(result), response.status
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/get/<int:student_id>', methods=['GET'])
async def get_student(student_id):
    # Directory routing: 1 lookup hop, then 1 shard hop. No scatter-gather.
    mapping = lookup_collection.find_one({"student_id": student_id}, {"_id": 0, "shard": 1})
    if not mapping:
        return jsonify({"status": "not found", "message": f"student {student_id} not in directory"}), 404

    server = BACKEND_SERVERS[mapping["shard"]]
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{server}/get/{student_id}") as response:
                result = await response.json()
                return jsonify(result), response.status
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/count', methods=['GET'])
async def count_all():
    async def fetch_count(session, shard_num, server):
        try:
            async with session.get(f"{server}/count") as response:
                data = await response.json()
                return shard_num, data['count']
        except Exception as e:
            return shard_num, f"error: {str(e)}"

    total = 0
    shard_counts = {}

    try:
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_count(session, num, srv) for num, srv in BACKEND_SERVERS.items()]
            results = await asyncio.gather(*tasks)

            for shard_num, count in results:
                shard_counts[f"shard_{shard_num}"] = count
                if isinstance(count, int):
                    total += count

        return jsonify({
            "status": "success",
            "total_count": total,
            "shard_breakdown": shard_counts
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
