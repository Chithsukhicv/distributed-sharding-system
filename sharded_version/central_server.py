import os
from flask import Flask, request, jsonify
import aiohttp
import asyncio

app = Flask(__name__)

# The three backend servers
BACKEND_SERVERS = {
    0: os.environ.get('BACKEND_1', "http://localhost:5001"),
    1: os.environ.get('BACKEND_2', "http://localhost:5002"),
    2: os.environ.get('BACKEND_3', "http://localhost:5003")
}

NUM_SHARDS = 3

def get_shard(student_id):
    return hash(student_id) % NUM_SHARDS

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "running", "role": "central_server", "port": 5000})

@app.route('/put', methods=['POST'])
async def put_student():
    data = request.get_json()

    if not data:
        return jsonify({"status": "error", "message": "no data sent"}), 400

    if "student_id" not in data:
        return jsonify({"status": "error", "message": "student_id required"}), 400

    student_id = data['student_id']
    shard = get_shard(student_id)
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
    shard = get_shard(student_id)
    server = BACKEND_SERVERS[shard]

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
        # Use asyncio.gather to fetch from all 3 shards SIMULTANEOUSLY!
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