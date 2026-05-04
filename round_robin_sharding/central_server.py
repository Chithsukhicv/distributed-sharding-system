import os
import itertools
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

# Sequential counter for Round Robin
shard_cycle = itertools.cycle([0, 1, 2])

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "running", "role": "central_server_round_robin", "port": 5000})

@app.route('/put', methods=['POST'])
async def put_student():
    data = request.get_json()

    if not data:
        return jsonify({"status": "error", "message": "no data sent"}), 400

    if "student_id" not in data:
        return jsonify({"status": "error", "message": "student_id required"}), 400

    # ROUND ROBIN ROUTING: Pick the next shard in sequence!
    shard = next(shard_cycle)
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
    # SCATTER-GATHER LOGIC
    # Since placement was round robin, we don't know which shard holds the specific ID.
    # We must ask all 3 simultaneously.
    async def fetch_student(session, server):
        try:
            async with session.get(f"{server}/get/{student_id}") as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                return None
        except:
            return None

    try:
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_student(session, srv) for srv in BACKEND_SERVERS.values()]
            results = await asyncio.gather(*tasks)
            
            for result in results:
                if result is not None:
                    # Return the very first valid response we find
                    return jsonify(result), 200
                    
            # If all 3 shards return None, the student does not exist
            return jsonify({"status": "not found", "message": f"student {student_id} not found anywhere"}), 404

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
