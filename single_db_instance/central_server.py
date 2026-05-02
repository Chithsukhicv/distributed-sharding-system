from flask import Flask, request, jsonify
import aiohttp
import asyncio
import itertools
import os

app = Flask(__name__)

# 3 backend servers all pointing to same college_db
BACKEND_SERVERS = [
    os.environ.get('BACKEND_1', "http://localhost:5001"),
    os.environ.get('BACKEND_2', "http://localhost:5002"),
    os.environ.get('BACKEND_3', "http://localhost:5003")
]

server_cycle = itertools.cycle(BACKEND_SERVERS)

def get_next_server():
    return next(server_cycle)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        "status": "running",
        "role": "central_server_single_db",
        "port": 5000
    })

@app.route('/put', methods=['POST'])
async def put_student():
    data = request.get_json()

    if not data:
        return jsonify({"status": "error", "message": "no data sent"}), 400

    if "student_id" not in data:
        return jsonify({"status": "error", "message": "student_id required"}), 400

    server = get_next_server()

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{server}/put", json=data) as response:
                result = await response.json()
                return jsonify(result), response.status

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/get/<int:student_id>', methods=['GET'])
async def get_student(student_id):
    server = get_next_server()

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{server}/get/{student_id}") as response:
                result = await response.json()
                return jsonify(result), response.status

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/count', methods=['GET'])
async def count_students():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BACKEND_SERVERS[0]}/count") as response:
                result = await response.json()
                return jsonify(result), response.status

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)