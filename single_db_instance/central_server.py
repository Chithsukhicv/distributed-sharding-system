from flask import Flask, request, jsonify
import aiohttp
import asyncio
import os

app = Flask(__name__)

# Single backend server - no load balancing needed
BACKEND_SERVER = os.environ.get('BACKEND_1', "http://localhost:5001")

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

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{BACKEND_SERVER}/put", json=data) as response:
                result = await response.json()
                return jsonify(result), response.status

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/get/<int:student_id>', methods=['GET'])
async def get_student(student_id):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BACKEND_SERVER}/get/{student_id}") as response:
                result = await response.json()
                return jsonify(result), response.status

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/count', methods=['GET'])
async def count_students():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BACKEND_SERVER}/count") as response:
                result = await response.json()
                return jsonify(result), response.status

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)