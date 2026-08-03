import os
from flask import Flask, request, jsonify
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

app = Flask(__name__)

PORT = int(os.environ.get('PORT', '5001'))
MONGO_HOST = os.environ.get('MONGO_HOST', 'localhost')

# Always same DB - college_db
client = MongoClient(f"mongodb://{MONGO_HOST}:27017/")
db = client["college_db"]
collection = db["students"]

# Index intentionally disabled to simulate heavy analytical queries (Full Collection Scan)
# collection.create_index([("student_id", ASCENDING)], unique=True)

@app.route('/put', methods=['POST'])
def put_student():
    data = request.get_json()

    if not data:
        return jsonify({"status": "error", "message":"no data sent"}), 400
    
    if "student_id" not in data:
        return jsonify({"status":"error", "message":"student_id required"}), 400
    
    try:
        collection.insert_one(data)
        return jsonify({"status":"success", "message": f"student {data['student_id']} stored"}), 201
    
    except DuplicateKeyError:
        return jsonify({"status" :"error", "message":"student already exists"}), 409
    
    except Exception as e:
        return jsonify({"status":"error", "message": str(e)}), 500

@app.route('/get/<int:student_id>', methods=['GET'])
def get_student(student_id):
    try:
        student = collection.find_one(
            {"student_id":student_id},
            {"_id": 0}
        )  

        if student:
            return jsonify({"status":"success","data":student})
        else:
            return jsonify({"status":"not found", "message":f"student {student_id} not found"}), 404
        
    except Exception as e:
        return jsonify({"status":"error", "message":str(e)}), 500
    
@app.route('/count', methods=["GET"])
def count_students():
    count = collection.count_documents({})
    return jsonify({"status":"success", "count":count})

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "running", "port": PORT, "database": "college_db"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, debug=True)