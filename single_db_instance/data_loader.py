import random
import time
from pymongo import MongoClient, ASCENDING
from faker import Faker

fake = Faker()

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
collection = client["college_db"]["students"]

departments = ["CSE", "ECE", "ME", "Civil", "Chemical", "IT", "EEE"]

def generate_batch(start_id, batch_size):
    students = []
    for i in range(batch_size):
        student_id = start_id + i
        students.append({
            "student_id": student_id,
            "name": fake.name(),
            "department": random.choice(departments),
            "age": random.randint(18, 23),
            "cgpa": round(random.uniform(5.0, 10.0), 1)
        })
    return students

def load_data(total=1_000_000, batch_size=10_000):
    print(f"Starting insertion of {total:,} records...")
    start_time = time.time()
    
  
    for batch_start in range(2, total + 2, batch_size):
        batch = generate_batch(batch_start, batch_size)
        collection.insert_many(batch, ordered=False)
        
        inserted = batch_start + batch_size - 2
        if inserted % 100_000 == 0:
            elapsed = time.time() - start_time
            print(f"  Inserted {inserted:,} records in {elapsed:.1f} seconds")
    
    total_time = time.time() - start_time
    print(f"\nDone! {collection.count_documents({}):,} total records")
    print(f"Time taken: {total_time:.1f} seconds")

if __name__ == '__main__':
    load_data()