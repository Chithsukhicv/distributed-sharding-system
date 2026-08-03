import os
from pymongo import MongoClient, ASCENDING
from faker import Faker
import time
import itertools
import random

fake = Faker()

NUM_SHARDS = 3

MONGO_URIS = {
    0: os.environ.get('MONGO_URI_1', 'mongodb://localhost:27018/'),
    1: os.environ.get('MONGO_URI_2', 'mongodb://localhost:27019/'),
    2: os.environ.get('MONGO_URI_3', 'mongodb://localhost:27020/')
}

shards = {
    0: MongoClient(MONGO_URIS[0])["shard_1_db"]["students"],
    1: MongoClient(MONGO_URIS[1])["shard_2_db"]["students"],
    2: MongoClient(MONGO_URIS[2])["shard_3_db"]["students"]
}

# Index intentionally disabled for benchmarking
# for shard in shards.values():
#     shard.create_index([("student_id", ASCENDING)], unique=True)

departments = ["CSE", "ECE", "ME", "Civil", "Chemical", "IT", "EEE"]

# Sequential counter for Round Robin
shard_cycle = itertools.cycle([0, 1, 2])

def get_shard(student_id):
    # Pure Round Robin
    return next(shard_cycle)

def generate_student(student_id):
    return {
        "student_id": student_id,
        "name": fake.name(),
        "department": random.choice(departments),
        "age": random.randint(18, 25),
        "cgpa": round(random.uniform(5.0, 10.0), 1)
    }

def load_data(total=10_000_000, batch_size=10_000):
    # Drop old data
    print("Dropping old collections...")
    for shard in shards.values():
        shard.drop()
    print(f"Starting ROUND ROBIN sharded insertion of {total:,} records across {NUM_SHARDS} shards...")
    start_time = time.time()

    batches = {0: [], 1: [], 2: []}

    for student_id in range(1, total + 1):
        shard = get_shard(student_id)
        batches[shard].append(generate_student(student_id))

        if len(batches[shard]) >= batch_size:
            shards[shard].insert_many(batches[shard], ordered=False)
            batches[shard] = []

        if student_id % 100_000 == 0:
            elapsed = time.time() - start_time
            print(f"  Processed {student_id:,} students in {elapsed:.1f} seconds")

    for shard_num, batch in batches.items():
        if batch:
            shards[shard_num].insert_many(batch, ordered=False)

    total_time = time.time() - start_time

    print(f"\nDone! Time taken: {total_time:.1f} seconds")
    print(f"\nShard breakdown:")
    for shard_num, collection in shards.items():
        count = collection.count_documents({})
        print(f"  shard_{shard_num + 1}_db: {count:,} students")

if __name__ == '__main__':
    load_data(total=10_000_000)
