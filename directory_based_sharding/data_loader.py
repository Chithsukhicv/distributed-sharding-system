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

LOOKUP_URI = os.environ.get('MONGO_URI_LOOKUP', 'mongodb://localhost:27021/')

shards = {
    0: MongoClient(MONGO_URIS[0])["shard_1_db"]["students"],
    1: MongoClient(MONGO_URIS[1])["shard_2_db"]["students"],
    2: MongoClient(MONGO_URIS[2])["shard_3_db"]["students"]
}

lookup_collection = MongoClient(LOOKUP_URI)["directory_db"]["lookup"]

departments = ["CSE", "ECE", "ME", "Civil", "Chemical", "IT", "EEE"]
# NOTE: shard_cycle is reset inside load_data() to avoid stale state
# when the function is called via: python -c "import data_loader; data_loader.load_data()"


def generate_student(student_id):
    return {
        "student_id": student_id,
        "name": fake.name(),
        "department": random.choice(departments),
        "age": random.randint(18, 25),
        "cgpa": round(random.uniform(5.0, 10.0), 1)
    }


def load_data(total=10_000_000, batch_size=10_000):
    # Reset cycle every call so shard assignment always starts from 0
    shard_cycle = itertools.cycle([0, 1, 2])
    # Drop old data and indexes
    print("Dropping old collections and indexes...")
    for shard in shards.values():
        shard.drop()
    lookup_collection.drop()

    print(f"Starting DIRECTORY BASED sharded insertion of {total:,} records...")
    start_time = time.time()

    batches = {0: [], 1: [], 2: []}
    lookup_batch = []

    for student_id in range(1, total + 1):
        shard = next(shard_cycle)
        batches[shard].append(generate_student(student_id))
        lookup_batch.append({"student_id": student_id, "shard": shard})

        # Insert chunks to the backend databases
        if len(batches[shard]) >= batch_size:
            shards[shard].insert_many(batches[shard], ordered=False)
            batches[shard] = []

        # Insert chunks to the directory phonebook database
        if len(lookup_batch) >= batch_size:
            lookup_collection.insert_many(lookup_batch, ordered=False)
            lookup_batch = []

        if student_id % 100_000 == 0:
            elapsed = time.time() - start_time
            print(f"  Processed {student_id:,} students in {elapsed:.1f} seconds")

    # Insert any remaining records
    for shard_num, batch in batches.items():
        if batch:
            shards[shard_num].insert_many(batch, ordered=False)
    if lookup_batch:
        lookup_collection.insert_many(lookup_batch, ordered=False)

    total_time = time.time() - start_time
    print(f"\nDone! Time taken: {total_time:.1f} seconds")
    print(f"\nShard breakdown:")
    for shard_num, collection in shards.items():
        count = collection.count_documents({})
        print(f"  shard_{shard_num + 1}_db: {count:,} students")
    print(f"  directory_db (lookup): {lookup_collection.count_documents({}):,} mappings")


if __name__ == '__main__':
    load_data(total=10_000_000)
