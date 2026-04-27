# Pure Storage EmpowerMe Mentorship Project – Database Sharding Simulation

## Overview

This project is being developed as part of EmpowerMe mentorship program to understand core distributed systems concepts through implementation and experimentation

The focus of the project is to compare a traditional single database setup with a sharded database architecture, analyze performance behavior, and identify bottlenecks in request routing.

## Objectives

- Understand how database sharding works
- Compare single database vs sharded database systems
- Measure query performance under different workloads
- Identify system bottlenecks such as central server limitations
- Explore concurrency and asynchronous request handling
- Use Docker for containerized deployment

## Project Structure

## Project Structure

```text
pure_storage_sharding_project/
│
├── single_db_instance/
│   ├── backend_server.py
│   ├── central_server.py
│   ├── client.py
│   ├── data_loader.py
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── requirements.txt
│
├── sharded_version/
│   ├── backend_server.py
│   ├── central_server.py 
│   ├── client.py
│   ├── data_loader.py
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── requirements.txt
│
├── .gitignore
└── README.md
```


## Implemented Components

### Single Database Version

* One MongoDB instance
* Backend API for data operations
* Central server for request routing
* Client script for testing query performance

### Sharded Version

* Data distributed across multiple shards
* Hash-based routing logic
* Central server for shard selection
* Concurrent request testing

## Key Learnings

* Sharding does not always reduce average latency for low workloads
* Central routing servers can become bottlenecks
* Concurrent traffic gives better insight than sequential testing
* System design choices directly affect scalability

## Technologies Used

* Python
* Flask
* MongoDB
* asyncio
* aiohttp
* concurrent.futures
* Docker

## Current Status

Project is under active development.

Planned improvements:

* Additional sharding strategies
* Better concurrency handling in central server
* Extended performance benchmarking
* Final presentation documentation

## How to Run

Install dependencies:

pip install -r requirements.txt

Run the required server files and client scripts from either:

* `single_db_instance/`
* `sharded_version/`

Docker setup can also be used through the provided Dockerfiles and docker-compose files.

## Author

Chithsukhi C V

