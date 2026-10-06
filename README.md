# Replicated Counter Service (Labs 2-5 & Midterm)

A distributed, fault-tolerant replicated counter service built with Python and gRPC, instrumented with Lamport logical clocks, and featuring majority quorum replication (2-of-3).

---

## 1. Setup and Environment

```bash
# Clone repository and enter directory
git clone https://github.com/in0w4k/dcc-assignment2
cd dcc-assignment2

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# Windows:                              # Linux / macOS:
.\.venv\Scripts\Activate.ps1            # source .venv/bin/activate

# Install dependencies
pip install grpcio grpcio-tools pytest
```

## 2. Running a Single Replica (Part A & B)
Start the server:

```bash
python server.py --port 50051 --node-id replica-A
```

In a second terminal:

```bash
# Increment counter
python client.py --servers localhost:50051 incr likes:post-42 --by 5

# Read counter value
python client.py --servers localhost:50051 get likes:post-42

# Idempotency test (same key returns duplicate=yes, counter does not increase twice)
python client.py --servers localhost:50051 incr likes:post-42 --by 5 --key test-key-1
python client.py --servers localhost:50051 incr likes:post-42 --by 5 --key test-key-1
```

## 3. Running Three-Replica Quorum (Part C1)
Start three independent server instances in separate terminals:

```bash
# Terminal 1
python server.py --port 50051 --node-id replica-A

# Terminal 2
python server.py --port 50052 --node-id replica-B

# Terminal 3
python server.py --port 50053 --node-id replica-C
```

In a fourth terminal, run the quorum client:

```bash
# Increments all 3 replicas in parallel, commits if >= 2 acknowledge
python client.py incr likes:post-42 --by 1
```

## 4. Running the Test Suite (Part C2 & C3)
Runs all 11 unit, integration, and failure-injection tests:

```bash
python -m pytest -v
```

## 5. Running the Performance Benchmark (Task C4)
Measures median and p95 latency across 4 configurations (2,000 requests each):

```bash
python benchmark.py
```