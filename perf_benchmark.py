import socket
import time
import math
from concurrent import futures
import grpc

import counter_pb2
import counter_pb2_grpc
from server import CounterServicer
from client import CounterClient


def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


def start_server_instance(port, node_id="replica"):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    servicer = CounterServicer(node_id=node_id)
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()
    return server


def measure_config(num_replicas, num_clients, total_requests=2000):
    ports = [get_free_port() for _ in range(num_replicas)]
    servers = [start_server_instance(p, f"node-{i}") for i, p in enumerate(ports)]
    addresses = [f"localhost:{p}" for p in ports]

    client = CounterClient(replicas=addresses, client_id="bench")
    latencies_ms = []

    def send_one(i):
        start = time.perf_counter()
        client.incr(f"bench_x", delta=1)
        elapsed = (time.perf_counter() - start) * 1000.0
        return elapsed

    with futures.ThreadPoolExecutor(max_workers=num_clients) as executor:
        latencies_ms = list(executor.map(send_one, range(total_requests)))

    client.close()
    for s in servers:
        s.stop(0)

    latencies_ms.sort()
    n = len(latencies_ms)
    median = latencies_ms[n // 2]
    p95_idx = math.ceil(0.95 * n) - 1
    p95 = latencies_ms[p95_idx]

    return median, p95, total_requests


def main():
    results = []

    m, p, reqs = measure_config(num_replicas=1, num_clients=1)
    results.append(("Single replica, 1 client", m, p, reqs))

    m, p, reqs = measure_config(num_replicas=1, num_clients=16)
    results.append(("Single replica, 16 clients", m, p, reqs))

    m, p, reqs = measure_config(num_replicas=3, num_clients=1)
    results.append(("Quorum (3 replicas), 1 client", m, p, reqs))

    m, p, reqs = measure_config(num_replicas=3, num_clients=16)
    results.append(("Quorum (3 replicas), 16 clients", m, p, reqs))

    print("\n" + "="*90)
    print(f"{'Configuration':<35} | {'Median latency (ms)':<20} | {'p95 latency (ms)':<16} | {'Requests'}")
    print("-" * 90)
    for name, med, p95, reqs in results:
        print(f"{name:<35} | {med:<20.2f} | {p95:<16.2f} | {reqs}")
    print("="*90 + "\n")


if __name__ == "__main__":
    main()