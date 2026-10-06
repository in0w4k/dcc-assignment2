import socket
import threading
import time
from concurrent import futures
import pytest
import grpc

import counter_pb2
import counter_pb2_grpc
from server import CounterServicer
from client import CounterClient


def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


def start_server_instance(port, node_id="replica", delay_ms=0, fault=None):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    servicer = CounterServicer(node_id=node_id, delay_ms=delay_ms, fault=fault)
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()
    return server, servicer


# TESTS

def test_failure_replica_crash_mid_request():
    ports = [get_free_port() for _ in range(3)]
    servers = [start_server_instance(p)[0] for p in ports]
    client = CounterClient(replicas=[f"localhost:{p}" for p in ports], client_id="c")

    def killer():
        time.sleep(0.01)
        servers[2].stop(0)

    t = threading.Thread(target=killer)
    t.start()

    res = client.incr("x", delta=1)
    t.join()

    assert res["success"] is True
    assert res["acked"] >= 2
    assert res["new_value"] == 1

    client.close()
    for s in servers:
        s.stop(0)


def test_failure_request_duplication():
    ports = [get_free_port() for _ in range(3)]
    servers = [start_server_instance(p)[0] for p in ports]
    client = CounterClient(replicas=[f"localhost:{p}" for p in ports], client_id="c")

    key = "same-key-42"
    r1 = client.incr("x", delta=5, key=key)
    r2 = client.incr("x", delta=5, key=key)

    assert r1["new_value"] == 5
    assert r2["new_value"] == 5
    assert r2["was_duplicate"] is True
    assert client.get("x").value == 5

    client.close()
    for s in servers:
        s.stop(0)


def test_failure_induced_timeout_with_retry():
    ports = [get_free_port() for _ in range(3)]
    servers = [
        start_server_instance(ports[0], delay_ms=0)[0],
        start_server_instance(ports[1], delay_ms=0)[0],
        start_server_instance(ports[2], delay_ms=2500)[0]
    ]

    client = CounterClient(replicas=[f"localhost:{p}" for p in ports], client_id="c")

    res = client.incr("x", delta=1, key="key-timeout")

    assert res["success"] is True
    assert res["acked"] == 2
    assert res["new_value"] == 1
    assert client.get("x").value == 1

    client.close()
    for s in servers:
        s.stop(0)