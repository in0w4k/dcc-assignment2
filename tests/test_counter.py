import socket
import threading
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


@pytest.fixture
def cluster():
    ports = [get_free_port() for _ in range(3)]
    servers = []
    servicers = []

    for i, port in enumerate(ports):
        srv, scer = start_server_instance(port, node_id=f"replica-{chr(65+i)}")
        servers.append(srv)
        servicers.append(scer)

    replica_addresses = [f"localhost:{p}" for p in ports]
    client = CounterClient(replicas=replica_addresses, client_id="test-client")

    yield {
        "client": client,
        "servers": servers,
        "servicers": servicers,
        "ports": ports
    }

    client.close()
    for srv in servers:
        srv.stop(0)


# TESTS

def test_increment_applies_delta(cluster):
    client = cluster["client"]
    res = client.incr("x", delta=5)
    assert res["success"] is True
    assert res["new_value"] == 5
    assert client.get("x").value == 5


def test_duplicate_key_not_reapplied(cluster):
    client = cluster["client"]
    r1 = client.incr("x", delta=5, key="k-1")
    r2 = client.incr("x", delta=5, key="k-1")
    assert r1["new_value"] == 5
    assert r2["new_value"] == 5
    assert r2["was_duplicate"] is True
    assert client.get("x").value == 5


def test_concurrent_increments_exact(cluster):
    ports = cluster["ports"]
    c1 = CounterClient(replicas=[f"localhost:{p}" for p in ports], client_id="c1")
    c2 = CounterClient(replicas=[f"localhost:{p}" for p in ports], client_id="c2")

    def worker(client):
        for _ in range(1000):
            client.incr("x", delta=1)

    t1 = threading.Thread(target=worker, args=(c1,))
    t2 = threading.Thread(target=worker, args=(c2,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    c1.close()
    c2.close()
    assert cluster["client"].get("x").value == 2000


def test_get_missing_counter(cluster):
    client = cluster["client"]
    reply = client.get("missing_key")
    assert reply.found is False
    assert reply.value == 0


def test_retry_after_timeout_is_safe():
    port = get_free_port()
    srv, scer = start_server_instance(port, node_id="node-1")
    client = CounterClient(replicas=[f"localhost:{port}"], client_id="c")

    r1 = client.incr("x", delta=10, key="key-retry")
    r2 = client.incr("x", delta=10, key="key-retry")

    assert r1["success"] is True
    assert r1["new_value"] == 10
    assert r2["was_duplicate"] is True
    assert client.get("x").value == 10

    client.close()
    srv.stop(0)


def test_majority_commit_two_acks(cluster):
    client = cluster["client"]
    servers = cluster["servers"]

    servers[2].stop(0)

    res = client.incr("x", delta=1)
    assert res["success"] is True
    assert res["acked"] == 2
    assert res["new_value"] == 1


def test_no_commit_below_majority(cluster):
    client = cluster["client"]
    servers = cluster["servers"]

    servers[1].stop(0)
    servers[2].stop(0)

    res = client.incr("x", delta=1)
    assert res["success"] is False
    assert res["acked"] == 1
    assert "Quorum not reached" in res["error"]


def test_replicas_converge(cluster):
    client = cluster["client"]
    servicers = cluster["servicers"]

    for _ in range(5):
        client.incr("x", delta=2)

    val_a = servicers[0]._values.get("x")
    val_b = servicers[1]._values.get("x")
    val_c = servicers[2]._values.get("x")

    assert val_a == 10
    assert val_b == 10
    assert val_c == 10