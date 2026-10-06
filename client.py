import argparse
from concurrent import futures
import time
import uuid
import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


class CounterClient:
    def __init__(self, replicas=None, client_id="client-1"):
        if replicas is None:
            replicas = ["localhost:50051", "localhost:50052", "localhost:50053"]
        self.replicas = replicas
        self.client_id = client_id
        self.clock = LamportClock()
        
        self.channels = [grpc.insecure_channel(r) for r in self.replicas]
        self.stubs = [counter_pb2_grpc.CounterStub(ch) for ch in self.channels]

    def incr(self, counter_id, delta=1, key=None):
        if key is None:
            key = str(uuid.uuid4())

        l_send = self.clock.tick()
        print(f"[{self.client_id}] SEND Increment(counter={counter_id}, delta={delta}) L={l_send}", flush=True)

        request = counter_pb2.IncrementRequest(
            counter_id=counter_id,
            delta=delta,
            idempotency_key=key,
            lamport_time=l_send
        )

        def send_to_one(stub):
            try:
                return stub.Increment(request, timeout=2.0)
            except grpc.RpcError:
                return None

        with futures.ThreadPoolExecutor() as executor:
            responses = list(executor.map(send_to_one, self.stubs))

        successful_acks = [r for r in responses if r is not None]

        for r in successful_acks:
            self.clock.update(r.lamport_time)

        majority = (len(self.stubs) // 2) + 1
        acked_count = len(successful_acks)
        if acked_count >= majority:
            return {
                "success": True,
                "new_value": successful_acks[0].new_value,
                "was_duplicate": any(r.was_duplicate for r in successful_acks),
                "acked": acked_count,
                "total": len(self.stubs)
            }
        else:
            return {
                "success": False,
                "error": "Quorum not reached",
                "acked": acked_count,
                "total": len(self.stubs)
            }

    def get(self, counter_id):
        l_send = self.clock.tick()
        print(f"[{self.client_id}] SEND Get(counter={counter_id}) L={l_send}", flush=True)

        request = counter_pb2.GetRequest(counter_id=counter_id, lamport_time=l_send)
        reply = self.stubs[0].Get(request, timeout=2.0)
        self.clock.update(reply.lamport_time)
        return reply

    def close(self):
        for ch in self.channels:
            ch.close()


def main():
    parser = argparse.ArgumentParser(description="Counter gRPC client")
    parser.add_argument("--replicas", default="localhost:50051,localhost:50052,localhost:50053")
    parser.add_argument("--client-id", default="client-1")
    subparsers = parser.add_subparsers(dest="command", required=True)

    incr_parser = subparsers.add_parser("incr")
    incr_parser.add_argument("counter_id", type=str)
    incr_parser.add_argument("--by", type=int, default=1, dest="delta")
    incr_parser.add_argument("--key", type=str, default=None)

    get_parser = subparsers.add_parser("get")
    get_parser.add_argument("counter_id", type=str)

    args = parser.parse_args()
    replicas_list = [r.strip() for r in args.replicas.split(",") if r.strip()]
    client = CounterClient(replicas=replicas_list, client_id=args.client_id)

    try:
        if args.command == "incr":
            res = client.incr(args.counter_id, delta=args.delta, key=args.key)
            if res["success"]:
                dup_str = "yes" if res["was_duplicate"] else "no"
                print(f"OK committed value={res['new_value']} (replicas acked: {res['acked']}/{res['total']}, duplicate: {dup_str})")
            else:
                print(f"FAIL: {res['error']} (replicas acked: {res['acked']}/{res['total']})")
        elif args.command == "get":
            reply = client.get(args.counter_id)
            if reply.found:
                print(f"value={reply.value}")
            else:
                print("value=0 (not found)")
    finally:
        client.close()


if __name__ == "__main__":
    main()