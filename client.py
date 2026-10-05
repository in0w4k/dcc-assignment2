import argparse
import time
import uuid
import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock

class CounterClient:
    def __init__(self, target="localhost:50051", client_id="client-1"):
        self.channel = grpc.insecure_channel(target)
        self.stub = counter_pb2_grpc.CounterStub(self.channel)
        self.client_id = client_id
        self.clock = LamportClock()
    
    def incr(self, counter_id, delta=1, key=None):
        if key is None:
            key = str(uuid.uuid4())

        backoff_delays = [0.2, 0.4, 0.8]
        attempt = 0

        while True:

            l_send = self.clock.tick()
            print(f"[{self.client_id}] SEND Increment(counter={counter_id}, delta={delta}) L={l_send}", flush=True)
            
            request = counter_pb2.IncrementRequest(
                counter_id = counter_id,
                delta = delta,
                idempotency_key = key,
                lamport_time = l_send
            )

            try:
                reply = self.stub.Increment(request, timeout=2.0)
                l_recv = self.clock.update(reply.lamport_time)

                print(f"[{self.client_id}] RECV IncrementReply(new_value={reply.new_value}) L={l_recv} (received L={reply.lamport_time})", flush=True)
                return reply

            except grpc.RpcError as error:
                status_code = error.code()
                if status_code in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE) and attempt < len(backoff_delays):
                    delay = backoff_delays[attempt]
                    attempt += 1
                    time.sleep(delay)
                    continue
                else:
                    raise error
    
    def get(self, counter_id):
        l_send = self.clock.tick()
        print(f"[{self.client_id}] SEND Get(counter={counter_id}) L={l_send}", flush=True)

        request = counter_pb2.GetRequest(
            counter_id = counter_id,
            lamport_time = l_send
        )
        reply = self.stub.Get(request, timeout=2.0)
        
        l_recv = self.clock.update(reply.lamport_time)
        print(f"[{self.client_id}] RECV GetReply(value={reply.value}) L={l_recv} (received L={reply.lamport_time})", flush=True)
        
        return reply
    
    def close(self):
        self.channel.close()

def main():
    parser = argparse.ArgumentParser(description="Counter gRPC client")
    parser.add_argument("--server", default="localhost:50051", help="Server address")
    parser.add_argument("--client-id", default="client-1", help="Client identifier")
    subparsers = parser.add_subparsers(dest="command", required=True)

    incr_parser = subparsers.add_parser("incr")
    incr_parser.add_argument("counter_id", type=str)
    incr_parser.add_argument("--by", type=int, default=1, dest="delta")
    incr_parser.add_argument("--key", type=str, default=None, help="Explicit idempotency key")
    
    get_parser = subparsers.add_parser("get")
    get_parser.add_argument("counter_id", type=str)

    args = parser.parse_args()
    client = CounterClient(args.server, client_id=args.client_id)

    try:
        if args.command == "incr":
            reply = client.incr(args.counter_id, delta=args.delta, key=args.key)
            dup_str = "yes" if reply.was_duplicate else "no"
            print(f"OK committed value={reply.new_value} (duplicate: {dup_str})")
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