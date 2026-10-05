from clocks import LamportClock
import argparse
import threading
import counter_pb2
import counter_pb2_grpc
import grpc

from concurrent import futures

class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, node_id="replica-A"):
        self._lock = threading.Lock()
        self._values = {}
        self._seen = {}
        self.node_id = node_id
        self.clock = LamportClock()
    
    def Increment(self, request, context):
        with self._lock:
            l_recv = self.clock.update(request.lamport_time)
            print(f"[{self.node_id}] RECV Increment(counter={request.counter_id}, delta={request.delta}) L={l_recv} (received L={request.lamport_time})", flush=True)
            
            if request.idempotency_key in self._seen:
                stored_value = self._seen[request.idempotency_key]
                l_send = self.clock.tick()
                print(f"[{self.node_id}] SEND IncrementReply(new_value{stored_value}) L={l_send}", flush=True)
                return counter_pb2.IncrementReply(
                    new_value = stored_value,
                    was_duplicate = True,
                    lamport_time = l_send
                )
            
            current_value = self._values.get(request.counter_id, 0)
            new_value = current_value + request.delta
            self._values[request.counter_id] = new_value
            self._seen[request.idempotency_key] = new_value
            l_apply = self.clock.tick()
            print(f"[{self.node_id}] APPLY counter={request.counter_id} -> {new_value} L={l_apply}", flush=True)

            l_send = self.clock.tick()
            print(f"[{self.node_id}] SEND IncrementReply(new_value={new_value}) L={l_send}", flush=True)
            return counter_pb2.IncrementReply(
                new_value = new_value,
                was_duplicate = False,
                lamport_time = l_send
            )
    
    def Get(self, request, context):
        with self._lock:
            l_recv = self.clock.update(request.lamport_time)
            print(f"[{self.node_id}] RECV Get(counter={request.counter_id}) L={l_recv} (received L={request.lamport_time})", flush=True)

            found = request.counter_id in self._values
            val = self._values.get(request.counter_id, 0)

            l_send = self.clock.tick()
            print(f"[{self.node_id}] SEND GetReply(value={val}, found={found}) L={l_send}", flush=True)

            return counter_pb2.GetReply(
                    value = val,
                    found = found,
                    lamport_time = l_send
            )


def serve(port=50051, node_id="replica-A"):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    servicer = CounterServicer(node_id=node_id)
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"Server {node_id} started on port {port}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051, help="Port to listen on")
    parser.add_argument("--node-id", type=str, default="replica-A", help="Replica identifier")
    args = parser.parse_args()
    serve(port=args.port, node_id=args.node_id)