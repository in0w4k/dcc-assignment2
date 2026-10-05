import threading
class LamportClock:
    def __init__(self, initial_time=0):
        self._lock = threading.Lock()
        self._time = initial_time
    
    def tick(self):
        with self._lock:
            self._time += 1
            return self._time
    
    def update(self, received_time):
        with self._lock:
            self._time = max(self._time, received_time) + 1
            return self._time
    
    def get_time(self):
        with self._lock:
            return self._time