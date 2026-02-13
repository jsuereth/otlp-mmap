import threading
import time
from typing import List, Callable

class Collector:
    def __init__(self, interval: float = 30.0):
        self._interval = interval
        self._callbacks: List[Callable[[], None]] = []
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._stop_event = threading.Event()

    def start(self):
        if not self._thread.is_alive():
            self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread.is_alive():
            self._thread.join()

    def register_callback(self, callback: Callable[[], None]):
        with self._lock:
            self._callbacks.append(callback)

    def _run(self):
        while not self._stop_event.is_set():
            start_time = time.time()
            self._collect()
            elapsed = time.time() - start_time
            sleep_time = max(0, self._interval - elapsed)
            self._stop_event.wait(sleep_time)

    def _collect(self):
        with self._lock:
            callbacks = list(self._callbacks)

        for callback in callbacks:
            try:
                callback()
            except Exception:
                # TODO: proper logging
                pass