import threading
import time

class RateLimiter:
    def __init__(self, max_calls: int, period: float, min_interval: float):
        self.max_calls = max_calls      # Max requests allowed within the time window
        self.period = period            # Duration of the sliding window in seconds
        self.min_interval = min_interval  # Minimum seconds between any two requests
        self.calls = []                 # Scheduled execution timestamps (not actual fire times)
        self.last_call_time = 0.0      # Tracks when the last request was (or will be) sent
        self.lock = threading.Lock()   # Ensures only one thread mutates state at a time

    def wait(self):
        """
        Blocks the calling thread just long enough to respect both the sliding
        window limit (max_calls / period) and the per-request minimum gap
        (min_interval). Threads that arrive concurrently are spaced out in FIFO
        order — each one reserves its execution slot before releasing the lock,
        so they never pile up and fire simultaneously.
        """
        sleep_time = 0.0

        with self.lock:
            now = time.time()

            # Drop timestamps that are no longer inside the current sliding window.
            # We compare against `now` (not the scheduled execution time) so that
            # slots genuinely in the past free up capacity for new requests.
            self.calls = [c for c in self.calls if now - c < self.period]

            # If the previous request was sent too recently, compute how long to
            # wait before this one can safely go out without triggering a burst.
            time_since_last = now - self.last_call_time
            if time_since_last < self.min_interval:
                sleep_time = self.min_interval - time_since_last

            # If all slots in the window are taken, wait until the oldest one
            # expires and a slot opens up. Take the larger of the two waits so
            # both constraints are satisfied simultaneously.
            if len(self.calls) >= self.max_calls:
                window_sleep = self.period - (now - self.calls[0])
                if window_sleep > sleep_time:
                    sleep_time = window_sleep

            # Reserve this thread's execution slot BEFORE releasing the lock.
            # Using the projected time (now + sleep_time) instead of `now` means
            # the next thread will see this slot as "occupied" and space itself
            # accordingly — preventing two threads from waking up at the same time.
            execution_time = now + sleep_time
            self.calls.append(execution_time)
            self.last_call_time = execution_time

        # Sleep after releasing the lock. Holding it during the sleep would force
        # every other thread to wait in line just to calculate its own delay —
        # turning parallelism into a serial queue and killing throughput.
        if sleep_time > 0:
            time.sleep(sleep_time)