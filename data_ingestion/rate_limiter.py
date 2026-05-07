import threading
import time

from logger import get_logger

log = get_logger(__name__)

# Umbral a partir del cual se registra la espera en el log (en segundos)
# Esperas cortas (<1s) son normales y no deben generar ruido
_LOG_THRESHOLD_SEC = 1.0


class RateLimiter:
    def __init__(self, max_calls: int, period: float, min_interval: float):
        self.max_calls = max_calls        # Max requests allowed within the time window
        self.period = period              # Duration of the sliding window in seconds
        self.min_interval = min_interval  # Minimum seconds between any two requests
        self.calls = []                   # Scheduled execution timestamps (not actual fire times)
        self.last_call_time = 0.0         # Tracks when the last request was (or will be) sent
        self.lock = threading.Lock()      # Ensures only one thread mutates state at a time

    def wait(self):
        """
        Blocks the calling thread just long enough to respect both the sliding
        window limit (max_calls / period) and the per-request minimum gap
        (min_interval). Threads that arrive concurrently are spaced out in FIFO
        order — each one reserves its execution slot before releasing the lock,
        so they never pile up and fire simultaneously.
        """
        sleep_time = 0.0
        throttled_by_window = False

        with self.lock:
            now = time.time()

            # Drop timestamps that are no longer inside the current sliding window.
            self.calls = [c for c in self.calls if now - c < self.period]

            # If the previous request was sent too recently, compute how long to
            # wait before this one can safely go out without triggering a burst.
            time_since_last = now - self.last_call_time
            if time_since_last < self.min_interval:
                sleep_time = self.min_interval - time_since_last

            # If all slots in the window are taken, wait until the oldest one
            # expires and a slot opens up.
            if len(self.calls) >= self.max_calls:
                window_sleep = self.period - (now - self.calls[0])
                if window_sleep > sleep_time:
                    sleep_time = window_sleep
                    throttled_by_window = True

            execution_time = now + sleep_time
            self.calls.append(execution_time)
            self.last_call_time = execution_time

        # Log throttling only when the wait is significant (>1s) to avoid noise
        if sleep_time >= _LOG_THRESHOLD_SEC:
            if throttled_by_window:
                log.warning(
                    "Rate limit reached (%d/%d req per %.0fs). Throttling for %.2fs...",
                    self.max_calls, self.max_calls, self.period, sleep_time
                )
            else:
                log.debug(
                    "Min interval enforced: waiting %.2fs before next request", sleep_time
                )

        if sleep_time > 0:
            time.sleep(sleep_time)