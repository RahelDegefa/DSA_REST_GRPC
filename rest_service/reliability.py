import logging
import time
import grpc
from pybreaker import CircuitBreaker, CircuitBreakerListener, CircuitBreakerError

logger = logging.getLogger(__name__)

class CircuitBreakerLogListener(CircuitBreakerListener):
    """Logs explicit state changes for the circuit breaker."""
    def state_change(self, cb, old_state, new_state):
        logger.warning(
            "*** CIRCUIT BREAKER STATE CHANGE *** Name: '%s' | %s -> %s",
            cb.name, old_state.name.upper(), new_state.name.upper()
        )

breaker = CircuitBreaker(
    fail_max=3,
    reset_timeout=10,
    name='grpc-item-service',
    listeners=[CircuitBreakerLogListener()]
)

class BackendUnavailable(Exception):
    pass

def call_with_retries(operation, attempts=3, timeout_label='grpc'):
    delays = [0.0, 0.1, 0.2]
    last_error = None

    for index in range(attempts):
        if delays[index] > 0:
            time.sleep(delays[index])
        try:
            return operation()
        except grpc.RpcError as exc:
            last_error = exc
            logger.warning("%s attempt %s/%s failed: %s", timeout_label, index + 1, attempts, exc)

    raise BackendUnavailable(f"{timeout_label} failed after {attempts} attempts") from last_error

def protected_call(operation):
    return breaker.call(lambda: call_with_retries(operation))