"""Common process lifecycle and sequential message dispatch for controls."""

import logging
import os
import signal

from common import middleware
from common.message_protocol import internal


def positive_setting(name):
    value = int(os.environ[name])
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def replica_queue(prefix, replica_id):
    return f"{prefix}_{replica_id}"


class QueueFilter:

    def __init__(self, host, input_queue):
        self.transport = middleware.MessageMiddlewareQueueRabbitMQ(host, input_queue)

    def start(self):
        self.transport.start_consuming(self._consume)

    def _consume(self, body, ack, nack):
        self.process_message(internal.deserialize(body))
        ack()

    def process_message(self, message):
        raise NotImplementedError

    def send(self, destination, message):
        self.transport.send_to_queue(destination, internal.serialize(message))

    def close(self):
        self.transport.close()


class _ShutdownRequested(BaseException):
    """Unwind the consumer loop without treating SIGTERM as a message error."""


def _request_shutdown(signum, frame):
    # Avoid reentrant broker operations in a signal handler. Cleanup runs while
    # unwinding the consumer loop and in run_filter's finally block.
    signal.signal(signum, signal.SIG_IGN)
    raise _ShutdownRequested


def run_filter(factory):
    logging.basicConfig(level=logging.INFO)
    control = None
    previous_handlers = {}
    exit_code = 0
    try:
        control = factory()
        for signum in (signal.SIGTERM, signal.SIGINT):
            previous_handlers[signum] = signal.signal(signum, _request_shutdown)
        control.start()
    except _ShutdownRequested:
        logging.info("Shutdown requested")
    except Exception:
        logging.exception("Control failed")
        exit_code = 1
    finally:
        if control is not None:
            try:
                control.close()
            except Exception:
                logging.exception("Could not close control resources")
                exit_code = 1
        for signum, previous in previous_handlers.items():
            signal.signal(signum, previous)
    return exit_code
