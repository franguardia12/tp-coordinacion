import pika

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)

_CONNECTION_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.ConnectionClosed,
    pika.exceptions.ConnectionWrongStateError,
)


class _CallbackError(Exception):
    """Keep application exceptions separate from communication failures."""


class _MessageMiddlewareRabbitMQ:
    """Connection owned by one process, with sequential consumer callbacks.

    Create and use the instance in the same process. Publishing from a consumer
    callback is supported; connections must not be shared with other processes.
    Communication failures close the connection and propagate without retries.
    """

    def __init__(self, host):
        self._connection = None
        self._channel = None
        self._consumer_queue_name = None
        self._is_consuming = False
        self._additional_consumers = {}
        self._declared_queues = set()

        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self._channel = self._connection.channel()
            self._channel.confirm_delivery()
            # The limit applies separately to data and control consumers.
            self._channel.basic_qos(
                prefetch_count=1, global_qos=False
            )
        except _CONNECTION_ERRORS as error:
            self._cleanup_after_failure(error)
            raise MessageMiddlewareDisconnectedError(
                "Could not connect to RabbitMQ"
            ) from error
        except Exception as error:
            self._cleanup_after_failure(error)
            raise MessageMiddlewareMessageError(
                "Could not initialize the RabbitMQ channel"
            ) from error

    def start_consuming(self, on_message_callback):
        if not callable(on_message_callback):
            raise MessageMiddlewareMessageError(
                "The message callback must be callable"
            )
        if self._is_consuming:
            raise MessageMiddlewareMessageError(
                "This middleware instance is already consuming"
            )

        self._ensure_connection_is_open()

        try:
            self._register_consumers(on_message_callback)
        except _CONNECTION_ERRORS as error:
            self._raise_disconnected(error)
        except Exception as error:
            self._raise_message_error("Could not register the consumer", error)

        self._is_consuming = True
        try:
            self._channel.start_consuming()
        except _CallbackError as error:
            original_error = error.args[0]
            self._cleanup_after_failure(original_error)
            raise original_error from original_error.__cause__
        except _CONNECTION_ERRORS as error:
            self._raise_disconnected(error)
        except Exception as error:
            self._raise_message_error("Could not consume messages", error)
        except BaseException as error:
            # Clean up on interruption without translating KeyboardInterrupt/SystemExit.
            self._cleanup_after_failure(error)
            raise
        finally:
            self._is_consuming = False

    def register_consumer(self, queue_name, on_message_callback):
        """Declare an additional named queue to consume on the next start.

        Register before start_consuming(). All callbacks receive (body, ack,
        nack), run sequentially, and are stopped together by stop_consuming().
        Registrations are retained for a subsequent start on this instance.
        """
        if self._is_consuming:
            raise MessageMiddlewareMessageError(
                "Consumers must be registered before starting consumption"
            )
        if not callable(on_message_callback):
            raise MessageMiddlewareMessageError(
                "The message callback must be callable"
            )
        self._validate_queue_name(queue_name)
        if (
            queue_name == self._consumer_queue_name
            or queue_name in self._additional_consumers
        ):
            raise MessageMiddlewareMessageError(
                "A consumer is already configured for this queue"
            )
        self._declare_named_queue(queue_name)
        self._additional_consumers[queue_name] = on_message_callback

    def send_to_queue(self, queue_name, message):
        """Declare a durable destination and wait for its publish confirmation.

        Reuses this connection, even before the destination consumer starts.
        Confirmation means broker acceptance, not downstream processing.
        """
        self._declare_named_queue(queue_name)
        self._publish_message("", queue_name, message, mandatory=True)

    def _register_consumers(self, on_message_callback):
        consumers = {self._consumer_queue_name: on_message_callback}
        consumers.update(self._additional_consumers)
        for queue_name, callback in consumers.items():
            self._channel.basic_consume(
                queue=queue_name,
                on_message_callback=self._build_message_callback(callback),
                auto_ack=False,
            )

    @staticmethod
    def _validate_queue_name(queue_name):
        if not isinstance(queue_name, str) or not queue_name:
            raise MessageMiddlewareMessageError(
                "Queue names must be non-empty strings"
            )

    def _declare_named_queue(self, queue_name):
        self._validate_queue_name(queue_name)
        self._ensure_connection_is_open()
        if queue_name in self._declared_queues:
            return
        try:
            self._channel.queue_declare(queue=queue_name, durable=True)
        except _CONNECTION_ERRORS as error:
            self._raise_disconnected(error)
        except Exception as error:
            self._raise_message_error("Could not declare the queue", error)
        self._declared_queues.add(queue_name)

    def stop_consuming(self):
        if not self._is_consuming:
            return

        try:
            self._ensure_connection_is_open()
            self._channel.stop_consuming()
        except MessageMiddlewareDisconnectedError:
            raise
        except _CONNECTION_ERRORS as error:
            self._raise_disconnected(error)
        except Exception as error:
            self._cleanup_after_failure(error)
            raise MessageMiddlewareCloseError(
                "Could not stop the consumer"
            ) from error

    def close(self):
        if self._connection is None or self._connection.is_closed:
            return

        close_error = None

        if self._is_consuming:
            try:
                self.stop_consuming()
            except Exception as error:
                close_error = error

        if self._channel is not None and self._channel.is_open:
            try:
                self._channel.close()
            except Exception as error:
                close_error = self._record_close_error(close_error, error)

        if self._connection.is_open:
            try:
                self._connection.close()
            except Exception as error:
                close_error = self._record_close_error(close_error, error)

        if close_error is not None:
            raise MessageMiddlewareCloseError(
                "Could not close the RabbitMQ middleware cleanly"
            ) from close_error

    def _publish_message(
        self, exchange_name, routing_key, message, mandatory=False
    ):
        self._ensure_connection_is_open()

        try:
            self._channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=message,
                mandatory=mandatory,
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                ),
            )
        except pika.exceptions.UnroutableError as error:
            self._raise_message_error(
                "The message could not be routed to a queue", error
            )
        except pika.exceptions.NackError as error:
            self._raise_message_error(
                "RabbitMQ rejected the publication", error
            )
        except _CONNECTION_ERRORS as error:
            self._raise_disconnected(error)
        except Exception as error:
            self._raise_message_error("Could not publish the message", error)

    def _build_message_callback(self, on_message_callback):
        def callback(channel, method, _properties, body):
            delivery_tag = method.delivery_tag

            def ack():
                self._acknowledge(channel, delivery_tag)

            def nack():
                self._reject(channel, delivery_tag)

            try:
                on_message_callback(body, ack, nack)
            except Exception as error:
                raise _CallbackError(error) from error

        return callback

    def _acknowledge(self, channel, delivery_tag):
        try:
            channel.basic_ack(delivery_tag=delivery_tag)
        except _CONNECTION_ERRORS as error:
            self._raise_disconnected(error)
        except Exception as error:
            self._raise_message_error("Could not acknowledge the message", error)

    def _reject(self, channel, delivery_tag):
        try:
            channel.basic_nack(
                delivery_tag=delivery_tag,
                requeue=True,
            )
        except _CONNECTION_ERRORS as error:
            self._raise_disconnected(error)
        except Exception as error:
            self._raise_message_error("Could not reject the message", error)

    def _ensure_connection_is_open(self):
        if self._connection is None or self._connection.is_closed:
            raise MessageMiddlewareDisconnectedError(
                "The RabbitMQ connection is closed"
            )
        if self._channel is None or self._channel.is_closed:
            error = MessageMiddlewareMessageError(
                "The RabbitMQ channel is closed"
            )
            self._cleanup_after_failure(error)
            raise error

    def _cleanup_after_failure(self, original_error):
        if self._connection is None or self._connection.is_closed:
            return

        try:
            self._connection.close()
        except Exception as cleanup_error:
            original_error.add_note(
                f"The connection could not be closed after failure: "
                f"{cleanup_error}"
            )

    @staticmethod
    def _record_close_error(first_error, error):
        if first_error is None:
            return error
        first_error.add_note(f"Additional error during close: {error!r}")
        return first_error

    def _raise_disconnected(self, error):
        self._cleanup_after_failure(error)
        raise MessageMiddlewareDisconnectedError(
            "The RabbitMQ connection was lost"
        ) from error

    def _raise_message_error(self, message, error):
        self._cleanup_after_failure(error)
        raise MessageMiddlewareMessageError(message) from error


class MessageMiddlewareQueueRabbitMQ(
    _MessageMiddlewareRabbitMQ,
    MessageMiddlewareQueue,
):

    def __init__(self, host, queue_name):
        self._validate_queue_name(queue_name)
        super().__init__(host)
        self._queue_name = queue_name
        self._consumer_queue_name = queue_name

        self._declare_named_queue(queue_name)

    def send(self, message):
        self.send_to_queue(self._queue_name, message)


class MessageMiddlewareExchangeRabbitMQ(
    _MessageMiddlewareRabbitMQ,
    MessageMiddlewareExchange,
):

    def __init__(self, host, exchange_name, routing_keys):
        if isinstance(routing_keys, (str, bytes)):
            raise MessageMiddlewareMessageError(
                "Routing keys must be an iterable of strings"
            )

        try:
            routing_keys = tuple(dict.fromkeys(routing_keys))
        except Exception as error:
            raise MessageMiddlewareMessageError(
                "Routing keys must be an iterable of strings"
            ) from error

        if any(
            not isinstance(routing_key, str) or not routing_key
            for routing_key in routing_keys
        ):
            raise MessageMiddlewareMessageError(
                "Routing keys must be non-empty strings"
            )

        super().__init__(host)
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys

        try:
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type="direct",
                durable=True,
            )
        except _CONNECTION_ERRORS as error:
            self._raise_disconnected(error)
        except Exception as error:
            self._raise_message_error("Could not declare the exchange", error)

    def start_consuming(self, on_message_callback):
        self._declare_consumer_queue()
        super().start_consuming(on_message_callback)

    def send(self, message):
        self._ensure_connection_is_open()
        for routing_key in self._routing_keys:
            self._publish_message(self._exchange_name, routing_key, message)

    def _declare_consumer_queue(self):
        if self._consumer_queue_name is not None:
            return

        self._ensure_connection_is_open()
        try:
            result = self._channel.queue_declare(
                queue="",
                durable=False,
                exclusive=True,
                auto_delete=False,
            )
            queue_name = result.method.queue

            for routing_key in self._routing_keys:
                self._channel.queue_bind(
                    exchange=self._exchange_name,
                    queue=queue_name,
                    routing_key=routing_key,
                )

            self._consumer_queue_name = queue_name
        except _CONNECTION_ERRORS as error:
            self._raise_disconnected(error)
        except Exception as error:
            self._raise_message_error(
                "Could not configure the exchange consumer", error
            )
