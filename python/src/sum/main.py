import logging
import os
from dataclasses import dataclass, field

from common.control import QueueFilter, positive_setting, replica_queue, run_filter
from common.message_protocol import internal
from common.processing import FruitTotals
from coordination import CompletionBarrier


@dataclass
class LocalQuery:
    totals: FruitTotals = field(default_factory=FruitTotals)
    coordinator_id: int | None = None


class SumFilter(QueueFilter):

    def __init__(self):
        self.replica_count = positive_setting("SUM_AMOUNT")
        self.replica_id = int(os.environ["ID"])
        if not 0 <= self.replica_id < self.replica_count:
            raise ValueError("Sum ID is outside the configured replica range")
        if positive_setting("AGGREGATION_AMOUNT") != 1:
            raise ValueError("Multiple Aggregation replicas require partitioning")
        self.control_prefix = os.environ["SUM_PREFIX"]
        self.destination = replica_queue(os.environ["AGGREGATION_PREFIX"], 0)
        self.queries = {}
        self.barriers = {}
        super().__init__(os.environ["MOM_HOST"], os.environ["INPUT_QUEUE"])

    def control_queue(self, replica_id):
        return f"{replica_queue(self.control_prefix, replica_id)}_control"

    def start(self):
        self.transport.register_consumer(
            self.control_queue(self.replica_id), self._consume_control
        )
        super().start()

    def _consume_control(self, body, ack, nack):
        message = internal.deserialize(body)
        sender_id = message.get("sender_id")
        if type(sender_id) is not int or not 0 <= sender_id < self.replica_count:
            raise ValueError("Unknown Sum control sender")
        handlers = {
            internal.PREPARE: self._prepare,
            internal.PROGRESS: self._progress,
            internal.FLUSH: self._flush,
            internal.FLUSHED: self._flushed,
        }
        handler = handlers.get(message["type"])
        if handler is None:
            raise ValueError("Unexpected message in Sum control queue")
        handler(message)
        ack()

    def _local_query(self, query_id):
        if query_id not in self.queries:
            self.queries[query_id] = LocalQuery()
        return self.queries[query_id]

    def _send_control(self, destination_id, message_type, query_id, **fields):
        self.send(
            self.control_queue(destination_id),
            internal.control(message_type, query_id, self.replica_id, **fields),
        )

    def _broadcast_control(self, message_type, query_id):
        for replica_id in range(self.replica_count):
            self._send_control(replica_id, message_type, query_id)

    def process_message(self, message):
        query_id = message["query_id"]
        if message["type"] == internal.DATA:
            query = self._local_query(query_id)
            query.totals.add(message["fruit"], message["amount"])
            if query.coordinator_id is not None:
                self._report_progress(query_id, query)
        elif message["type"] == internal.EOF:
            if query_id in self.barriers:
                raise ValueError("Repeated gateway EOF")
            self.barriers[query_id] = CompletionBarrier(
                message["total_records"], self.replica_count
            )
            self._broadcast_control(internal.PREPARE, query_id)
        else:
            raise ValueError("Expected data or gateway EOF")

    def _report_progress(self, query_id, query):
        self._send_control(
            query.coordinator_id, internal.PROGRESS, query_id,
            processed_records=query.totals.record_count,
        )

    def _prepare(self, message):
        query_id = message["query_id"]
        query = self._local_query(query_id)
        if query.coordinator_id is not None:
            raise ValueError("Repeated preparation for a query")
        query.coordinator_id = message["sender_id"]
        # EOF was already dequeued. With FIFO delivery and prefetch=1, at most
        # one earlier record per data consumer can still finish after this report.
        # Control and data may arrive in either order; only the counts close it.
        self._report_progress(query_id, query)

    def _progress(self, message):
        query_id = message["query_id"]
        barrier = self.barriers[query_id]
        if barrier.record_progress(message["sender_id"], message["processed_records"]):
            self._broadcast_control(internal.FLUSH, query_id)

    def _flush(self, message):
        query_id = message["query_id"]
        query = self.queries[query_id]
        if query.coordinator_id != message["sender_id"]:
            raise ValueError("Flush requested by a different coordinator")
        for item in query.totals.by_fruit.values():
            self.send(self.destination, internal.data(query_id, item.fruit, item.amount))
        # Every partial is broker-confirmed before this report is published.
        self._send_control(
            query.coordinator_id, internal.FLUSHED, query_id,
            partial_count=len(query.totals.by_fruit),
        )
        logging.info(
            "Sum %s published query %s: %s records, %s partials",
            self.replica_id, query_id, query.totals.record_count,
            len(query.totals.by_fruit),
        )
        del self.queries[query_id]

    def _flushed(self, message):
        query_id = message["query_id"]
        barrier = self.barriers[query_id]
        if barrier.record_publication(message["sender_id"], message["partial_count"]):
            # All producers finished before the marker enters the data queue.
            self.send(self.destination, internal.eof(query_id, barrier.partial_count))
            del self.barriers[query_id]
            logging.info("Closed Sum stage for query %s", query_id)


def main():
    return run_filter(SumFilter)


if __name__ == "__main__":
    raise SystemExit(main())
