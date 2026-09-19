import logging
import os

from common.control import QueueFilter, positive_setting, run_filter
from common.fruit_item import FruitItem
from common.message_protocol import internal
from common.processing import as_records, select_top


class JoinFilter(QueueFilter):

    def __init__(self):
        self.top_size = positive_setting("TOP_SIZE")
        self.aggregation_count = positive_setting("AGGREGATION_AMOUNT")
        self.destination = os.environ["OUTPUT_QUEUE"]
        self.queries = {}
        super().__init__(os.environ["MOM_HOST"], os.environ["INPUT_QUEUE"])

    def process_message(self, message):
        if message["type"] != internal.PARTIAL_TOP:
            raise ValueError("Expected a partial top")
        query_id = message["query_id"]
        sender_id = message["sender_id"]
        if sender_id >= self.aggregation_count:
            raise ValueError("Unknown Aggregation replica")
        candidates, senders = self.queries.setdefault(query_id, ([], set()))
        if sender_id in senders:
            raise ValueError("Repeated partial top from Aggregation")
        incoming = [FruitItem(fruit, amount) for fruit, amount in message["items"]]
        candidates = select_top(candidates + incoming, self.top_size)
        senders.add(sender_id)
        self.queries[query_id] = candidates, senders
        if len(senders) == self.aggregation_count:
            self.send(
                self.destination,
                internal.top(internal.RESULT, query_id, as_records(candidates)),
            )
            del self.queries[query_id]
            logging.info("Published final top for query %s", query_id)


def main():
    return run_filter(JoinFilter)


if __name__ == "__main__":
    raise SystemExit(main())
