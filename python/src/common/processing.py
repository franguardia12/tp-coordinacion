"""Shared accumulation and top selection using the opaque FruitItem operators."""

from heapq import nlargest

from common.fruit_item import FruitItem
from common.message_protocol import internal
from common.control import QueueFilter


def select_top(items, size):
    return nlargest(size, items)


def as_records(items):
    return [(item.fruit, item.amount) for item in items]


class FruitTotals:

    def __init__(self):
        self.by_fruit = {}
        self.record_count = 0

    def add(self, fruit, amount):
        current = self.by_fruit.get(fruit, FruitItem(fruit, 0))
        self.by_fruit[fruit] = current + FruitItem(fruit, amount)
        self.record_count += 1


class AccumulatingFilter(QueueFilter):

    def __init__(self, host, input_queue):
        self.queries = {}
        super().__init__(host, input_queue)

    def process_message(self, message):
        query_id = message["query_id"]
        message_type = message["type"]
        if message_type not in (internal.DATA, internal.EOF):
            raise ValueError("Expected data or end of records")
        totals = self.queries.setdefault(query_id, FruitTotals())
        if message_type == internal.DATA:
            totals.add(message["fruit"], message["amount"])
            return
        if totals.record_count != message["total_records"]:
            raise ValueError(f"Record count mismatch for query {query_id}")
        self.finish_query(query_id, totals)
        # Keep state until all outgoing publications have been confirmed.
        del self.queries[query_id]

    def finish_query(self, query_id, totals):
        raise NotImplementedError
