import logging
import os

from common.control import positive_setting, replica_queue, run_filter
from common.message_protocol import internal
from common.processing import AccumulatingFilter, as_records, select_top


class AggregationFilter(AccumulatingFilter):

    def __init__(self):
        self.replica_id = int(os.environ["ID"])
        if not 0 <= self.replica_id < positive_setting("AGGREGATION_AMOUNT"):
            raise ValueError("Aggregation ID is outside the configured replica range")
        self.top_size = positive_setting("TOP_SIZE")
        self.destination = os.environ["OUTPUT_QUEUE"]
        input_queue = replica_queue(os.environ["AGGREGATION_PREFIX"], self.replica_id)
        super().__init__(os.environ["MOM_HOST"], input_queue)

    def finish_query(self, query_id, totals):
        # Rank only consolidated values: a fruit's position may change on add.
        items = as_records(select_top(totals.by_fruit.values(), self.top_size))
        self.send(
            self.destination,
            internal.top(internal.PARTIAL_TOP, query_id, items, self.replica_id),
        )
        logging.info(
            "Aggregation %s published query %s: %s fruits, %s top entries",
            self.replica_id, query_id, len(totals.by_fruit), len(items),
        )


def main():
    return run_filter(AggregationFilter)


if __name__ == "__main__":
    raise SystemExit(main())
