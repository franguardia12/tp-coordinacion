import logging
import os

from common.control import positive_setting, replica_queue, run_filter
from common.message_protocol import internal
from common.processing import AccumulatingFilter


class SumFilter(AccumulatingFilter):

    def __init__(self):
        if positive_setting("SUM_AMOUNT") != 1:
            raise ValueError("Multiple Sum replicas require distributed completion")
        if positive_setting("AGGREGATION_AMOUNT") != 1:
            raise ValueError("Multiple Aggregation replicas require partitioning")
        self.destination = replica_queue(os.environ["AGGREGATION_PREFIX"], 0)
        super().__init__(os.environ["MOM_HOST"], os.environ["INPUT_QUEUE"])

    def finish_query(self, query_id, totals):
        for item in totals.by_fruit.values():
            self.send(self.destination, internal.data(query_id, item.fruit, item.amount))
        # This marker shares the data queue and follows every confirmed partial.
        self.send(self.destination, internal.eof(query_id, len(totals.by_fruit)))
        logging.info("Published totals for query %s", query_id)


def main():
    return run_filter(SumFilter)


if __name__ == "__main__":
    raise SystemExit(main())
