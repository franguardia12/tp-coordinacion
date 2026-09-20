"""Per-query barriers; no data records pass through the coordinator."""


class CompletionBarrier:

    def __init__(self, expected_records, replica_count):
        self.expected_records = expected_records
        self.replica_count = replica_count
        self.progress = {}
        self.processed_records = 0
        self.published = {}
        self.partial_count = 0
        self.flushing = False

    def record_progress(self, sender_id, processed_records):
        if self.flushing:
            raise ValueError("Progress received after the processing barrier")
        previous = self.progress.get(sender_id, 0)
        if processed_records < previous:
            raise ValueError("A replica's processed count decreased")
        self.progress[sender_id] = processed_records
        # Update incrementally: scanning every replica on each report is O(S²).
        self.processed_records += processed_records - previous
        if self.processed_records > self.expected_records:
            raise ValueError("Processed more records than announced by the gateway")
        complete = (
            len(self.progress) == self.replica_count
            and self.processed_records == self.expected_records
        )
        if complete:
            self.flushing = True
        return complete

    def record_publication(self, sender_id, partial_count):
        if not self.flushing:
            raise ValueError("Publication received before the processing barrier")
        if sender_id in self.published:
            raise ValueError("A replica reported publication twice")
        self.published[sender_id] = partial_count
        self.partial_count += partial_count
        return len(self.published) == self.replica_count
