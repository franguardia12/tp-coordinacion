from uuid import uuid4

from common.message_protocol import internal


class _MatchedResult(list):
    """The fixed gateway uses truthiness for matching, even for an empty top."""

    def __bool__(self):
        return True


class MessageHandler:

    def __init__(self):
        # Created before the gateway copies the handler to its worker processes.
        self.query_id = uuid4().hex
        self.total_records = 0

    def serialize_data_message(self, message):
        fruit, amount = message
        serialized = internal.serialize(internal.data(self.query_id, fruit, amount))
        self.total_records += 1
        return serialized

    def serialize_eof_message(self, message):
        return internal.serialize(internal.eof(self.query_id, self.total_records))

    def deserialize_result_message(self, message):
        fields = internal.deserialize(message)
        if fields["type"] != internal.RESULT:
            raise ValueError("Expected a final result")
        if fields["query_id"] != self.query_id:
            return None
        return _MatchedResult(fields["items"])
