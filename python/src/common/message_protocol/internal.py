import json

DATA = "data"
EOF = "eof"
PARTIAL_TOP = "partial_top"
RESULT = "result"


def serialize(message):
    return json.dumps(message).encode("utf-8")


def data(query_id, fruit, amount):
    return dict(type=DATA, query_id=query_id, fruit=fruit, amount=amount)


def eof(query_id, total_records):
    return dict(type=EOF, query_id=query_id, total_records=total_records)


def top(message_type, query_id, items, sender_id=None):
    message = dict(type=message_type, query_id=query_id, items=items)
    if sender_id is not None:
        message["sender_id"] = sender_id
    return message


def _require_integer(value, name, minimum=None):
    if type(value) is not int or (minimum is not None and value < minimum):
        raise ValueError(f"Invalid {name}: {value!r}")


def _validate_record(fruit, amount):
    if not isinstance(fruit, str):
        raise ValueError("Fruit must be a string")
    _require_integer(amount, "amount")


def deserialize(message):
    fields = json.loads(message.decode("utf-8"))
    if not isinstance(fields, dict):
        raise ValueError("Expected an internal message object")
    if not isinstance(fields.get("query_id"), str) or not fields["query_id"]:
        raise ValueError("Missing query identifier")
    message_type = fields.get("type")
    if message_type == DATA:
        _validate_record(fields.get("fruit"), fields.get("amount"))
    elif message_type == EOF:
        _require_integer(fields.get("total_records"), "total_records", 0)
    elif message_type in (PARTIAL_TOP, RESULT):
        items = fields.get("items")
        if not isinstance(items, list):
            raise ValueError("Top items must be a list")
        for item in items:
            if not isinstance(item, list) or len(item) != 2:
                raise ValueError("Invalid top record")
            _validate_record(*item)
        if message_type == PARTIAL_TOP:
            _require_integer(fields.get("sender_id"), "sender_id", 0)
    else:
        raise ValueError(f"Unknown internal message type: {message_type!r}")
    return fields
