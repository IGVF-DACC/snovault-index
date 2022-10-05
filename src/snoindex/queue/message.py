import json


from dataclasses import dataclass

from typing import Any
from typing import Dict


@dataclass
class InboundMessage:
    message_id: str
    receipt_handle: str
    md5_of_body: str
    body: str

    @property
    def json_body(self):
        return json.loads(
            self.body
        )


@dataclass
class OutboundMessage:
    unique_id: str
    body: Dict[str, Any]

    @property
    def str_body(self):
        return json.dumps(
            self.body
        )


field_mapping = {
    'MessageId': 'message_id',
    'ReceiptHandle': 'receipt_handle',
    'MD5OfBody': 'md5_of_body',
    'Body': 'body'
}


def map_fields(value):
    return {
        field_mapping[k]: v
        for k, v in value.items()
    }


def convert_to_inbound_message(message):
    return InboundMessage(
        **map_fields(message)
    )


def convert_received_messages(messages):
    return [
        convert_to_inbound_message(message)
        for message in messages
    ]
