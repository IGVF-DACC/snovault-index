import json

from dataclasses import dataclass

from typing import Any
from typing import Dict
from typing import List


@dataclass
class InboundMessage:
    message_id: str
    receipt_handle: str
    md5_of_body: str
    body: str

    @property
    def json_body(self) -> Any:
        return json.loads(
            self.body
        )


@dataclass
class OutboundMessage:
    unique_id: str
    body: Dict[str, Any]

    @property
    def str_body(self) -> str:
        return json.dumps(
            self.body
        )


field_mapping: Dict[str, str] = {
    'MessageId': 'message_id',
    'ReceiptHandle': 'receipt_handle',
    'MD5OfBody': 'md5_of_body',
    'Body': 'body'
}


def map_fields(value: Dict[str, Any]) -> Dict[str, Any]:
    return {
        field_mapping[k]: v
        for k, v in value.items()
    }


def convert_to_inbound_message(message: Dict[str, Any]) -> InboundMessage:
    return InboundMessage(
        **map_fields(message)
    )


def convert_received_messages(messages: List[Dict[str, Any]]) -> List[InboundMessage]:
    return [
        convert_to_inbound_message(message)
        for message in messages
    ]
