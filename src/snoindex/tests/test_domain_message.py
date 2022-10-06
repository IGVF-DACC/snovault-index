import pytest

import json


def test_domain_message_inbound_message():
    from snoindex.domain.message import InboundMessage
    json_body = {
        'some': [
            'json',
            'string'
        ]
    }
    json_string = json.dumps(json_body)
    message = InboundMessage(
        message_id='abc',
        receipt_handle='abc123',
        md5_of_body='ccc',
        body=json_string
    )
    assert isinstance(message, InboundMessage)
    assert message.message_id == 'abc'
    assert message.receipt_handle == 'abc123'
    assert message.md5_of_body == 'ccc'
    assert message.body == json_string
    assert message.json_body == json_body


def test_domain_message_outbound_message():
    from snoindex.domain.message import OutboundMessage
    json_body = {
        'data': {
            'and': 'metadata'
        }
    }
    message = OutboundMessage(
        unique_id='something-unique',
        body=json_body,
    )
    assert isinstance(message, OutboundMessage)
    assert message.unique_id == 'something-unique'
    assert message.body == json_body
    assert message.str_body == json.dumps(
        json_body
    )


def test_domain_message_map_fields():
    from snoindex.domain.message import map_fields
    value = {
        'MessageId': 'xyz',
        'ReceiptHandle': 'abc',
        'MD5OfBody': 'ccc',
        'Body': '{"some": "body"}'
    }
    expected = {
        'message_id': 'xyz',
        'receipt_handle': 'abc',
        'md5_of_body': 'ccc',
        'body': '{"some": "body"}'
    }
    actual = map_fields(value)
    assert actual == expected


def test_domain_message_convert_to_inbound_message():
    from snoindex.domain.message import InboundMessage
    from snoindex.domain.message import convert_to_inbound_message
    value = {
        'MessageId': 'xyz',
        'ReceiptHandle': 'abc',
        'MD5OfBody': 'ccc',
        'Body': '{"some": "body"}'
    }
    message = convert_to_inbound_message(value)
    assert isinstance(message, InboundMessage)
    assert message.message_id == 'xyz'


def test_domain_message_convert_received_messages():
    from snoindex.domain.message import convert_received_messages
    value = {
        'MessageId': 'xyz',
        'ReceiptHandle': 'abc',
        'MD5OfBody': 'ccc',
        'Body': '{"some": "body"}'
    }
    messages = [value for i in range(3)]
    converted_messages = convert_received_messages(messages)
    assert len(converted_messages) == 3
