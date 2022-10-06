import pytest


@pytest.fixture
def inbound_message():
    import json
    from snoindex.domain.message import InboundMessage
    json_body = {
        'some': [
            'json',
            'string'
        ],
        'with': {
            'meta': 'data',
        }
    }
    json_string = json.dumps(json_body)
    return InboundMessage(
        message_id='abc',
        receipt_handle='abc123',
        md5_of_body='ccc',
        body=json_string
    )
