import pytest


def test_repository_queue_sqs_batch():
    from snoindex.repository.queue.sqs import batch
    items = list(range(10))
    actual = list(batch(items, batchsize=3))
    expected = [
        [0, 1, 2],
        [3, 4, 5],
        [6, 7, 8],
        [9]
    ]
    assert actual == expected


def test_repository_queue_sqs_sqsqueue_init():
    from snoindex.repository.queue.sqs import SQSQueueProps
    from snoindex.repository.queue.sqs import SQSQueue
    import boto3
    url = 'some-url'
    client = boto3.client(
        'sqs',
        endpoint_url='http://testing',
        aws_access_key_id='testing',
        aws_secret_access_key='testing',
        region_name='us-west-2',
    )
    queue = SQSQueue(
        props=SQSQueueProps(
            client=client,
            queue_url=url
        )
    )
    assert isinstance(queue, SQSQueue)


@pytest.mark.integration
def test_repository_queue_sqs_send_messages(queue_for_testing):
    from snoindex.domain.message import OutboundMessage
    assert queue_for_testing._queue_has_zero_messages()
    messages = []
    for i in range(20):
        messages.append(
            OutboundMessage(
                unique_id=f'xyz-{i}',
                body={'some': f'xyz-{i}-data'}
            )
        )
    queue_for_testing.send_messages(
        messages
    )
    assert int(queue_for_testing.info()['ApproximateNumberOfMessages']) == 20
    queue_for_testing.send_messages(
        messages
    )
    queue_for_testing.send_messages(
        messages
    )
    assert int(queue_for_testing.info()['ApproximateNumberOfMessages']) == 60


@pytest.mark.integration
def test_repository_queue_sqs_get_messages(queue_for_testing):
    from snoindex.domain.message import OutboundMessage
    from snoindex.domain.message import InboundMessage
    assert queue_for_testing._queue_has_zero_messages()
    messages = []
    for i in range(20):
        messages.append(
            OutboundMessage(
                unique_id=f'xyz-{i}',
                body={'some': f'xyz-{i}-data'}
            )
        )
    queue_for_testing.send_messages(
        messages
    )
    new_messages = list(
        queue_for_testing.get_messages(
            desired_number_of_messages=5
        )
    )
    assert isinstance(new_messages[0], InboundMessage)
    assert new_messages[0].json_body['some'].startswith('xyz')
    assert len(new_messages) == 5
    queue_for_testing.mark_as_processed(new_messages)
    new_messages = list(
        queue_for_testing.get_messages(
            desired_number_of_messages=15
        )
    )
    assert len(new_messages) == 15
    queue_for_testing.mark_as_processed(new_messages)
    assert int(queue_for_testing.info()['ApproximateNumberOfMessages']) == 0


@pytest.mark.integration
def test_repository_queue_sqs_mark_as_processed(queue_for_testing):
    from snoindex.domain.message import OutboundMessage
    assert queue_for_testing._queue_has_zero_messages()
    messages = []
    for i in range(20):
        messages.append(
            OutboundMessage(
                unique_id=f'xyz-{i}',
                body={'some': f'xyz-{i}-data'}
            )
        )
    queue_for_testing.send_messages(
        messages
    )
    new_messages = list(
        queue_for_testing.get_messages(
            desired_number_of_messages=20
        )
    )
    for new_message in new_messages:
        queue_for_testing.mark_as_processed([new_message])
    assert int(queue_for_testing.info()['ApproximateNumberOfMessages']) == 0


@pytest.mark.integration
def test_repository_queue_sqs_info(queue_for_testing):
    from snoindex.domain.message import OutboundMessage
    assert queue_for_testing._queue_has_zero_messages()
    messages = []
    for i in range(3):
        messages.append(
            OutboundMessage(
                unique_id=f'xyz-{i}',
                body={'some': f'xyz-{i}-data'}
            )
        )
    queue_for_testing.send_messages(
        messages
    )
    assert int(queue_for_testing.info()['ApproximateNumberOfMessages']) == 3
    assert int(queue_for_testing.info()[
               'ApproximateNumberOfMessagesNotVisible']) == 0


@pytest.mark.integration
def test_repository_queue_sqs_wait_for_queue_to_exist(queue_for_testing):
    queue_for_testing.wait_for_queue_to_exist()
    assert True


@pytest.mark.integration
def test_repository_queue_sqs_queue_has_zero_messages(queue_for_testing):
    from snoindex.domain.message import OutboundMessage
    messages = []
    for i in range(3):
        messages.append(
            OutboundMessage(
                unique_id=f'xyz-{i}',
                body={'some': f'xyz-{i}-data'}
            )
        )
    queue_for_testing.send_messages(
        messages
    )
    assert not queue_for_testing._queue_has_zero_messages()
    queue_for_testing.clear()
    queue_for_testing.wait_for_queue_to_drain()
    assert queue_for_testing._queue_has_zero_messages()


@pytest.mark.integration
def test_repository_queue_sqs_wait_for_queue_to_drain(queue_for_testing):
    queue_for_testing.wait_for_queue_to_drain()
    assert True
