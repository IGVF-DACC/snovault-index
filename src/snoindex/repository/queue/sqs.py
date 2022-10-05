import logging
import time

from botocore.client import BaseClient

from dataclasses import dataclass

from typing import Any
from typing import Dict
from typing import Iterable
from typing import List

from snoindex.repository.queue.constants import AWS_SQS_MAX_NUMBER

from snoindex.domain.message import convert_received_messages
from snoindex.domain.message import InboundMessage
from snoindex.domain.message import OutboundMessage


def batch(items, batchsize=AWS_SQS_MAX_NUMBER):
    for i in range(0, len(items), batchsize):
        yield items[i:i + batchsize]


@dataclass
class SQSQueueProps:
    client: BaseClient
    queue_url: str
    wait_time_seconds: int = 20
    visibility_timeout: int = 60


class SQSQueue:

    def __init__(self, *args, props, **kwargs):
        self.props = props

    def _send_messages(self, messages: List[OutboundMessage]):
        self.props.client.send_message_batch(
            QueueUrl=self.props.queue_url,
            Entries=[
                {
                    'Id': message.unique_id,
                    'MessageBody': message.str_body,
                }
                for message in messages
            ]
        )

    def send_messages(self, messages: List[OutboundMessage]):
        batched_messages = batch(messages, batchsize=AWS_SQS_MAX_NUMBER)
        for message_batch in batched_messages:
            self._send_messages(message_batch)

    def _get_messages(
            self,
            max_number_of_messages=AWS_SQS_MAX_NUMBER
    ) -> List[InboundMessage]:
        response = self.props.client.receive_message(
            QueueUrl=self.props.queue_url,
            MaxNumberOfMessages=max_number_of_messages,
            WaitTimeSeconds=self.props.wait_time_seconds,
            VisibilityTimeout=self.props.visibility_timeout,
        )
        messages = response.get('Messages', [])
        return convert_received_messages(messages)

    def get_messages(
            self,
            desired_number_of_messages: int = 50
    ) -> Iterable[InboundMessage]:
        number_of_received_messages = 0
        while True:
            number_of_messages_left_to_get = (
                desired_number_of_messages - number_of_received_messages
            )
            if number_of_messages_left_to_get <= 0:
                break
            max_number_of_messages = min(
                number_of_messages_left_to_get,
                AWS_SQS_MAX_NUMBER
            )
            messages = self._get_messages(
                max_number_of_messages=max_number_of_messages
            )
            if not messages:
                break
            number_of_received_messages += len(messages)
            for message in messages:
                yield message

    def _mark_as_processed(self, messages: List[InboundMessage]):
        self.props.client.delete_message_batch(
            QueueUrl=self.props.queue_url,
            Entries=[
                {
                    'Id': message.message_id,
                    'ReceiptHandle': message.receipt_handle,
                }
                for message in messages
            ]
        )

    def mark_as_processed(self, messages: List[InboundMessage]):
        batched_messages = batch(messages, batchsize=AWS_SQS_MAX_NUMBER)
        for message_batch in batched_messages:
            self._mark_as_processed(message_batch)

    def info(self) -> Dict[str, Any]:
        return self.props.client.get_queue_attributes(
            QueueUrl=self.props.queue_url,
            AttributeNames=['All']
        )['Attributes']

    def wait_for_queue_to_exist(self):
        logging.warning(f'Connecting to queue: {self.props.queue_url}')
        caught = None
        for attempt in range(1, 4):
            try:
                self.info()
                break
            except self.props.client.exceptions.QueueDoesNotExist as error:
                time.sleep(attempt * 3)
                caught = error
                continue
            raise caught

    def _queue_has_zero_messages(self):
        info = self.info()
        conditions = [
            info['ApproximateNumberOfMessages'] == '0',
            info['ApproximateNumberOfMessagesNotVisible'] == '0',
        ]
        return all(conditions)

    def wait_for_queue_to_drain(
            self,
            number_of_checks: int = 3,
            seconds_between_checks: int = 3
    ):
        checks_left = number_of_checks
        while True:
            if self._queue_has_zero_messages():
                checks_left -= 1
            else:
                checks_left = number_of_checks
            if checks_left <= 0:
                break
            time.sleep(seconds_between_checks)
