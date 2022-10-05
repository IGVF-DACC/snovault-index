from snoindex.queue.message import InboundMessage
from snoindex.queue.message import OutboundMessage

from typing import List


class InMemoryQueue:

    def __init__(self, *args, **kwargs) -> None:
        self._queue = []

    def send_messages(self, messages: List[OutboundMessage]) -> None:
        pass

    def get_messages(
            self,
            desired_number_of_messages=50
    ) -> List[InboundMessage]:
        pass

    def mark_as_processed(self, messages: List[InboundMessage]) -> None:
        pass

    def info(self):
        pass

    def wait_for_queue_to_exist(self):
        pass

    def wait_for_queue_to_drain(self):
        pass
