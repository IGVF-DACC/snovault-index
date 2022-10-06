from snoindex.domain.message import InboundMessage

from typing import Dict
from typing import Iterable
from typing import List


class MessageTracker:

    def __init__(self) -> None:
        self.clear_stats()
        self.clear()

    def clear_stats(self) -> None:
        self.number_all_messages: int = 0
        self.number_handled_messages: int = 0
        self.number_failed_messages: int = 0

    def clear(self) -> None:
        self.new_messages: List[InboundMessage] = []
        self.handled_messages: List[InboundMessage] = []
        self.failed_messages: List[InboundMessage] = []

    def add_new_messages(self, messages: List[InboundMessage]) -> None:
        self.number_all_messages += len(messages)
        self.new_messages.extend(messages)

    def add_handled_messages(self, messages: List[InboundMessage]) -> None:
        self.number_handled_messages += len(messages)
        self.handled_messages.extend(messages)

    def add_failed_messages(self, messages: List[InboundMessage]) -> None:
        self.number_failed_messages += len(messages)
        self.failed_messages.extend(messages)

    def stats(self) -> Dict[str, int]:
        return {
            'all': self.number_all_messages,
            'handled': self.number_handled_messages,
            'failed': self.number_failed_messages,
        }
