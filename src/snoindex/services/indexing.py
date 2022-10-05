from dataclasses import dataclass

from snoindex.domain.tracker import MessageTracker
from snoindex.repository.queue.sqs import SQSQueue
from snoindex.remote.portal import Portal
from snoindex.repository.opensearch import Opensearch


@dataclass
class IndexingServiceProps:
    invalidation_queue: SQSQueue
    portal: Portal
    opensearch: Opensearch
    messages_to_handle_per_run: int = 1


class IndexingService:

    def __init__(self, props: IndexingServiceProps):
        self.props = props
        self.tracker = MessageTracker()

    def handle_message(self, message) -> None:
        pass

    def try_to_handle_messages(self, messages) -> None:
        pass

    def run_once(self):
        pass

    def log_stats(self) -> None:
        if self.tracker.number_all_messages % 100 == 0:
            logging.warning(f'{self.__class__.__name__}: {self.tracker.stats()}')

    def poll(self):
        while True:
            self.run_once()
