from dataclasses import dataclass

from snoindex.domain.tracker import MessageTracker
from snoindex.repository.queue.sqs import SQSQueue
from snoindex.remote.portal import Portal
from snoindex.repository.opensearch import Opensearch


def get_uuid_and_version_from_message(message):
    uuid = message.json_body['data']['uuid']
    version = message.json_body['metadata']['xid']
    return (uuid, version)


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
        uuid, version = get_uuid_and_version_from_message(message)
        item = self.props.portal.get_item(uuid, version)
        self.props.opensearch.index_item(item)
        self.tracker.add_handled_messages([message])

    def try_to_handle_messages(self, messages) -> None:
        for message in self.tracker.new_messages:
            try:
                self.handle_message(message)
            except Exception as e:
                logging.error(e)
                self.tracker.add_failed_messages(
                    [
                        message
                    ]
                )

    def get_new_messages_from_queue(self) -> None:
        self.tracker.add_new_messages(
            self.props.invalidation_queue_queue.get_messages(
                desired_number_of_messages=self.props.messages_to_handle_per_run
            )
        )

    def log_stats(self) -> None:
        if self.tracker.number_all_messages % 100 == 0:
            logging.warning(f'{self.__class__.__name__}: {self.tracker.stats()}')

    def clear(self) -> None:
        self.tracker.clear()

    def run_once(self) -> None:
        self.get_new_messages_from_queue()
        self.try_to_handle_messages()
        self.mark_handled_messages_as_processed()
        self.log_stats()
        self.clear()

    def poll(self):
        while True:
            self.run_once()


@dataclass
class BulkIndexingServiceProps:
    bulk_invalidation_queue: SQSQueue
    portal: Portal
    opensearch: Opensearch
    messages_to_handle_per_run: int = 50


class BulkIndexingService:

    def __init__(self, props: BulkIndexingServiceProps):
        self.props = props
        self.tracker = MessageTracker()

    def handle_messages(self, messages) -> None:
        items = []
        for message in messages:
            uuid, version = get_uuid_and_version_from_message(
                message
            )
            items.append(
                self.props.portal.get_item(
                    uuid,
                    version,
                )
            )
        self.props.opensearch.bulk_index_items(
            items
        )
        self.tracker.add_handled_messages(
            messages
        )

    def try_to_handle_messages(self) -> None:
        try:
            self.handle_messages()
        except Exception as e:
            logging.error(e)
            self.tracker.add_failed_messages(
                self.tracker.new_messages
            )

    def get_new_messages_from_queue(self) -> None:
        self.tracker.add_new_messages(
            self.props.bulk_invalidation_queue.get_messages(
                desired_number_of_messages=self.props.messages_to_handle_per_run
            )
        )

    def log_stats(self) -> None:
        if self.tracker.number_all_messages % 100 == 0:
            logging.warning(f'{self.__class__.__name__}: {self.tracker.stats()}')

    def clear(self) -> None:
        self.tracker.clear()

    def run_once(self) -> None:
        self.get_new_messages_from_queue()
        self.try_to_handle_messages()
        self.mark_handled_messages_as_processed()
        self.log_stats()
        self.clear()

    def poll(self):
        while True:
            self.run_once()
