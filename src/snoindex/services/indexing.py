import logging

from dataclasses import dataclass

from snoindex.domain.message import InboundMessage

from snoindex.domain.tracker import MessageTracker

from snoindex.repository.queue.sqs import SQSQueue

from snoindex.repository.opensearch import Opensearch

from snoindex.remote.portal import Portal

from typing import List
from typing import Tuple


def get_uuid_and_version_from_message(message: InboundMessage) -> Tuple[str, int]:
    uuid = message.json_body['data']['uuid']
    version = int(message.json_body['metadata']['xid'])
    return (uuid, version)


@dataclass
class IndexingServiceProps:
    invalidation_queue: SQSQueue
    portal: Portal
    opensearch: Opensearch
    messages_to_handle_per_run: int = 1


class IndexingService:

    def __init__(self, props: IndexingServiceProps) -> None:
        self.props = props
        self.tracker = MessageTracker()

    def handle_message(self, message: InboundMessage) -> None:
        uuid, version = get_uuid_and_version_from_message(message)
        item = self.props.portal.get_item(uuid, version)
        self.props.opensearch.delete_by_item(item)
        self.props.opensearch.index_item(item)
        self.tracker.add_handled_messages([message])

    def try_to_handle_messages(self) -> None:
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

    def mark_handled_messages_as_processed(self) -> None:
        self.props.invalidation_queue.mark_as_processed(
            self.tracker.handled_messages
        )

    def get_new_messages_from_queue(self) -> None:
        self.tracker.add_new_messages(
            list(
                self.props.invalidation_queue.get_messages(
                    desired_number_of_messages=self.props.messages_to_handle_per_run
                )
            )
        )

    def _should_log_stats(self) -> bool:
        return (
            self.tracker.number_all_messages != 0
            and self.tracker.number_all_messages % 100 == 0
        )

    def log_stats(self) -> None:
        if self._should_log_stats():
            logging.warning(
                f'{self.__class__.__name__}: {self.tracker.stats()}'
            )

    def clear(self) -> None:
        self.tracker.clear()

    def run_once(self) -> None:
        self.get_new_messages_from_queue()
        self.try_to_handle_messages()
        self.mark_handled_messages_as_processed()
        self.log_stats()
        self.clear()

    def poll(self) -> None:
        while True:
            self.run_once()


@dataclass
class BulkIndexingServiceProps:
    bulk_invalidation_queue: SQSQueue
    portal: Portal
    opensearch: Opensearch
    messages_to_handle_per_run: int = 50


class BulkIndexingService:

    def __init__(self, props: BulkIndexingServiceProps) -> None:
        self.props = props
        self.tracker = MessageTracker()

    def handle_messages(self, messages: List[InboundMessage]) -> None:
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
            self.handle_messages(
                self.tracker.new_messages
            )
        except Exception as e:
            logging.error(e)
            self.tracker.add_failed_messages(
                self.tracker.new_messages
            )

    def mark_handled_messages_as_processed(self) -> None:
        self.props.bulk_invalidation_queue.mark_as_processed(
            self.tracker.handled_messages
        )

    def get_new_messages_from_queue(self) -> None:
        self.tracker.add_new_messages(
            list(
                self.props.bulk_invalidation_queue.get_messages(
                    desired_number_of_messages=self.props.messages_to_handle_per_run
                )
            )
        )

    def _should_log_stats(self) -> bool:
        return (
            self.tracker.number_all_messages != 0
            and self.tracker.number_all_messages % 100 == 0
        )

    def log_stats(self) -> None:
        if self._should_log_stats():
            logging.warning(
                f'{self.__class__.__name__}: {self.tracker.stats()}'
            )

    def clear(self) -> None:
        self.tracker.clear()

    def run_once(self) -> None:
        self.get_new_messages_from_queue()
        self.try_to_handle_messages()
        self.mark_handled_messages_as_processed()
        self.log_stats()
        self.clear()

    def poll(self) -> None:
        while True:
            self.run_once()
