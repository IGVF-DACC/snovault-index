import logging

from dataclasses import dataclass

from snoindex.domain.message import InboundMessage
from snoindex.domain.message import OutboundMessage

from snoindex.domain.tracker import MessageTracker

from snoindex.repository.queue.sqs import SQSQueue

from snoindex.repository.opensearch import Opensearch

from typing import Any
from typing import Iterable
from typing import List
from typing import Set
from typing import Tuple
from typing import cast


def batch(items: List[Any], batchsize: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), batchsize):
        yield items[i:i + batchsize]


def get_updated_uuids_from_transaction(message: InboundMessage) -> List[str]:
    return cast(
        List[str],
        message.json_body['data']['payload']['updated'],
    )


def get_renamed_uuids_from_transaction(message: InboundMessage) -> List[str]:
    return cast(
        List[str],
        message.json_body['data']['payload']['renamed'],
    )


def get_all_uuids_from_transaction(message: InboundMessage) -> List[str]:
    uuids = set()
    uuids.update(
        get_updated_uuids_from_transaction(message)
    )
    uuids.update(
        get_renamed_uuids_from_transaction(message)
    )
    return list(uuids)


def make_unique_id(uuid: str, xid: str) -> str:
    return f'{uuid}-{xid}'


def make_outbound_message(message: InboundMessage, uuid: str) -> OutboundMessage:
    xid = message.json_body['metadata']['xid']
    body = {
        'metadata': {
            'xid': xid,
            'tid': message.json_body['metadata']['tid'],
        },
        'data': {
            'uuid': uuid,
        }
    }
    outbound_message = OutboundMessage(
        unique_id=make_unique_id(uuid, xid),
        body=body,
    )
    return outbound_message


@dataclass
class InvalidationServiceProps:
    transaction_queue: SQSQueue
    invalidation_queue: SQSQueue
    opensearch: Opensearch
    messages_to_handle_per_run: int = 1


class InvalidationService:

    def __init__(self, props: InvalidationServiceProps) -> None:
        self.props = props
        self.tracker = MessageTracker()

    def invalidate_all_uuids_from_transaction(self, message: InboundMessage) -> None:
        outbound_messages: List[OutboundMessage] = []
        uuids = get_all_uuids_from_transaction(message)
        for uuid in uuids:
            outbound_messages.append(
                make_outbound_message(
                    message,
                    uuid,
                )
            )
        self.props.invalidation_queue.send_messages(
            outbound_messages
        )

    def invalidate_all_related_uuids(self, message: InboundMessage) -> None:
        outbound_messages = []
        already_invalidated_uuids = get_all_uuids_from_transaction(message)
        updated = get_updated_uuids_from_transaction(message)
        renamed = get_renamed_uuids_from_transaction(message)
        related_uuids = self.props.opensearch.get_related_uuids_from_updated_and_renamed(
            updated,
            renamed
        )
        for uuid in related_uuids:
            if uuid in already_invalidated_uuids:
                continue
            outbound_messages.append(
                make_outbound_message(
                    message,
                    uuid,
                )
            )
        self.props.invalidation_queue.send_messages(
            outbound_messages
        )

    def handle_message(self, message: InboundMessage) -> None:
        self.invalidate_all_uuids_from_transaction(message)
        self.invalidate_all_related_uuids(message)
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
        self.props.transaction_queue.mark_as_processed(
            self.tracker.handled_messages
        )

    def get_new_messages_from_queue(self) -> None:
        self.tracker.add_new_messages(
            list(
                self.props.transaction_queue.get_messages(
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
class BulkInvalidationServiceProps:
    transaction_queue: SQSQueue
    invalidation_queue: SQSQueue
    opensearch: Opensearch
    messages_to_handle_per_run: int = 5000
    related_uuids_search_batch_size: int = 1000


class BulkInvalidationService:

    def __init__(self, props: BulkInvalidationServiceProps) -> None:
        self.props = props
        self.tracker = MessageTracker()

    def parse_uuids_from_messages(self, messages: List[InboundMessage]) -> Tuple[Set[str], Set[str], Set[str]]:
        all_uuids_from_transactions = set()
        all_updated_uuids_from_transactions = set()
        all_renamed_uuids_from_transactions = set()
        for message in messages:
            all_uuids = get_all_uuids_from_transaction(message)
            all_uuids_from_transactions.update(all_uuids)
            updated_uuids = get_updated_uuids_from_transaction(message)
            all_updated_uuids_from_transactions.update(updated_uuids)
            renamed_uuids = get_renamed_uuids_from_transaction(message)
            all_renamed_uuids_from_transactions.update(renamed_uuids)
        return (
            all_uuids_from_transactions,
            all_updated_uuids_from_transactions,
            all_renamed_uuids_from_transactions,
        )

    def get_related_uuids(
            self,
            all_uuids: Set[str],
            all_updated_uuids: Set[str],
            all_renamed_uuids: Set[str]
    ) -> Set[str]:
        updated: List[str] = list(all_updated_uuids)
        renamed: List[str] = list(all_renamed_uuids)
        related_uuids: Set[str] = set()
        # Do this in batches to not overload Opensearch.
        for batch_updated in batch(updated, batchsize=self.props.related_uuids_search_batch_size):
            related_uuids.update(
                self.props.opensearch.get_related_uuids_from_updated_and_renamed(
                    updated=batch_updated,
                    renamed=[],
                )
            )
        for batch_renamed in batch(renamed, batchsize=self.props.related_uuids_search_batch_size):
            related_uuids.update(
                self.props.opensearch.get_related_uuids_from_updated_and_renamed(
                    updated=[],
                    renamed=batch_renamed,
                )
            )
        return related_uuids - all_uuids

    def make_outbound_messages(self, uuids: Set[str], message: InboundMessage) -> List[OutboundMessage]:
        outbound = []
        for uuid in uuids:
            outbound.append(
                make_outbound_message(
                    message,
                    uuid
                )
            )
        return outbound

    def handle_messages(self, messages: List[InboundMessage]) -> None:
        # Split uuids into directly modified, updated, and renamed.
        all_uuids, all_updated_uuids, all_renamed_uuids = self.parse_uuids_from_messages(
            messages
        )
        # Objects that were directly modified will be sent to indexing queue first.
        primary_outbound = self.make_outbound_messages(
            all_uuids,
            messages[0],
        )
        logging.warning(
            f'{self.__class__.__name__}: Primary outbound = {len(primary_outbound)}'
        )
        # Search for all objects that are invalidated because of the directly modified objects.
        related_uuids = self.get_related_uuids(
            all_uuids,
            all_updated_uuids,
            all_renamed_uuids,
        )
        # Objects that are invalidated because of another object will be sent to indexing queue second.
        related_outbound = self.make_outbound_messages(
            related_uuids,
            messages[0],
        )
        logging.warning(
            f'{self.__class__.__name__}: Related outbound = {len(related_outbound)}'
        )
        # Send directly modified objects to invalidation queue.
        self.props.invalidation_queue.send_messages(
            primary_outbound
        )
        # Send invalidated but not directly modified objects to invalidation queue.
        self.props.invalidation_queue.send_messages(
            related_outbound
        )
        # Record handled messages.
        self.tracker.add_handled_messages(messages)

    def try_to_handle_messages(self) -> None:
        messages = self.tracker.new_messages
        if not messages:
            return
        try:
            self.handle_messages(messages)
        except Exception as e:
            logging.error(e)
            self.tracker.add_failed_messages(
                messages
            )

    def mark_handled_messages_as_processed(self) -> None:
        self.props.transaction_queue.mark_as_processed(
            self.tracker.handled_messages
        )

    def get_new_messages_from_queue(self) -> None:
        self.tracker.add_new_messages(
            list(
                self.props.transaction_queue.get_messages(
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
