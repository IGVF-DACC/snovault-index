from dataclasses import dataclass

from snoindex.domain.tracker import MessageTracker
from snoindex.repository.queue.sqs import SQSQueue
from snoindex.repository.queue.message import OutboundMessage
from snoindex.repository.opensearch import Opensearch


@dataclass
class InvalidationServiceProps:
    transaction_queue: SQSQueue
    invalidation_queue: SQSQueue
    opensearch: Opensearch
    messages_to_handle_per_run: int = 1


def get_updated_uuids_from_transaction(message):
    message.json_body['data']['payload']['updated']


def get_renamed_uuids_from_transaction(message):
    payload = message.json_body['data']['payload']['renamed']


def get_all_uuids_from_transaction(message):
    uuids = set()
    uuids.update(
        get_updated_uuids_from_transaction(message)
    )
    uuids.update(
        get_renamed_uuids_from_transaction(message)
    )
    return list(uuids)


def make_unique_id(uuid, xid):
    return f'{uuid}-{xid}'


def make_outbound_message(message, uuid):
    xid = message.json_body['metadata']['xid']
    message = {
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
        body=message,
    )
    return outbound_message


class InvalidationService:

    def __init__(self, props: InvalidationServiceProps):
        self.props = props
        self.tracker = MessageTracker()

    def invalidate_all_uuids_from_transaction(self, message):
        outbound_messages = []
        uuids = get_all_uuids_from_transaction(message)
        for uuid in uuids:
            outbound_messages.append(
                make_outbound_message(
                    message,
                    uuid
                )
            )
        self.props.invalidation_queue.send_messages(
            outbound_messages
        )

    def invalidate_all_related_uuids(self, message):
        outbound_messages = []
        already_invalidated_uuids = get_all_uuids_from_transaction(messages)
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

    def handle_message(self, messages) -> None:
        self.invalidate_all_uuids_from_transaction(message)
        self.invalidate_all_related_uuids(message)
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

    def mark_handled_messages_as_processed(self) -> None:
        self.props.transaction_queue.mark_as_processed(
            self.tracker.handled_messages
        )

    def get_new_messages_from_transaction_queue(self) -> None:
        self.tracker.add_new_messages(
            self.props.transaction_queue.get_messages(
                desired_number_of_messages=self.props.messages_to_handle_per_run
            )
        )

    def log_stats(self) -> None:
        if self.tracker.number_all_messages % 100 == 0:
            logging.warning(f'{self.__class__.__name__}: {self.tracker.stats()}')

    def clear(self) -> None:
        self.tracker.clear()

    def run_once(self) -> None:
        self.get_new_messages_from_transaction_queue()
        self.try_to_handle_messages()
        self.mark_handled_messages_as_processed()
        self.log_stats()
        self.clear()

    def poll(self):
        while True:
            self.run_once()
