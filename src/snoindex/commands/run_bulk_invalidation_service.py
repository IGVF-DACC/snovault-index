import os

from snoindex.config import BulkInvalidationServiceConfig
from snoindex.config import get_sqs_client
from snoindex.config import get_opensearch_client

from snoindex.services.invalidation import BulkInvalidationServiceProps
from snoindex.services.invalidation import BulkInvalidationService

from snoindex.repository.opensearch import OpensearchProps
from snoindex.repository.opensearch import Opensearch

from snoindex.repository.queue.sqs import SQSQueueProps
from snoindex.repository.queue.sqs import SQSQueue


def get_bulk_invalidation_service_config() -> BulkInvalidationServiceConfig:
    return BulkInvalidationServiceConfig(
        transaction_queue_url=os.environ['TRANSACTION_QUEUE_URL'],
        invalidation_queue_url=os.environ['INVALIDATION_QUEUE_URL'],
        opensearch_client=get_opensearch_client(
            os.environ['OPENSEARCH_URL']
        ),
        opensearch_resources_index=os.environ.get('RESOURCES_INDEX'),
        sqs_client=get_sqs_client(
            os.environ.get('LOCALSTACK_ENDPOINT_URL')
        ),
    )


def make_opensearch_from_config(config: BulkInvalidationServiceConfig) -> Opensearch:
    return Opensearch(
        props=OpensearchProps(
            client=config.opensearch_client,
            resources_index=config.opensearch_resources_index,
        )
    )


def make_transaction_queue_from_config(config: BulkInvalidationServiceConfig) -> SQSQueue:
    return SQSQueue(
        props=SQSQueueProps(
            client=config.sqs_client,
            queue_url=config.transaction_queue_url,
            visibility_timeout=1800,
        )
    )


def make_invalidation_queue_from_config(config: BulkInvalidationServiceConfig) -> SQSQueue:
    return SQSQueue(
        props=SQSQueueProps(
            client=config.sqs_client,
            queue_url=config.invalidation_queue_url,
        )
    )


def wait(bulk_invalidation_service: BulkInvalidationService) -> None:
    bulk_invalidation_service.props.transaction_queue.wait_for_queue_to_exist()
    bulk_invalidation_service.props.invalidation_queue.wait_for_queue_to_exist()
    bulk_invalidation_service.props.opensearch.wait_for_resources_index_to_exist()


def make_bulk_invalidation_service_from_config(config: BulkInvalidationServiceConfig) -> BulkInvalidationService:
    transaction_queue = make_transaction_queue_from_config(
        config
    )
    invalidation_queue = make_invalidation_queue_from_config(
        config
    )
    opensearch = make_opensearch_from_config(
        config
    )
    bulk_invalidation_service = BulkInvalidationService(
        props=BulkInvalidationServiceProps(
            transaction_queue=transaction_queue,
            invalidation_queue=invalidation_queue,
            opensearch=opensearch,
            messages_to_handle_per_run=5000,
            related_uuids_search_batch_size=1000,
        )
    )
    wait(bulk_invalidation_service)
    return bulk_invalidation_service


def poll() -> None:
    config = get_bulk_invalidation_service_config()
    bulk_invalidation_service = make_bulk_invalidation_service_from_config(
        config
    )
    bulk_invalidation_service.poll()


if __name__ == '__main__':
    poll()
