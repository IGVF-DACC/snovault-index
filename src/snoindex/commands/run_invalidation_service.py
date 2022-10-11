import os

from snoindex.config import InvalidationServiceConfig
from snoindex.config import get_sqs_client
from snoindex.config import get_opensearch_client

from snoindex.services.invalidation import InvalidationServiceProps
from snoindex.services.invalidation import InvalidationService

from snoindex.repository.opensearch import OpensearchProps
from snoindex.repository.opensearch import Opensearch

from snoindex.repository.queue.sqs import SQSQueueProps
from snoindex.repository.queue.sqs import SQSQueue


def get_invalidation_service_config() -> InvalidationServiceConfig:
    return InvalidationServiceConfig(
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


def make_opensearch_from_config(config: InvalidationServiceConfig) -> Opensearch:
    return Opensearch(
        props=OpensearchProps(
            client=config.opensearch_client,
            resources_index=config.opensearch_resources_index,
        )
    )


def make_transaction_queue_from_config(config: InvalidationServiceConfig) -> SQSQueue:
    return SQSQueue(
        props=SQSQueueProps(
            client=config.sqs_client,
            queue_url=config.transaction_queue_url,
        )
    )


def make_invalidation_queue_from_config(config: InvalidationServiceConfig) -> SQSQueue:
    return SQSQueue(
        props=SQSQueueProps(
            client=config.sqs_client,
            queue_url=config.invalidation_queue_url,
        )
    )


def wait(invalidation_service: InvalidationService) -> None:
    invalidation_service.props.transaction_queue.wait_for_queue_to_exist()
    invalidation_service.props.invalidation_queue.wait_for_queue_to_exist()
    invalidation_service.props.opensearch.wait_for_resources_index_to_exist()


def make_invalidation_service_from_config(config: InvalidationServiceConfig) -> InvalidationService:
    transaction_queue = make_transaction_queue_from_config(
        config
    )
    invalidation_queue = make_invalidation_queue_from_config(
        config
    )
    opensearch = make_opensearch_from_config(
        config
    )
    invalidation_service = InvalidationService(
        props=InvalidationServiceProps(
            transaction_queue=transaction_queue,
            invalidation_queue=invalidation_queue,
            opensearch=opensearch
        )
    )
    wait(invalidation_service)
    return invalidation_service


def poll() -> None:
    config = get_invalidation_service_config()
    invalidation_service = make_invalidation_service_from_config(config)
    invalidation_service.poll()


if __name__ == '__main__':
    poll()
