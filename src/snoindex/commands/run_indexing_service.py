import os

from snoindex.config import IndexingServiceConfig
from snoindex.config import get_sqs_client
from snoindex.config import get_opensearch_client

from snoindex.services.indexing import IndexingServiceProps
from snoindex.services.indexing import IndexingService

from snoindex.remote.portal import PortalProps
from snoindex.remote.portal import Portal

from snoindex.repository.opensearch import OpensearchProps
from snoindex.repository.opensearch import Opensearch

from snoindex.repository.queue.sqs import SQSQueueProps
from snoindex.repository.queue.sqs import SQSQueue


def get_indexing_service_config() -> IndexingServiceConfig:
    return IndexingServiceConfig(
        backend_url=os.environ['BACKEND_URL'],
        auth=(
            os.environ['BACKEND_KEY'],
            os.environ['BACKEND_SECRET_KEY'],
        ),
        invalidation_queue_url=os.environ['INVALIDATION_QUEUE_URL'],
        opensearch_client=get_opensearch_client(
            os.environ['OPENSEARCH_URL']
        ),
        opensearch_resources_index=os.environ.get('RESOURCES_INDEX'),
        sqs_client=get_sqs_client(
            os.environ.get('LOCALSTACK_ENDPOINT_URL')
        ),
    )


def make_portal_from_config(config: IndexingServiceConfig) -> Portal:
    return Portal(
        props=PortalProps(
            backend_url=config.backend_url,
            auth=config.auth,
        )
    )


def make_opensearch_from_config(config: IndexingServiceConfig) -> Opensearch:
    return Opensearch(
        props=OpensearchProps(
            client=config.opensearch_client,
            resources_index=config.opensearch_resources_index,
        )
    )


def make_invalidation_queue_from_config(config: IndexingServiceConfig) -> SQSQueue:
    return SQSQueue(
        props=SQSQueueProps(
            client=config.sqs_client,
            queue_url=config.invalidation_queue_url,
        )
    )


def wait(indexing_service: IndexingService) -> None:
    indexing_service.props.invalidation_queue.wait_for_queue_to_exist()
    indexing_service.props.portal.wait_for_portal_connection()
    indexing_service.props.portal.wait_for_access_key_to_exist()
    indexing_service.props.opensearch.wait_for_resources_index_to_exist()


def make_indexing_service_from_config(config: IndexingServiceConfig) -> IndexingService:
    invalidation_queue = make_invalidation_queue_from_config(
        config
    )
    portal = make_portal_from_config(
        config
    )
    opensearch = make_opensearch_from_config(
        config
    )
    indexing_service = IndexingService(
        props=IndexingServiceProps(
            invalidation_queue=invalidation_queue,
            portal=portal,
            opensearch=opensearch,
        )
    )
    wait(indexing_service)
    return indexing_service


def poll() -> None:
    config = get_indexing_service_config()
    indexing_service = make_indexing_service_from_config(config)
    indexing_service.poll()


if __name__ == '__main__':
    poll()
