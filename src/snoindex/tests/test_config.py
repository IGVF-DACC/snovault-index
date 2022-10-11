import pytest


def test_config_get_sqs_client():
    from botocore.client import BaseClient
    from snoindex.config import get_sqs_client
    client = get_sqs_client('http://localstackendpoint:4566')
    assert isinstance(client, BaseClient)


def test_config_get_opensearch_client():
    from opensearchpy import OpenSearch
    from snoindex.config import get_opensearch_client
    client = get_opensearch_client('http://opensearch')
    assert isinstance(client, OpenSearch)


def test_config_invalidation_service_config(opensearch_client):
    from snoindex.config import InvalidationServiceConfig
    from snoindex.config import get_sqs_client
    config = InvalidationServiceConfig(
        transaction_queue_url='some-queue-url',
        invalidation_queue_url='another-queue-url',
        opensearch_client=opensearch_client,
        opensearch_resources_index='some-index',
        sqs_client=get_sqs_client('http://localstackendpoint:4566'),
    )
    assert isinstance(config, InvalidationServiceConfig)
    assert config.transaction_queue_url == 'some-queue-url'


def test_config_indexing_service_config(opensearch_client):
    from snoindex.config import IndexingServiceConfig
    from snoindex.config import get_sqs_client
    config = IndexingServiceConfig(
        backend_url='some-url',
        auth=('some', 'auth'),
        invalidation_queue_url='some-queue-url',
        opensearch_client=opensearch_client,
        opensearch_resources_index='some-index',
        sqs_client=get_sqs_client('http://localstackendpoint:4566')
    )
    assert config.backend_url == 'some-url'
    assert isinstance(config, IndexingServiceConfig)
