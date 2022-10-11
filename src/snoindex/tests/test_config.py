import pytest


def test_get_sqs_client():
    from botocore.client import BaseClient
    from snoindex.config import get_sqs_client
    client = get_sqs_client('http://localstackendpoint:4566')
    assert isinstance(client, BaseClient)


def test_get_opensearch_client():
    from opensearchpy import OpenSearch
    from snoindex.config import get_opensearch_client
    client = get_opensearch_client('http://opensearch')
    assert isinstance(client, OpenSearch)
