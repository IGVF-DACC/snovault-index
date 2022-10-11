import boto3

from opensearchpy import OpenSearch

from urllib3.util import Retry

from dataclasses import dataclass

from typing import Optional
from typing import Tuple

from botocore.client import BaseClient


def get_sqs_client(localstack_endpoint_url: Optional[str] = None) -> BaseClient:
    if localstack_endpoint_url is not None:
        return boto3.client(
            'sqs',
            endpoint_url=localstack_endpoint_url,
            aws_access_key_id='testing',
            aws_secret_access_key='testing',
            region_name='us-west-2',
        )
    return boto3.client(
        'sqs'
    )


def get_opensearch_client(url: str) -> OpenSearch:
    return OpenSearch(
        url,
        timeout=30,
        retries=Retry(3),
        retry_on_timeout=True,
    )


@dataclass
class InvalidationServiceConfig:
    transaction_queue_url: str
    invalidation_queue_url: str
    opensearch_client: OpenSearch
    opensearch_resources_index: Optional[str]
    sqs_client: BaseClient


@dataclass
class IndexingServiceConfig:
    backend_url: str
    auth: Tuple[str, str]
    invalidation_queue_url: str
    opensearch_client: OpenSearch
    opensearch_resources_index: Optional[str]
    sqs_client: BaseClient
