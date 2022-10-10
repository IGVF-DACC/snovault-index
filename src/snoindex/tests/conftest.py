import pytest


@pytest.fixture
def inbound_message():
    import json
    from snoindex.domain.message import InboundMessage
    json_body = {
        'some': [
            'json',
            'string'
        ],
        'with': {
            'meta': 'data',
        }
    }
    json_string = json.dumps(json_body)
    return InboundMessage(
        message_id='abc',
        receipt_handle='abc123',
        md5_of_body='ccc',
        body=json_string
    )


@pytest.fixture
def portal_props():
    from snoindex.remote.portal import PortalProps
    return PortalProps(
        backend_url='testing.domain',
        auth=('some', 'auth'),
    )


@pytest.fixture
def raw_index_data_view():
    return {
        'audit': {},
        'embedded': {
            'lab': {
                'pi': '/users/7e763d5a-634b-4181-a812-4361183359e9/',
                'fax': '000-000-0000',
                'city': 'Seattle',
                'name': 'robert-waterston',
                'state': 'WA',
                'title': 'Robert Waterston, UW',
                'awards': ['/awards/U41HG007355/'],
                'phone1': '000-000-0000',
                'phone2': '000-000-0000',
                'status': 'current',
                'country': 'USA',
                'address1': 'Department of Genome Sciences',
                'address2': '3720 15th Ave NE, Foege S-350D, Box 355065',
                'postal_code': '98195-5065',
                'institute_name': 'University of Washington',
                'schema_version': '3',
                'institute_label': 'UW',
                '@id': '/labs/robert-waterston/',
                '@type': ['Lab', 'Item'],
                'uuid': '44324de6-c057-451f-a8f1-1d9a6cb2762c'
            },
            'award': {
                'pi': '/users/09d05b87-4d30-4dfb-b243-3327005095f2/',
                'rfa': 'ENCODE3',
                'url': 'http://projectreporter.nih.gov/project_info_details.cfm?aid=8402436',
                'name': 'U54HG007004',
                'title': 'LANDSCAPE OF TRANSCRIPTION IN HUMAN AND MOUSE',
                'status': 'current',
                'project': 'ENCODE',
                'end_date': '2016-07-31',
                'start_date': '2012-09-21',
                'description': "The overall goal of this project is to generate fine-structure RNA maps in human and mouse (C57BL/6NJ) tissues and primary cell lines using a variety of high-throughput sequencing platforms, to evaluate the biological importance of novel transcripts by determining if evidence of their translated products can be identified. From each sample analyzed, we propose to isolate long (>200 nucleotides) and short (< 200 nucleotides) RNA in biological duplicate. Illumina-based maps for these samples will initially be generated using (1) RNA sequencing (-seq) of ribosomal (r-)RNA depleted long total RNA. (2) RNA-seq of tobacco acid pyrophosphatase (TAP) pre-treated short RNA (3) Pair-end Cap Analysis of Gene Expression (PE-CAGE) of total RNA. Additionally, for a subset of primary cell lines we will generate the above libraries from nuclear and cytoplasmic subcellular fractions. Long RNA-Seq data will be distilled down into functional elements consisting of splice junctions, polyadenylatio sites and de novo genes and transcripts. The short RNA data will be distilled into contigs representing the 5' ends of short RNAs up to the read length. PE-CAGE data will be analyzed to form clusters representing the 5' ends of transcripts linked to a tag internal to the transcript body. Importantly, each element will be assessed for reproducibility using a nonparametric Irreproducible Detection Rate (nplDR) script. Collectively, these data will allow for the detection of novel transcribed regions and supportive information as to the location of promoter regions and subcellular residence of transcripts. In aggregate, these data will be used to generate models of both noncoding and protein coding transcripts and to distinguish isoforms at complex loci necessary to obtain a comprehensive view of mammalian transcriptomes. For a subset of these samples we will simultaneously collect the genome sequence of the human donors to provide a reference map that will be used to map the RNA data against and derive information concerning allele-specific expression and RNA editing. Unannotated transcript models will be tested using long-range (PacBio/454) sequencing. Lastly, proteogenomic analysis will be done and the results compared against the unannotated transcripts.",
                'viewing_group': 'ENCODE',
                'schema_version': '3',
                '@id': '/awards/U54HG007004/',
                '@type': ['Award', 'Item'],
                'uuid': '90be62e4-0757-4097-b5cf-2e6a20240a6f'
            },
            'method': 'hand-packed',
            'status': 'released',
            'accession': 'SNOSS001SER',
            'description': 'example snowball',
            'date_created': '2022-10-04T20:35:59.088790+00:00',
            'submitted_by': {
                '@id': '/users/0abbd494-b852-433c-b360-93996f679dae/',
                '@type': ['User', 'Item'],
                'uuid': '0abbd494-b852-433c-b360-93996f679dae',
                'lab': '/labs/thomas-gingeras/',
                'title': 'Ad Est'
            },
            'date_released': '2016-01-01',
            'schema_version': '2',
            'alternate_accessions': [],
            '@id': '/snowballs/SNOSS001SER/',
            '@type': ['Snowball', 'Snowset', 'Item'],
            'uuid': '4cead359-10e9-49a8-9d20-f05b2499b919',
            'month_released': 'January, 2016',
            'snowflakes': [],
            'test_calculated': 'test_calculated_value',
            'another_test_calculated': 'another_test_calculated_value',
            'conditional_test_calculated': 'conditional_test_calculated_value'
        },
        'embedded_uuids': [
            '09d05b87-4d30-4dfb-b243-3327005095f2',
            '0abbd494-b852-433c-b360-93996f679dae',
            '44324de6-c057-451f-a8f1-1d9a6cb2762c',
            '4cead359-10e9-49a8-9d20-f05b2499b919',
            '7e763d5a-634b-4181-a812-4361183359e9',
            '90be62e4-0757-4097-b5cf-2e6a20240a6f',
            'b0b9c607-f8b4-4f02-93f4-9895b461334b'
        ],
        'item_type': 'snowball',
        'linked_uuids': [
            '09d05b87-4d30-4dfb-b243-3327005095f2',
            '0abbd494-b852-433c-b360-93996f679dae',
            '3a3ffb78-7f16-4135-87d6-5a7ad1246dcb',
            '44324de6-c057-451f-a8f1-1d9a6cb2762c',
            '4cead359-10e9-49a8-9d20-f05b2499b919',
            '7e763d5a-634b-4181-a812-4361183359e9',
            '90be62e4-0757-4097-b5cf-2e6a20240a6f',
            'b0b9c607-f8b4-4f02-93f4-9895b461334b',
            'dfc72c8c-d45c-4acd-979b-49fc93cf3c62'
        ],
        'links': {
            'award': [
                '90be62e4-0757-4097-b5cf-2e6a20240a6f'
            ],
            'lab': [
                '44324de6-c057-451f-a8f1-1d9a6cb2762c'
            ],
            'submitted_by': [
                '0abbd494-b852-433c-b360-93996f679dae'
            ]
        },
        'object': {
            'lab': '/labs/robert-waterston/',
            'award': '/awards/U54HG007004/',
            'method': 'hand-packed',
            'status': 'released',
            'accession': 'SNOSS001SER',
            'description': 'example snowball',
            'date_created': '2022-10-04T20:35:59.088790+00:00',
            'submitted_by': '/users/0abbd494-b852-433c-b360-93996f679dae/',
            'date_released': '2016-01-01',
            'schema_version': '2',
            'alternate_accessions': [],
            '@id': '/snowballs/SNOSS001SER/',
            '@type': ['Snowball', 'Snowset', 'Item'],
            'uuid': '4cead359-10e9-49a8-9d20-f05b2499b919',
            'month_released': 'January, 2016',
            'snowflakes': [],
            'test_calculated': 'test_calculated_value',
            'another_test_calculated': 'another_test_calculated_value',
            'conditional_test_calculated': 'conditional_test_calculated_value'
        },
        'paths': [
            '/4cead359-10e9-49a8-9d20-f05b2499b919',
            '/SNOSS001SER',
            '/snowballs/4cead359-10e9-49a8-9d20-f05b2499b919',
            '/snowballs/SNOSS001SER'
        ],
        'principals_allowed': {
            'view': ['system.Everyone'],
            'edit': ['group.admin'],
            'audit': ['system.Everyone']
        },
        'properties': {
            'lab': '44324de6-c057-451f-a8f1-1d9a6cb2762c',
            'award': '90be62e4-0757-4097-b5cf-2e6a20240a6f',
            'method': 'hand-packed',
            'status': 'released',
            'accession': 'SNOSS001SER',
            'description': 'example snowball',
            'date_created': '2022-10-04T20:35:59.088790+00:00',
            'submitted_by': '0abbd494-b852-433c-b360-93996f679dae',
            'date_released': '2016-01-01',
            'schema_version': '2',
            'alternate_accessions': []
        }, 'propsheets': {},
        'tid': '93c8ba77-64ea-4c84-ac1c-d5429192856d',
        'unique_keys': {
            'accession': ['SNOSS001SER']
        },
        'uuid': '4cead359-10e9-49a8-9d20-f05b2499b919'
    }


@pytest.fixture
def mocked_portal(portal_props, raw_index_data_view, mocker):
    from snoindex.remote.portal import Portal
    return_data = mocker.Mock()
    return_data.json = lambda: raw_index_data_view
    mocker.patch(
        'snoindex.remote.portal.make_authorized_remote_request',
        return_value=return_data
    )
    return Portal(
        props=portal_props
    )


@pytest.fixture
def opensearch_client(url='http://opensearch:9200'):
    from urllib3.util import Retry
    from opensearchpy import OpenSearch
    return OpenSearch(url, timeout=30, retries=Retry(3))


@pytest.fixture
def opensearch_props(opensearch_client):
    from snoindex.repository.opensearch import OpensearchProps
    return OpensearchProps(
        client=opensearch_client,
        resources_index='snowball',
    )


@pytest.fixture
def generic_mapping():
    return {
        'mappings': {
            'dynamic_templates': [
                {
                    'template_principals_allowed': {
                        'path_match': 'principals_allowed.*',
                        'match_mapping_type': 'string',
                        'mapping': {
                            'type': 'keyword',
                        },
                    },
                },
                {
                    'template_unique_keys': {
                        'path_match': 'unique_keys.*',
                        'match_mapping_type': 'string',
                        'mapping': {
                            'type': 'keyword',
                            'copy_to': '_all',
                        },
                    },
                },
                {
                    'template_links': {
                        'path_match': 'links.*',
                        'match_mapping_type': 'string',
                        'mapping': {
                            'type': 'keyword',
                        },
                    },
                },
                {
                    'strings': {
                        'match_mapping_type': 'string',
                        'mapping': {
                            'type': 'keyword',
                        },
                    },
                },
                {
                    'integers': {
                        'match_mapping_type': 'long',
                        'mapping': {
                            'type': 'long',
                            'fields': {
                                'raw': {
                                    'type': 'keyword'
                                }
                            },
                        },
                    },
                }
            ],
            'properties': {
                '_all': {
                    'type': 'text',
                    'store': False,
                },
                'uuid': {
                    'type': 'keyword',
                    'copy_to': '_all',
                },
                'tid': {
                    'type': 'keyword',
                },
                'item_type': {
                    'type': 'keyword',
                    'copy_to': '_all',
                },
                'object': {
                    'type': 'object',
                    'enabled': False,
                },
                'properties': {
                    'type': 'object',
                    'enabled': False,
                },
                'propsheets': {
                    'type': 'object',
                    'enabled': False,
                },
                'embedded_uuids': {
                    'type': 'keyword',
                },
                'linked_uuids': {
                    'type': 'keyword',
                },
                'paths': {
                    'type': 'keyword',
                }
            }
        }
    }


@pytest.fixture
def get_all_results():
    def search(client):
        return client.search(
            body={
                'query': {
                    'match_all': {},
                }
            },
            version=True,
        )
    return search


@pytest.fixture(scope='function')
def opensearch_repository(opensearch_props, generic_mapping):
    from snoindex.repository.opensearch import Opensearch
    os = Opensearch(
        props=opensearch_props
    )
    os.clear()
    os.props.client.indices.create(
        index=os.props.resources_index,
        body=generic_mapping
    )
    yield os
    os.clear()


@pytest.fixture
def localstack_sqs_client():
    import boto3
    return boto3.client(
        'sqs',
        endpoint_url='http://localstack:4566',
        aws_access_key_id='testing',
        aws_secret_access_key='testing',
        region_name='us-west-2',
    )


@pytest.fixture(scope='function')
def queue_for_testing(localstack_sqs_client):
    from snoindex.repository.queue.sqs import SQSQueue
    from snoindex.repository.queue.sqs import SQSQueueProps
    queue = SQSQueue(
        props=SQSQueueProps(
            client=localstack_sqs_client,
            queue_url='http://localstack:4566/000000000000/queue-for-testing'
        )
    )
    queue.wait_for_queue_to_exist()
    queue.clear()
    yield queue
    queue.clear()


@pytest.fixture(scope='function')
def transaction_queue(localstack_sqs_client):
    from snoindex.repository.queue.sqs import SQSQueue
    from snoindex.repository.queue.sqs import SQSQueueProps
    queue = SQSQueue(
        props=SQSQueueProps(
            client=localstack_sqs_client,
            queue_url='http://localstack:4566/000000000000/transaction-queue'
        )
    )
    queue.wait_for_queue_to_exist()
    queue.clear()
    yield queue
    queue.clear()


@pytest.fixture(scope='function')
def invalidation_queue(localstack_sqs_client):
    from snoindex.repository.queue.sqs import SQSQueue
    from snoindex.repository.queue.sqs import SQSQueueProps
    queue = SQSQueue(
        props=SQSQueueProps(
            client=localstack_sqs_client,
            queue_url='http://localstack:4566/000000000000/invalidation-queue'
        )
    )
    queue.wait_for_queue_to_exist()
    queue.clear()
    yield queue
    queue.clear()


@pytest.fixture
def mock_transaction_message():
    import json
    from snoindex.domain.message import InboundMessage
    message_body = {
        'metadata': {
            'xid': 1234,
            'tid': 'abcd',
        },
        'data': {
            'payload': {
                'updated': [
                    '09d05b87-4d30-4dfb-b243-3327005095f2',
                ],
                'renamed': [
                    '09d05b87-4d30-4dfb-b243-3327005095f2',
                ]
            }
        }
    }
    return InboundMessage(
        message_id=message_body['metadata']['tid'],
        receipt_handle='xyz',
        md5_of_body='abc',
        body=json.dumps(message_body),
    )


@pytest.fixture
def mock_transaction_message_outbound():
    from snoindex.domain.message import OutboundMessage
    message_body = {
        'metadata': {
            'xid': 1234,
            'tid': 'abcd',
        },
        'data': {
            'payload': {
                'updated': [
                    '09d05b87-4d30-4dfb-b243-3327005095f2',
                ],
                'renamed': [
                    '09d05b87-4d30-4dfb-b243-3327005095f2',
                ]
            }
        }
    }
    return OutboundMessage(
        unique_id=message_body['metadata']['tid'],
        body=message_body,
    )


@pytest.fixture
def invalidation_service_props(transaction_queue, invalidation_queue, opensearch_repository):
    from snoindex.services.invalidation import InvalidationServiceProps
    return InvalidationServiceProps(
        transaction_queue=transaction_queue,
        invalidation_queue=invalidation_queue,
        opensearch=opensearch_repository
    )


@pytest.fixture
def invalidation_service(invalidation_service_props):
    from snoindex.services.invalidation import InvalidationService
    return InvalidationService(
        props=invalidation_service_props
    )


@pytest.fixture
def mock_invalidation_message():
    import json
    from snoindex.domain.message import InboundMessage
    message_body = {
        'metadata': {
            'xid': 1234,
            'tid': 'abcd',
        },
        'data': {
            'uuid': '09d05b87-4d30-4dfb-b243-3327005095f2'
        }
    }
    return InboundMessage(
        message_id=message_body['metadata']['tid'],
        receipt_handle='xyz',
        md5_of_body='abc',
        body=json.dumps(message_body),
    )


@pytest.fixture
def mock_invalidation_message_outbound():
    from snoindex.domain.message import OutboundMessage
    message_body = {
        'metadata': {
            'xid': 1234,
            'tid': 'abcd',
        },
        'data': {
            'uuid': '09d05b87-4d30-4dfb-b243-3327005095f2'
        }
    }
    return OutboundMessage(
        unique_id=message_body['metadata']['tid'],
        body=message_body,
    )


@pytest.fixture
def indexing_service_props(transaction_queue, invalidation_queue, opensearch_repository, mocked_portal):
    from snoindex.services.indexing import IndexingServiceProps
    return IndexingServiceProps(
        invalidation_queue=invalidation_queue,
        portal=mocked_portal,
        opensearch=opensearch_repository,
    )


@pytest.fixture
def indexing_service(indexing_service_props):
    from snoindex.services.indexing import IndexingService
    return IndexingService(
        props=indexing_service_props
    )
