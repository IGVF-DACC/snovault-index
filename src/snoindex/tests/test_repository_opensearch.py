import pytest


def test_repository_opensearch_get_related_uuids_query():
    from snoindex.repository.opensearch import get_related_uuids_query
    actual = get_related_uuids_query(
        updated=['abc', 'zyx'],
        renamed=['something']
    )
    expected = {
        'query': {
            'bool': {
                'should': [
                    {
                        'terms': {
                            'embedded_uuids': ['abc', 'zyx']
                        }
                    },
                    {
                        'terms': {
                            'linked_uuids': ['something']
                        }
                    }
                ]
            }
        },
        '_source': False
    }
    assert actual == expected


def test_repository_opensearch_get_search(opensearch_client):
    from opensearch_dsl import Search
    from snoindex.repository.opensearch import get_search
    search = get_search(opensearch_client, 'index1')
    assert isinstance(search, Search)
    assert search._index == ['index1']


def test_repository_opensearch_opensearch_init(opensearch_props):
    from snoindex.repository.opensearch import Opensearch
    os = Opensearch(
        props=opensearch_props
    )
    assert isinstance(os, Opensearch)


def test_repository_opensearch_opensearch_get_related_uuids_from_updated_and_renamed():
    assert False


def test_repository_opensearch_opensearch_index_item():
    assert False


def test_repository_opensearch_opensearch_bulk_index_items():
    assert False


def test_repository_opensearch_opensearch_referesh_resources_index():
    assert False


def test_repository_opensearch_opensearch_wait_for_resources_index_to_exist():
    assert False
