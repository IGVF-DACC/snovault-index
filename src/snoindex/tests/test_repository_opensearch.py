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


@pytest.mark.integration
def test_repository_opensearch_opensearch_get_related_uuids_from_updated_and_renamed(opensearch_props, mocked_portal, generic_mapping):
    from snoindex.repository.opensearch import Opensearch
    os = Opensearch(
        props=opensearch_props
    )
    os.clear()
    os.props.client.indices.create(
        index=os.props.resources_index, body=generic_mapping)
    item = mocked_portal.get_item('xyz123', 4)
    os.index_item(item)
    related_uuids = list(
        os.get_related_uuids_from_updated_and_renamed(
            updated=[],
            renamed=[],
        )
    )
    assert len(related_uuids) == 0
    related_uuids = list(
        os.get_related_uuids_from_updated_and_renamed(
            updated=['09d05b87-4d30-4dfb-b243-3327005095f2'],
            renamed=[],
        )
    )
    assert len(related_uuids) == 1
    assert related_uuids[0] == 'xyz123'
    item = mocked_portal.get_item('xyz345', 4)
    os.index_item(item)
    related_uuids = list(
        os.get_related_uuids_from_updated_and_renamed(
            updated=[],
            renamed=['dfc72c8c-d45c-4acd-979b-49fc93cf3c62'],
        )
    )
    assert len(related_uuids) == 2
    assert 'xyz123' in related_uuids
    assert 'xyz345' in related_uuids


@pytest.mark.integration
def test_repository_opensearch_opensearch_index_item(opensearch_repository, mocked_portal, get_all_results):
    item = mocked_portal.get_item('xyz123', 4)
    opensearch_repository.index_item(item)
    opensearch_repository.refresh_resources_index()
    for hit in get_all_results(opensearch_repository.props.client)['hits']['hits']:
        assert hit['_version'] == 4
    # Higher version allowed.
    item = mocked_portal.get_item('xyz123', 5)
    opensearch_repository.index_item(item)
    opensearch_repository.refresh_resources_index()
    for hit in get_all_results(opensearch_repository.props.client)['hits']['hits']:
        assert hit['_version'] == 5
    # Lower version ignored.
    item = mocked_portal.get_item('xyz123', 3)
    opensearch_repository.index_item(item)
    opensearch_repository.refresh_resources_index()
    for hit in get_all_results(opensearch_repository.props.client)['hits']['hits']:
        assert hit['_version'] == 5


@pytest.mark.integration
def test_repository_opensearch_opensearch_bulk_index_items(opensearch_repository, mocked_portal, get_all_results):
    item1 = mocked_portal.get_item('xyz123', 4)
    item2 = mocked_portal.get_item('xyz345', 4)
    opensearch_repository.bulk_index_items(
        [
            item1,
            item2,
        ]
    )
    opensearch_repository.refresh_resources_index()
    results = list(
        get_all_results(
            opensearch_repository.props.client
        )['hits']['hits']
    )
    assert len(results) == 2


@pytest.mark.integration
def test_repository_opensearch_opensearch_referesh_resources_index(opensearch_repository, mocked_portal, get_all_results):
    item1 = mocked_portal.get_item('xyz123', 4)
    item2 = mocked_portal.get_item('xyz345', 4)
    opensearch_repository.bulk_index_items(
        [
            item1,
            item2,
        ]
    )
    results = list(get_all_results(
        opensearch_repository.props.client)['hits']['hits'])
    assert len(results) == 0
    opensearch_repository.refresh_resources_index()
    results = list(get_all_results(
        opensearch_repository.props.client)['hits']['hits'])
    assert len(results) == 2


@pytest.mark.integration
def test_repository_opensearch_opensearch_wait_for_resources_index_to_exist(opensearch_repository):
    opensearch_repository.wait_for_resources_index_to_exist()
    assert True
