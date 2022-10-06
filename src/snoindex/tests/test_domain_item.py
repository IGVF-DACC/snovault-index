import pytest


def test_domain_item_item_init():
    from snoindex.domain.item import Item
    item = Item(
        data={'some': 'data'},
        version=123,
        uuid='xyz123',
        index='item-index-123'
    )
    assert isinstance(item, Item)
    assert item.data == {'some': 'data'}
    assert item.version == 123
    assert item.uuid == 'xyz123'
    assert item.index == 'item-index-123'


def test_domain_item_item_as_bulk_action():
    from snoindex.domain.item import Item
    item = Item(
        data={'some': 'data'},
        version=123,
        uuid='xyz123',
        index='item-index-123'
    )
    expected = {
        '_index': 'item-index-123',
        '_id': 'xyz123',
        '_version': 123,
        '_version_type':
        'external_gte',
        'some': 'data'
    }
    actual = item.as_bulk_action()
    assert actual == expected
