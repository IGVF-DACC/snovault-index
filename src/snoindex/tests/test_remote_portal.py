import pytest


def test_remote_portal_portal_init(portal_props):
    from snoindex.remote.portal import Portal
    portal = Portal(
        props=portal_props
    )
    assert isinstance(portal, Portal)


def test_remote_portal_portal_make_index_data_view_url_from_uuid(portal_props):
    from snoindex.remote.portal import Portal
    portal = Portal(
        props=portal_props
    )
    expected = 'testing.domain/abc123/@@index-data-external/?datastore=database'
    actual = portal._make_index_data_view_url_from_uuid('abc123')
    assert actual == expected


def test_remote_portal_portal_get_raw_item_by_uuid(portal_props, raw_index_data_view, mocker):
    from snoindex.remote.portal import Portal
    return_data = mocker.Mock()
    return_data.json = lambda: raw_index_data_view
    mocker.patch(
        'snoindex.remote.portal.make_authorized_remote_request',
        return_value=return_data
    )
    portal = Portal(
        props=portal_props
    )
    actual = portal.get_raw_item_by_uuid('abc123')
    assert actual == raw_index_data_view


def test_remote_portal_portal_get_item(portal_props, raw_index_data_view, mocker):
    from snoindex.remote.portal import Portal
    from snoindex.domain.item import Item
    return_data = mocker.Mock()
    return_data.json = lambda: raw_index_data_view
    mocker.patch(
        'snoindex.remote.portal.make_authorized_remote_request',
        return_value=return_data
    )
    portal = Portal(
        props=portal_props
    )
    item = portal.get_item('abc123', 4)
    assert isinstance(item, Item)
    assert item.version == 4
    assert item.uuid == 'abc123'
    assert item.index == 'snowball'
    assert item.data == raw_index_data_view


def raise_until_two(*args, **kwargs):
    from requests.exceptions import ConnectionError
    raise_until_two.called += 1
    if raise_until_two.called < 2:
        raise ConnectionError


def test_remote_portal_wait_for_portal_connection(portal_props, mocker):
    from snoindex.remote.portal import Portal
    from snoindex.domain.item import Item
    raise_until_two.called = 0
    mocker.patch(
        'snoindex.remote.portal.make_remote_request',
        side_effect=raise_until_two
    )
    from snoindex.remote.portal import Portal
    portal = Portal(
        props=portal_props
    )
    portal.wait_for_portal_connection()


def test_remote_portal_portal_make_access_key_url(portal_props):
    from snoindex.remote.portal import Portal
    portal = Portal(
        props=portal_props
    )
    actual = portal._make_access_key_url()
    expected = 'testing.domain/access-keys/?datastore=database'
    assert actual == expected


def return_200_on_two(*args, **kwargs):
    return_200_on_two.called += 1
    from requests import Response
    response = Response()
    if return_200_on_two.called < 2:
        response.status_code = 400
        return response
    response.status_code = 200
    return response


def test_remote_portal_portal_wait_for_access_key_to_exist(portal_props, mocker):
    return_200_on_two.called = 0
    from snoindex.remote.portal import Portal
    mocker.patch(
        'snoindex.remote.portal.make_authorized_remote_request',
        side_effect=return_200_on_two,
    )
    portal = Portal(
        props=portal_props
    )
    portal.wait_for_access_key_to_exist()
