import logging
import time
import requests

from requests import Response

from dataclasses import dataclass

from snoindex.domain.item import Item

from typing import Any
from typing import Dict
from typing import Tuple


INDEX_DATA_VIEW = '@@index-data-external'


def make_remote_request(url: str) -> Response:
    return requests.get(url)


def make_authorized_remote_request(url: str, auth: Tuple[str, str]) -> Response:
    return requests.get(
        url,
        auth=auth,
    )


@dataclass
class PortalProps:
    backend_url: str
    auth: Tuple[str, str]
    index_data_view: str = INDEX_DATA_VIEW


class Portal:

    def __init__(self, props: PortalProps) -> None:
        self.props = props

    def _make_index_data_view_url_from_uuid(self, uuid: str) -> str:
        return (
            f'{self.props.backend_url}/{uuid}/{self.props.index_data_view}'
            '/?datastore=database'
        )

    def get_raw_item_by_uuid(self, uuid: str) -> Any:
        url = self._make_index_data_view_url_from_uuid(uuid)
        return make_authorized_remote_request(
            url,
            self.props.auth,
        ).json()

    def get_item(self, uuid: str) -> Item:
        raw_item = self.get_raw_item_by_uuid(uuid)
        return Item(
            data=raw_item,
            version=int(raw_item['xmin']),
            uuid=uuid,
            index=raw_item['index_name'],
        )

    def wait_for_portal_connection(self) -> None:
        logging.warning('Waiting for portal connection')
        url = self.props.backend_url
        attempt = 0
        while True:
            attempt += 1
            try:
                make_remote_request(url)
                logging.warning(f'Portal connected, attempt {attempt}')
                break
            except requests.exceptions.ConnectionError:
                time.sleep(attempt * 5)
                continue

    def _make_access_key_url(self) -> str:
        return f'{self.props.backend_url}/access-keys/?datastore=database'

    def wait_for_access_key_to_exist(self) -> None:
        logging.warning('Checking for access key')
        url = self._make_access_key_url()
        attempt = 0
        while True:
            attempt += 1
            response = make_authorized_remote_request(
                url,
                self.props.auth,
            )
            if response.status_code == 200:
                logging.warning(f'Found access key, attempt {attempt}')
                break
            time.sleep(attempt * 3)
