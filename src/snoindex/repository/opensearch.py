import logging
import time

from dataclasses import dataclass

from opensearchpy import OpenSearch

from opensearchpy import helpers

from opensearchpy.exceptions import ConflictError

from opensearch_dsl import Search

from snoindex.domain.item import Item

from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional


def get_related_uuids_query(updated: List[str], renamed: List[str]) -> Dict[str, Any]:
    return {
        'query': {
            'bool': {
                'should': [
                    {
                        'terms': {
                            'embedded_uuids': updated,
                        },
                    },
                    {
                        'terms': {
                            'linked_uuids': renamed,
                        },
                    },
                ],
            },
        },
        '_source': False,
    }


def get_search(client: OpenSearch, index: Optional[str]) -> Search:
    return Search(
        using=client,
        index=index,
    )


@dataclass
class OpensearchProps:
    client: OpenSearch
    resources_index: Optional[str] = None


class Opensearch:

    def __init__(self, props: OpensearchProps):
        self.props = props

    def get_related_uuids_from_updated_and_renamed(
            self,
            updated: List[str],
            renamed: List[str],
    ) -> Iterator[str]:
        self.refresh_resources_index()
        query = get_related_uuids_query(updated, renamed)
        search = get_search(
            self.props.client,
            self.props.resources_index,
        )
        search = search.update_from_dict(
            query
        ).params(
            request_timeout=60,
        )
        for hit in search.scan():
            yield hit.meta.id

    def _index_item(self, item: Item) -> None:
        self.props.client.index(
            index=item.index,
            body=item.data,
            id=item.uuid,
            request_timeout=30,
            version=item.version,
            version_type='external_gte',
        )

    def index_item(self, item: Item) -> None:
        try:
            self._index_item(item)
        except ConflictError as e:
            logging.warning(f'Skipping: {e}')

    def bulk_index_items(self, items: List[Item]) -> None:
        helpers.bulk(
            self.props.client,
            (
                item.as_bulk_action()
                for item in items
            ),
        )

    def refresh_resources_index(self) -> None:
        if self.props.resources_index is not None:
            self.props.client.indices.refresh(
                self.props.resources_index
            )

    def wait_for_resources_index_to_exist(self) -> None:
        if self.props.resources_index is not None:
            logging.warning('Waiting for resources index to exist')
            attempt = 0
            while True:
                attempt += 1
                try:
                    if self.props.client.indices.exists(self.props.resources_index):
                        logging.warning(
                            f'Found resources index, attempt {attempt}'
                        )
                        break
                except Exception as e:
                    logging.warning(e)
                    time.sleep(attempt * 5)

    def clear(self) -> None:
        for index in self.props.client.indices.get('*'):
            self.props.client.indices.delete(index)
