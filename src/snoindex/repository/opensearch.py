from dataclasses import dataclass

from opensearchpy import OpenSearch

from snoindex.domain.item import Item

from typing import Any
from typing import Dict
from typing import List
from typing import Optional


def get_related_uuids_query(updated, renamed) -> Dict[str, Any]:
    query = {
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


def get_search(client, index):
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
        self._search = None

    def get_related_uuids_from_updated_and_renamed(self, updated, renamed):
        self.refresh_resources_index()
        query = get_related_uuids_query(updated, renamed)
        search = get_search()
        search = search.update_from_dict(
            query
        ).params(
            request_timeout=60,
        )
        for hit in search.scan():
            yield hit.meta.id

    def index_item(self, item: Item) -> None:
        self.props.opensearch_client.index(
            index=item.index,
            body=item.data,
            id=item.uuid,
            request_timeout=30,
            version=item.version,
            version_type='external_gte',
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
                if self.props.client.indices.exists(self.props.resources_index):
                    logging.warning(f'Found resources index, attempt {attempt}')
                    break
            time.sleep(attempt * 5)
