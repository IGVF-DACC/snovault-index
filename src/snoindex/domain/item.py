from dataclasses import dataclass

from typing import Any
from typing import Dict


@dataclass
class Item:
    data: Dict[str, Any]
    version: int
    uuid: str
    index: str

    def as_bulk_action(self) -> Dict[str, Any]:
        return {
            '_index': self.index,
            '_id': self.uuid,
            '_version': self.version,
            '_version_type': 'external_gte',
            **self.data,
        }
