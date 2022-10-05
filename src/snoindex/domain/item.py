from dataclasses import dataclass

from typing import Any
from typing import Dict


@dataclass
class Item:
    data: Dict[str, Any]
    version: int
    uuid: str
    index: str
