from typing import Optional

from ..config import CONF_NAME
from ..index import IndexCollection
from ..utils import ensure_index_dir

BUF_SIZE = 2 * 1024 * 1024


def run_reindex(index_name: Optional[str] = None) -> None:
    ensure_index_dir()
    indexes = IndexCollection.from_yaml(CONF_NAME)
    for index in indexes.values():
        if index_name and index_name != index.name:
            continue
        index.reindex()
