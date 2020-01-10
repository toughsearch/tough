from typing import Any, Iterator, TypeVar

T = TypeVar('T')


def tqdm(iterator: Iterator[T], *args: Any, **kwargs: Any) -> Iterator[T]: ...
