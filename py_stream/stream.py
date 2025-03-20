from __future__ import annotations
from abc import abstractmethod
from typing import Any, Callable, Generator, Generic, TypeVar

_T: TypeVar = TypeVar('_T')
_R: TypeVar = TypeVar('_R')


def _yield_stream(stream_2: Stream[_T] | Generator[_T, Any, Any]) -> Generator[_T, Any, Any]:
    if isinstance(stream_2, Generator):
        return stream_2
    return stream_2.get_stream()

def _is_list_contains_value(the_list: list[_T], value: _T, comparator: Callable[[_T, _T], bool]) -> bool:
    if comparator is None:
        return value in the_list

    for l in the_list:
        if comparator(l, value):
            return True
    return False


class Stream(Generic[_T]):
    """ Stream Wrapper for generators.

    Similar to java's Stream<T> """

    @staticmethod
    def empty() -> Stream[_T]:
        """Similar to java's Stream.empty()
        :returns: empty Stream"""

        def _empty_generator() -> Generator[_T, Any, Any]:
            yield from ()
        return _DefaultStream(_empty_generator())

    @staticmethod
    def stream(stream: Generator[_T, Any, Any]) -> Stream[_T]:
        """create Stream[T] from a given Generator[T]"""

        return _DefaultStream(stream)

    @staticmethod
    def concat(*streams:  Stream[_T] | Generator[_T, Any, Any]) -> Stream[_T]:
        """Similar to java's Stream.concat()
        :returns: A new Stream containing entries from all given Streams/generators"""

        def _new_generator() -> Generator[_T, Any, Any]:
            for stream in streams:
                for t in _yield_stream(stream):
                    yield t

        return _DefaultStream(_new_generator())

    @abstractmethod
    def get_stream(self) -> Generator[_T, Any, Any]:
        """:returns: The wrapped generator"""
        pass

    @abstractmethod
    def map(self, mapper: Callable[[_T], _R]) -> Stream[_R]:
        """Similar to java's Stream::map()
        :returns: A new Stream containing mapped entries"""
        pass

    @abstractmethod
    def filter(self, filtration: Callable[[_T], bool]) -> Stream[_T]:
        """Similar to java's Stream::filter()
        :returns: A new Stream containing only entries which satisfies the given filtration"""
        pass

    @abstractmethod
    def not_null(self) -> Stream[_T]:
        """remove any None entries from the Stream"""
        pass

    @abstractmethod
    def distinct(self, comparator: Callable[[_T, _T], bool] = None) -> Stream[_T]:
        """Similar to java's Stream::distinct(), but using the given comparator.

        In case no comparator is provided, default comparison methods may be used.
        :returns: A new Stream containing only distinct entries"""
        pass

    @abstractmethod
    def peek(self, action: Callable[[_T], None]) -> Stream[_T]:
        """Similar to java's Stream::peek()"""
        pass

    @abstractmethod
    def skip(self, skip_index: int) -> Stream[_T]:
        """Similar to java's Stream::skip()"""
        pass

    @abstractmethod
    def limit(self, max_size: int) -> Stream[_T]:
        """Similar to java's Stream::limit()"""
        pass

    @abstractmethod
    def any_match(self, predicate: Callable[[_T], bool]) -> bool:
        """Similar to java's Stream::anyMatch().

        WARNING: The wrapped generator shall be consumed."""
        pass

    @abstractmethod
    def all_match(self, predicate: Callable[[_T], bool]) -> bool:
        """Similar to java's Stream::allMatch().

        WARNING: The wrapped generator shall be consumed."""
        pass

    @abstractmethod
    def none_match(self, predicate: Callable[[_T], bool]) -> bool:
        """Similar to java's Stream::noneMatch().

        WARNING: The wrapped generator shall be consumed."""
        pass

    @abstractmethod
    def count(self) -> int:
        """Similar to java's Stream::count().

        WARNING: The wrapped generator shall be consumed."""
        pass


class _DefaultStream(Stream[_T]):
    _stream: Generator[_T, Any, Any] = None

    def __init__(self, stream: Generator[_T, Any, Any]) -> None:
        self._stream = stream

    def get_stream(self) -> Generator[_T, Any, Any]:
        return self._stream

    def map(self, mapper: Callable[[_T], _R]) -> Stream[_R]:
        def _new_generator() -> Generator[_R, Any, Any]:
            for t in self._stream:
                yield mapper(t)

        return _DefaultStream(_new_generator())

    def filter(self, filtration: Callable[[_T], bool]) -> Stream[_T]:
        def _new_generator() -> Generator[_T, Any, Any]:
            for t in self._stream:
                if filtration(t):
                    yield t

        return _DefaultStream(_new_generator())

    def not_null(self) -> Stream[_T]:
        def _new_generator() -> Generator[_T, Any, Any]:
            for t in self._stream:
                if t is not None:
                    yield t

        return _DefaultStream(_new_generator())

    def distinct(self, comparator: Callable[[_T, _T], bool] = None) -> Stream[_T]:
        def _new_generator() -> Generator[_T, Any, Any]:
            _previous_values: list[_T] = list()

            for t in self._stream:
                if _is_list_contains_value(_previous_values, t, comparator):
                    continue
                yield t
                _previous_values.append(t)

        return _DefaultStream(_new_generator())

    def peek(self, action: Callable[[_T], None]) -> Stream[_T]:
        def _new_generator() -> Generator[_T, Any, Any]:
            for t in self._stream:
                action(t)
                yield t

        return _DefaultStream(_new_generator())

    def skip(self, skip_index: int = 0) -> Stream[_T]:
        def _new_generator() -> Generator[_T, Any, Any]:
            _idx: int = skip_index

            for t in self._stream:
                if _idx > 0:
                    _idx -= 1
                    continue
                yield t

        return _DefaultStream(_new_generator())

    def limit(self, max_size: int) -> Stream[_T]:
        def _new_generator() -> Generator[_T, Any, Any]:
            _idx: int = max_size

            for t in self._stream:
                if _idx <= 0:
                    _idx -= 1
                    break
                yield t

        return _DefaultStream(_new_generator())

    def any_match(self, predicate: Callable[[_T], bool]) -> bool:
        for t in self._stream:
            if predicate(t):
                return True
        return False

    def all_match(self, predicate: Callable[[_T], bool]) -> bool:
        for t in self._stream:
            if not predicate(t):
                return False
        return True

    def none_match(self, predicate: Callable[[_T], bool]) -> bool:
        return self.all_match(lambda t: not predicate(t))

    def count(self) -> int:
        return len(list(self._stream))
