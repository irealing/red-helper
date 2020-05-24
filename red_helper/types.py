import abc
from typing import AnyStr, AsyncGenerator, Tuple, Callable, Union, Generic, TypeVar, Any

from aredis import StrictRedis
from datetime import timedelta
import json
import pickle

_Ret = TypeVar('_Ret')
TTL = Union[int, timedelta]
Encoder = Callable[[Any], AnyStr]
Decoder = Callable[[bytes], Any]
KeyType = Union[AnyStr, Callable[[], AnyStr]]
_DecoratorFunc = Callable[[Callable[[...], _Ret]], Callable[[...], _Ret]]


def json_encoder(o: Any) -> AnyStr:
    return json.dumps(o)


def pickle_encoder(o: Any) -> AnyStr:
    return pickle.dumps(o)


def json_decoder(data: bytes) -> Any:
    return json.loads(data)


def pickle_decoder(data: bytes) -> Any:
    return pickle.loads(data)


class RedObject(metaclass=abc.ABCMeta):
    def __init__(self, redis: StrictRedis, resource: AnyStr):
        self._resource = resource
        self._redis = redis

    @property
    def resource(self):
        return self._resource

    @property
    def redis(self) -> StrictRedis:
        return self._redis

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.clear()
        if exc_val:
            raise exc_val

    @abc.abstractmethod
    async def size(self) -> int:
        pass

    async def clear(self) -> int:
        return self._redis.delete(self._resource)


class RedMapping(RedObject, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def __aiter__(self):
        pass

    @abc.abstractmethod
    async def find(self, match: AnyStr = None) -> AsyncGenerator[Tuple[bytes, bytes], None]:
        pass

    @abc.abstractmethod
    async def has(self, key: AnyStr) -> bool:
        pass

    @abc.abstractmethod
    async def set(self, key: AnyStr, value: AnyStr, ex: TTL = None):
        pass

    @abc.abstractmethod
    async def get(self, key: AnyStr, default_value: AnyStr = None) -> bytes:
        pass

    @abc.abstractmethod
    async def incr(self, key: AnyStr, value: int = 1) -> int:
        pass

    @abc.abstractmethod
    async def delete(self, key: AnyStr, *args: AnyStr) -> int:
        pass

    def counter(self, key: AnyStr) -> 'Counter':
        return Counter(self, key)

    @abc.abstractmethod
    def cache_it(self, key: KeyType, ttl: TTL = None, encoder: Encoder = json_encoder,
                 decoder: Decoder = json_decoder, force: bool = False) -> _DecoratorFunc:
        pass

    @abc.abstractmethod
    def remove_it(self, key: KeyType, by_return: bool = False) -> _DecoratorFunc:
        pass


class RedCollection(RedObject, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def add(self, value: AnyStr, *args: AnyStr) -> int:
        pass

    @abc.abstractmethod
    async def remove(self, value: str) -> int:
        pass

    @abc.abstractmethod
    async def __aiter__(self) -> AsyncGenerator[bytes, None]:
        pass

    async def filter(self, f: Callable[[bytes], bool] = lambda _: True) -> AsyncGenerator[bytes, None]:
        async for item in self:
            if f(item):
                yield item

    async def pop(self) -> bytes:
        pass


class Counter:
    def __init__(self, mapping: RedMapping, resource: AnyStr, step: int = 1):
        self._mapping = mapping
        self._resource = resource
        self._step = step

    async def get(self):
        return await self.incr(self._step)

    async def incr(self, step: int = None) -> int:
        return await self._mapping.incr(self._resource, step)

    async def value(self) -> int:
        return await self._mapping.get(self._resource) or 0

    async def clear(self):
        await self._mapping.delete(self._resource)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.clear()
        if exc_val:
            raise exc_val
