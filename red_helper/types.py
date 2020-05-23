import abc
from aredis import StrictRedis
from typing import AnyStr, AsyncGenerator, Tuple


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

    @abc.abstractmethod
    async def clear(self) -> int:
        pass


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
    async def set(self, key: AnyStr, value: AnyStr):
        pass

    @abc.abstractmethod
    async def get(self, key: AnyStr, default_value: AnyStr = None) -> bytes:
        pass
