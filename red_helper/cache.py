import functools
import inspect
from typing import Callable, Awaitable

from ._exc import UnsupportedOperation
from .types import RedMapping, TTL, KeyType, Encoder, Decoder, json_decoder, json_encoder


class CacheOpt:
    def __init__(self, mapping: RedMapping, key: KeyType):
        self._mapping = mapping
        self.key = (lambda *args, **kwargs: key) if isinstance(key, (str, bytes)) else key
        self._method = None

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return functools.partial(self.__call__, instance)

    @property
    def mapping(self) -> RedMapping:
        return self._mapping

    @property
    def method(self):
        return self._method

    def mount(self, method: Callable) -> 'CacheOpt':
        self._method = method
        return self

    async def __call__(self, *args, **kwargs):
        pass


class CacheIt(CacheOpt):

    def __init__(self, mapping: RedMapping, key: KeyType, ttl: TTL = None, encoder: Encoder = json_encoder,
                 decoder: Decoder = json_decoder, force: bool = False):
        super().__init__(mapping, key)
        self._ttl = ttl
        self._encoder = encoder
        self._decoder = decoder
        self._force = force

    async def __call__(self, *args, **kwargs):
        key = self.key(*args, **kwargs)
        cache = None
        if not self._force and (cache := await self.mapping.get(key)):
            return self._decoder(cache)
        ret = self._method(*args, **kwargs)
        if isinstance(ret, Awaitable) and inspect.isawaitable(ret):
            ret = await ret
        cache = self._encoder(ret)
        await self.mapping.set(key, cache, ex=self._ttl)
        return ret


class RemoveIt(CacheOpt):

    def __init__(self, mapping: RedMapping, key: KeyType, by_return: bool = False):
        super().__init__(mapping, key)
        self._by_return = by_return

    async def __call__(self, *args, **kwargs):
        ret = self.method(*args, **kwargs)
        if isinstance(ret, Awaitable) and inspect.isawaitable(ret):
            ret = await ret
        k = self.key(*args, **kwargs) if not self._by_return else self.key(ret)
        await self.mapping.delete(k)
        return ret


class GenRemoveIt(RemoveIt):

    async def __call__(self, *args, **kwargs):
        if inspect.isasyncgenfunction(self.method):
            async for item in self.method(*args, **kwargs):
                yield item
        else:
            for item in self.method(*args, **kwargs):
                yield item
        await self.mapping.delete(self.key(*args, **kwargs))


class _RmOpFactory:
    @staticmethod
    def new(mapping: RedMapping, func: Callable, key: KeyType, by_return: bool = False) -> 'RemoveIt':
        if inspect.isasyncgenfunction(func) or inspect.isgeneratorfunction(func):
            if by_return:
                raise UnsupportedOperation()
            wrapper = GenRemoveIt
        else:
            wrapper = RemoveIt
        return functools.wraps(func)(wrapper(mapping, key, by_return).mount(func))
