import inspect
import json
import pickle
from datetime import timedelta
from typing import Callable, AnyStr, Union, Any, Awaitable

from aredis import StrictRedis

KeyType = Union[AnyStr, Callable[[], AnyStr]]
Encoder = Callable[[Any], AnyStr]
TTL = Union[int, timedelta]
Decoder = Callable[[bytes], Any]


def json_encoder(o: Any) -> AnyStr:
    return json.dumps(o)


def pickle_encoder(o: Any) -> AnyStr:
    return pickle.dumps(o)


def json_decoder(data: bytes) -> Any:
    return json.loads(data)


def pickle_decoder(data: bytes) -> Any:
    return pickle.loads(data)


class CacheOpt:
    def __init__(self, redis: StrictRedis, key: KeyType):
        self._redis = redis
        self.key = (lambda *args, **kwargs: key) if isinstance(key, (str, bytes)) else key
        self._method = None

    @property
    def redis(self) -> StrictRedis:
        return self._redis

    @property
    def method(self):
        return self._method

    def mount(self, method: Callable) -> 'CacheOpt':
        self._method = method
        return self


class CacheIt(CacheOpt):

    def __init__(self, redis: StrictRedis, key: KeyType, ttl: TTL = None, encoder: Encoder = json_encoder,
                 decoder: Decoder = json_decoder, force: bool = False):
        super().__init__(redis, key)

        self._ttl = ttl
        self._encoder = encoder
        self._decoder = decoder
        self._force = force

    async def __call__(self, *args, **kwargs):
        key = self.key(*args, **kwargs)
        cache = None
        if not self._force and (cache := await self._redis.get(key)):
            return self._decoder(cache)
        ret = self._method(*args, **kwargs)
        if isinstance(ret, Awaitable) and inspect.isawaitable(ret):
            ret = await ret
        cache = self._encoder(ret)
        await self._redis.set(key, cache, ex=self._ttl)
        return ret


class RemoveIt(CacheOpt):
    def __init__(self, redis: StrictRedis, key: KeyType, by_return: bool = False):
        super().__init__(redis, key)
        self._by_return = by_return
        self.__call__ = None

    def mount(self, method: Callable) -> 'RemoveIt':
        if self._by_return and (inspect.isgeneratorfunction(method) or inspect.isasyncgenfunction(method)):
            raise RuntimeError()
        super().mount(method)
        self.__call__ = self._select()
        return self

    def _select(self):
        if inspect.isasyncgenfunction(self.method):
            return self.remove_gen
        if inspect.isgeneratorfunction(self.method):
            return self.remove_gen
        return self.remove

    async def remove(self, *args, **kwargs):
        ret = self.method(*args, **kwargs)
        if isinstance(ret, Awaitable) and inspect.isawaitable(ret):
            ret = await ret
        k = self.key(*args, **kwargs) if not self._by_return else self.key(ret)
        await self.redis.delete(k)
        return ret

    async def remove_gen(self, *args, **kwargs):
        if inspect.isasyncgenfunction(self.method):
            async for item in self.method(*args, **kwargs):
                yield item
            else:
                for item in self.method(*args, **kwargs):
                    yield item
        await self.redis.delete(self.method)
