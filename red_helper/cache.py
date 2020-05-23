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


class CacheIt:
    def __init__(self, redis: StrictRedis, key: KeyType, ttl: TTL = None, encoder: Encoder = json_encoder,
                 decoder: Decoder = json_decoder, force: bool = False, prefix: AnyStr = ''):
        self._redis = redis
        self._key = (lambda *args, **kwargs: key) if isinstance(key, (str, bytes)) else key
        self._ttl = ttl
        self._encoder = encoder
        self._decoder = decoder
        self._force = force
        self._method = None
        self.prefix = prefix

    def mount(self, method: Callable) -> 'CacheIt':
        self._method = method
        return self

    async def __call__(self, *args, **kwargs):
        key = self._key(*args, **kwargs)
        if self.prefix:
            key = ''.join((self.prefix, key))
        cache = None
        if not self._force and (cache := await self._redis.get(key)):
            return self._decoder(cache)
        ret = self._method(*args, **kwargs)
        if isinstance(ret, Awaitable) and inspect.isawaitable(ret):
            ret = await ret
        cache = self._encoder(ret)
        await self._redis.set(key, cache, ex=self._ttl)
        return ret
