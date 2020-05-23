import functools
from typing import AnyStr, AsyncGenerator, Tuple

from aredis import StrictRedis

from .cache import CacheIt, KeyType, TTL, Encoder, Decoder, json_decoder, json_encoder, pickle_decoder, pickle_encoder


class DangerousOperation(Exception):
    pass


class RedHelper:
    def __init__(self, redis: StrictRedis, prefix: str = ''):
        self.redis = redis
        self.prefix = prefix

    def red_hash(self, resource: str):
        return RedHash(self.redis, resource if not self.prefix else "{}{}".format(self.prefix, resource))

    async def delete(self, key: AnyStr, *name: AnyStr) -> int:
        return await self.redis.delete(key, *name)

    async def find(self, match: AnyStr = None) -> AsyncGenerator[Tuple[str, str], None]:
        cursor = 0
        while True:
            cursor, rows = await self.redis.scan(cursor=cursor, match=match)
            for row in rows.items():
                yield row
            if not cursor:
                break

    async def set(self, key: AnyStr, value: AnyStr) -> int:
        return await self.redis.set(key, value)

    async def __aiter__(self) -> AsyncGenerator[Tuple[str, str], None]:
        async for row in self.find():
            yield row

    def clear(self):
        raise DangerousOperation('FLUSHDB')

    def cache_it(self, key: KeyType, ttl: TTL = None, encoder: Encoder = json_encoder,
                 decoder: Decoder = json_decoder, force: bool = False):
        def _warps(func):
            it = CacheIt(self.redis, key, ttl, encoder, decoder, force, prefix=self.prefix).mount(func)
            functools.wraps(func)(it)
            return it

        return _warps

    def json_cache(self, key: KeyType, ttl: TTL = None, force: bool = False):
        return self.cache_it(key, ttl, force=force)

    def pickle_cache(self, key: KeyType, ttl: TTL = None, force: bool = False):
        return self.cache_it(key, ttl, encoder=pickle_encoder, decoder=pickle_decoder, force=force)


class RedHash:
    def __init__(self, redis: StrictRedis, resource: str):
        self._redis: StrictRedis = redis
        self._resource = resource

    async def remove(self):
        await self._redis.delete(self._resource)

    async def __aenter__(self) -> 'RedHash':
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.remove()
        if exc_val:
            raise exc_val

    async def has(self, key: AnyStr) -> bool:
        return await self._redis.hexists(key)

    async def set(self, key: AnyStr, value: AnyStr):
        await self._redis.hset(self._resource, key, value)

    async def get(self, key: AnyStr, default_value: AnyStr = None) -> bytes:
        ret = await self._redis.hget(self._resource, key)
        return default_value if ret is None else ret

    async def find(self, match: AnyStr = None) -> AsyncGenerator[Tuple[bytes, bytes], None]:
        cursor = 0
        while True:
            cursor, rows = await self._redis.hscan(self._resource, cursor=cursor, match=match)
            for row in rows.items():
                yield row
            if not cursor:
                break

    async def __aiter__(self):
        async for row in self.find():
            yield row
