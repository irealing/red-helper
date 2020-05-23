import functools
from typing import AnyStr, AsyncGenerator, Tuple

from .cache import CacheIt, KeyType, TTL, Encoder, Decoder, json_decoder, json_encoder, pickle_decoder, pickle_encoder
from .types import RedMapping
from aredis import StrictRedis


class RedHelper(RedMapping):

    def __init__(self, redis: StrictRedis):
        super().__init__(redis, "")

    @classmethod
    def new(cls, url: str, db: int, **kwargs) -> 'RedHelper':
        redis = StrictRedis.from_url(url, db, **kwargs)
        return cls(redis)

    def red_hash(self, resource: AnyStr) -> 'RedHash':
        return RedHash(self.redis, resource)

    async def delete(self, key: AnyStr, *name: AnyStr) -> int:
        return await self.redis.delete(key, *name)

    async def find(self, match: AnyStr = None) -> AsyncGenerator[bytes, None]:
        cursor = 0
        while True:
            cursor, rows = await self.redis.scan(cursor=cursor, match=match)
            for row in rows:
                yield row
            if not cursor:
                break

    async def set(self, key: AnyStr, value: AnyStr) -> int:
        return await self.redis.set(key, value)

    async def __aiter__(self) -> AsyncGenerator[bytes, None]:
        async for row in self.find():
            yield row

    async def has(self, key: AnyStr) -> bool:
        return await self.redis.exists(key)

    async def get(self, key: AnyStr, default_value: AnyStr = None) -> bytes:
        return ret if (ret := await self.redis.get(key)) is not None else default_value

    async def clear(self):
        await self.redis.flushdb()

    def cache_it(self, key: KeyType, ttl: TTL = None, encoder: Encoder = json_encoder,
                 decoder: Decoder = json_decoder, force: bool = False):
        def _warps(func):
            it = CacheIt(self.redis, key, ttl, encoder, decoder, force).mount(func)
            functools.wraps(func)(it)
            return it

        return _warps

    async def size(self) -> int:
        return await self.redis.dbsize()

    def json_cache(self, key: KeyType, ttl: TTL = None, force: bool = False):
        return self.cache_it(key, ttl, force=force)

    def pickle_cache(self, key: KeyType, ttl: TTL = None, force: bool = False):
        return self.cache_it(key, ttl, encoder=pickle_encoder, decoder=pickle_decoder, force=force)


class RedHash(RedMapping):

    async def remove(self):
        await self.redis.delete(self._resource)

    async def __aenter__(self) -> 'RedHash':
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.remove()
        if exc_val:
            raise exc_val

    async def has(self, key: AnyStr) -> bool:
        return await self.redis.hexists(key)

    async def set(self, key: AnyStr, value: AnyStr):
        await self.redis.hset(self._resource, key, value)

    async def get(self, key: AnyStr, default_value: AnyStr = None) -> bytes:
        ret = await self.redis.hget(self._resource, key)
        return default_value if ret is None else ret

    async def find(self, match: AnyStr = None) -> AsyncGenerator[Tuple[bytes, bytes], None]:
        cursor = 0
        while True:
            cursor, rows = await self.redis.hscan(self._resource, cursor=cursor, match=match)
            for row in rows.items():
                yield row
            if not cursor:
                break

    async def __aiter__(self):
        async for row in self.find():
            yield row

    async def size(self) -> int:
        return await self.redis.hlen(self.resource)

    async def clear(self) -> int:
        return await self.redis.delete(self.resource)
