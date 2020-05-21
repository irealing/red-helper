from aredis import StrictRedis
from typing import AnyStr, AsyncGenerator, Tuple


class RedHelper:
    def __init__(self, redis: StrictRedis, prefix: str = ''):
        self.redis = redis
        self.prefix = prefix

    def red_hash(self, resource: str):
        return RedHash(self.redis, resource if not self.prefix else "{}{}".format(self.prefix, resource))


class RedHash:
    def __init__(self, redis: StrictRedis, resource: str):
        self._redis = redis
        self._resource = resource
        self._auto_remove = self._auto_remove

    async def remove(self):
        self._redis.delete(self._resource)

    def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.remove()
        if exc_val:
            raise exc_val

    async def get(self, key: AnyStr, default_value: AnyStr = None) -> AnyStr:
        ret = await self._redis.hget(self._resource, key)
        return default_value if ret is None else ret

    async def __aiter__(self) -> AsyncGenerator[Tuple[bytes, bytes], None, None]:
        cursor = 0
        while True:
            await self._redis.hscan()
