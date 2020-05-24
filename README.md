# red-helper

Redis缓存工具[red-cache](https://github.com/irealing/red_cache) 的 asyncio 版本.

## 安装

```shell script
$ pip install red-helper
```

## 示例

### 初始化

```python
import red_helper

red_helper.new("redis://redis:6379",db=0)

```
或:

```python
from red_helper import RedHelper
from aredis import StrictRedis

redis=StrictRedis(**{})
helper=RedHelper(redis)
```

### 一般操作

```python
import red_helper
import asyncio

helper = red_helper.new("redis://redis", db=0)


async def simple_operations():
    # 设置
    await helper.set("hello", "world", ex=180)
    # 查询字段
    print(await helper.get("hello", default_value="WORLD!"))
    # 删除字段
    await helper.delete("hello")


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(simple_operations())

```
### Hash

```python
import asyncio

import red_helper

helper = red_helper.new("redis://redis", db=0)
hs = helper.red_hash("red::hash")


async def simple_operations():
    # 设置
    await hs.set("hello", "world", ex=180)
    # 查询字段
    print(await hs.get("hello", default_value="WORLD!"))
    # 删除字段
    await hs.delete("hello")


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(simple_operations())

```

###缓存

```python
import asyncio

import red_helper

helper = red_helper.new("redis://redis", db=0)


# 缓存函数返回值
@helper.cache_it(lambda asset_id: "asset::cache:key:{}".format(asset_id), ttl=180)
async def read_data(asset_id: int) -> dict:
    await asyncio.sleep(0.1)
    return dict(zip(range(10), range(10)))


# 删除缓存
@helper.remove_it(lambda asset_id: "asset::cache:key:{}".format(asset_id), by_return=True)
async def update_date(asset_id: int) -> int:
    await asyncio.sleep(0.1)
    return asset_id


async def main():
    await read_data(10)
    await update_date(10)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
```

#### 基于HASH的缓存

```python
import asyncio

import red_helper

helper = red_helper.new("redis://redis", db=0)
hs = helper.red_hash("red::hash")


# 缓存函数返回值
@hs.cache_it(lambda asset_id: "asset::cache:key:{}".format(asset_id), ttl=180)
async def read_data(asset_id: int) -> dict:
    await asyncio.sleep(0.1)
    return dict(zip(range(10), range(10)))


# 删除缓存
@hs.remove_it(lambda asset_id: "asset::cache:key:{}".format(asset_id), by_return=True)
async def update_date(asset_id: int) -> int:
    await asyncio.sleep(0.1)
    return asset_id


async def main():
    await read_data(10)
    await update_date(10)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())

```

author:[Memory_Leak](mailto:irealing@163.com)

