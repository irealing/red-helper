from .types import RedMapping, KeyType, _DecoratorFunc, TTL, Encoder, Decoder, json_encoder, json_decoder, _Func
import abc
from .cache import CacheIt, _RmOpFactory
import functools


class _BaseMapping(RedMapping, metaclass=abc.ABCMeta):
    def cache_it(self, key: KeyType, ttl: TTL = None, encoder: Encoder = json_encoder, decoder: Decoder = json_decoder,
                 force: bool = False) -> _DecoratorFunc:
        def _wraps(func: _Func) -> _Func:
            it = CacheIt(self, key, ttl, encoder, decoder, force).mount(func)
            return functools.wraps(func)(it)

        return _wraps

    def remove_it(self, key: KeyType, by_return: bool = False) -> _DecoratorFunc:
        def _wraps(func: _Func) -> _Func:
            return _RmOpFactory.new(self, func, key, by_return)

        return _wraps
