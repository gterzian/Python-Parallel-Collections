import atexit
import multiprocessing

from itertools import chain

from lambdatools import prepare_func
from pool import FPPool


_lock = multiprocessing.Lock()


def init(init_lock):
    global _lock

    _lock = init_lock


def close_pool(pool):
    pool.close()


class _Reducer(object):
    """Helper for the reducer methods"""

    def __init__(self, func, init=None):
        self.func = func
        self.ns = multiprocessing.Manager().Namespace()
        self.ns.result = init

    def __call__(self, item):
        global _lock
        _lock.acquire()
        try:
            aggregate = self.func(self.ns.result, item)
            self.ns.result = aggregate
        finally:
            _lock.release()

    @property
    def result(self):
        return self.ns.result


class ParallelGen(object):

    def __init__(self, data_source, pool_size=None, pool=None):
        self.data = data_source
        self.pool = pool or FPPool(
            processes=pool_size,
            initializer=init,
            initargs=(_lock,),
        )

        atexit.register(close_pool, pool=self.pool)

    def __iter__(self):
        for item in self.data:
            yield item

    def foreach(self, func):
        func = prepare_func(func)
        self.data = self.pool.map(func, self)
        return None

    def filter(self, func):
        func = prepare_func(func)
        return self.__class__(self.pool.filter(func, self), pool=self.pool)

    def map(self, func):
        func = prepare_func(func)
        return self.__class__(self.pool.map(func, self), pool=self.pool)

    def flatmap(self, func):
        func = prepare_func(func)
        return self.__class__(chain(*self.pool.map(func, self)), pool=self.pool)

    def reduce(self, func, init=None):
        func = prepare_func(func)
        _reducer = _Reducer(func, init)
        for i in self.pool.map(_reducer, self):
            # need to consume the generator returned by self.pool.map
            pass
        return _reducer.result


def parallel(data_source, pool_size=None):
    """factory function that returns ParallelGen objects,
    pass an iterable as data_source"""
    if data_source.__class__.__name__ == 'function':
        if data_source().__class__.__name__ == 'generator':
            return ParallelGen(data_source(), pool_size=pool_size)
    else:
        try:
            iter(data_source)
        except TypeError:
            raise TypeError("""supplied data source must be a generator,
               a generator function or an iterable,
               not %s""" % data_source.__class__.__name__)
        return ParallelGen(data_source, pool_size=pool_size)
