"""The main module. Use as 'from parallel import parallel'."""

import atexit
import multiprocessing

from collections import namedtuple
from itertools import chain

from lambdatools import prepare_func


_lock = multiprocessing.Lock()


def init(init_lock):
    global _lock

    _lock = init_lock


def close_pool(pool):
    pool.close()


'''Helper for the filter methods.
This is a container of the result of some_evaluation(arg) and arg itself.'''
EvalResult = namedtuple('EvalResult', ['bool', 'item'])


def _map(fn, iterable, pool):
    """Using our own internal map function.

    This is to avoid evaluation of the generator as done
    in futures.ProcessPoolExecutor().map.
    """
    return pool.map(fn, iterable)


class _Filter(object):
    """Helper for the filter methods.

    We need to use a class as closures cannot be
    pickled and sent to other processes.
    """

    def __init__(self, predicate):
        if predicate is None:
            self.predicate = bool
        else:
            self.predicate = predicate

    def __call__(self, item):
        if self.predicate(item):
            return EvalResult(bool=True, item=item)
        else:
            return EvalResult(bool=False, item=None)


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
        self.pool = pool or multiprocessing.Pool(
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
        self.data = [i for i in _map(func, self, self.pool)]
        return None

    def filter(self, func):
        func = prepare_func(func)
        _filter = _Filter(func)
        return self.__class__((i.item for i in _map(_filter, self, self.pool) if i.bool), pool=self.pool)

    def map(self, func):
        func = prepare_func(func)
        return self.__class__(_map(func, self, self.pool), pool=self.pool)

    def flatmap(self, func):
        func = prepare_func(func)
        return self.__class__(chain(*_map(func, self, self.pool)), pool=self.pool)

    def reduce(self, func, init=None):
        func = prepare_func(func)
        _reducer = _Reducer(func, init)
        for i in _map(_reducer, self, self.pool):
            # need to consume the generator returned by _map
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
