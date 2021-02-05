"""The main module. Use as 'from parallel import parallel'."""

import multiprocessing
from collections import namedtuple
from concurrent import futures
from itertools import chain

from lambdatools import prepare_func


Pool = futures.ProcessPoolExecutor()

'''Helper for the filter methods.
This is a container of the result of some_evaluation(arg) and arg itself.'''
EvalResult = namedtuple('EvalResult', ['bool', 'item'])


def _map(fn, *iterables):
    """Using our own internal map function.

    This is to avoid evaluation of the generator as done
    in futures.ProcessPoolExecutor().map.
    """
    fs = (Pool.submit(fn, *args) for args in zip(*iterables))
    for future in fs:
        yield future.result()


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

    """Helper for the reducer methods."""

    def __init__(self, func, init=None):
        self.func = func
        self.list = multiprocessing.Manager().list([init])

    def __call__(self, item):
        aggregate = self.func(self.list[0], item)
        self.list[0] = aggregate

    @property
    def result(self):
        return self.list[0]


class ParallelGen(object):

    def __init__(self, data_source):
        self.data = data_source

    def __iter__(self):
        for item in self.data:
            yield item

    def foreach(self, func):
        func = prepare_func(func)
        self.data = [i for i in _map(func, self)]
        return None

    def filter(self, func):
        func = prepare_func(func)
        _filter = _Filter(func)
        return self.__class__((i.item for i in _map(_filter, self, ) if i.bool))

    def map(self, func):
        func = prepare_func(func)
        return self.__class__(_map(func, self, ))

    def flatmap(self, func):
        func = prepare_func(func)
        return self.__class__(chain(*_map(func, self)))

    def reduce(self, func, init=None):
        func = prepare_func(func)
        _reducer = _Reducer(func, init)
        for i in _map(_reducer, self, ):
            # need to consume the generator returned by _map
            pass
        return _reducer.result


def parallel(data_source):
    """factory function that returns ParallelGen objects,
    pass an iterable as data_source"""
    if data_source.__class__.__name__ == 'function':
        if data_source().__class__.__name__ == 'generator':
            return ParallelGen(data_source())
    else:
        try:
            iter(data_source)
        except TypeError:
            raise TypeError("""supplied data source must be a generator,
               a generator function or an iterable,
               not %s""" % data_source.__class__.__name__)
        return ParallelGen(data_source)
