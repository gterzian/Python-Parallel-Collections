"""The main module. Use as 'from parallel import parallel'."""

import multiprocessing
from collections import namedtuple
from concurrent import futures
from itertools import chain, izip


Pool = futures.ProcessPoolExecutor()

'''Helper for the filter methods.
This is a container of the result of some_evaluation(arg) and arg itself.'''
EvalResult = namedtuple('EvalResult', ['bool', 'item'])


def _map(fn, *iterables):
    """Using our own internal map function.

    This is to avoid evaluation of the generator as done
    in futures.ProcessPoolExecutor().map.
    """
    fs = (Pool.submit(fn, *args) for args in izip(*iterables))
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
        self.data = [i for i in _map(func, self)]
        return None

    def filter(self, pred):
        _filter = _Filter(pred)
        return self.__class__((i.item for i in _map(_filter, self, ) if i.bool))

    def map(self, func):
        return self.__class__(_map(func, self, ))

    def flatmap(self, func):
        return self.__class__(chain(*_map(func, self)))

    def reduce(self, function, init=None):
        _reducer = _Reducer(function, init)
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


"""Below is all deprecated stuff, with DeprecationWarning"""

def lazy_parallel(data_source):
    raise DeprecationWarning("""lazy_parallel has been deprecated,
    please use "parallel" instead, it has become lazy too :)""")


class ParallelSeq(object):
    def __init__(self, *args, **kwargs):
        raise DeprecationWarning("""{0} has been deprecated, please use
         the "parallel" factory function instead
         """.format(self.__class__.__name__))

class ParallelList(ParallelSeq):
    pass

class ParallelDict(ParallelSeq):
    pass

class ParallelString(ParallelSeq):
    pass
