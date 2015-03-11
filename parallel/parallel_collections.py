from concurrent import futures
import multiprocessing
from itertools import chain, imap, izip
from UserList import UserList
from UserDict import UserDict
from UserString import UserString


Pool = futures.ProcessPoolExecutor()


def _map(fn, *iterables):
    '''using our own internal map function to avoid evaluation 
    of the generator as done in futures.ProcessPoolExecutor().map'''
    fs = (Pool.submit(fn, *args) for args in izip(*iterables))
    for future in fs:
        yield future.result()


class _Filter(object):
    '''Helper for the filter methods, 
    need to use class as closures cannot be 
    pickled and sent to other processes'''
    
    def __init__(self, predicate):
        self.predicate = predicate
        
    def __call__(self, item):
        if self.predicate(item):
            return item
        
            
class _Reducer(object):
    '''Helper for the reducer methods'''
    
    def __init__(self, func, init=None):
        self.func = func
        self.list = multiprocessing.Manager().list([init,])
        
    def __call__(self, item):
        aggregate = self.func(self.list[0], item)
        self.list[0] = aggregate
    
    @property
    def result(self):
        return self.list[0]
        
    
class ParallelGen(object):
    
    def __init__(self, data_source):
        self.data = data_source
        self.pool = Pool
    
    def __iter__(self):
        for item in self.data:
            yield item
       
    def foreach(self, func):
        self.data = [i for i in _map(func, self)]
        return None
        
    def filter(self, pred):
        _filter = _Filter(pred)
        return self.__class__((i for i in _map(_filter, self, ) if i is not None))
        
    def flatten(self):
        '''if the data source consists of several sequences, those will be chained in one'''
        return self.__class__(chain(*self))
        
    def map(self, func):
        return self.__class__(_map(func, self, ))
        
    def flatmap(self, func):
        data = self.flatten()
        return self.__class__(_map(func, data, ))
        
    def reduce(self, function, init=None):
        _reducer = _Reducer(function, init)
        for i in _map(_reducer, self, ):
            #need to consume the generator returned by _map
            pass
        return _reducer.result
            
 
def parallel(data_source):
    '''factory function that returns ParallelGen objects, pass an iterable as data_source'''
    if data_source.__class__.__name__ == 'function':
        if data_source().__class__.__name__ == 'generator':
            return ParallelGen(data_source())
    else:
        try:
            iterator = iter(data_source)
        except TypeError:
            raise TypeError('supplied data source must be a generator, a generator function or an iterable, not %s' % data_source.__class__.__name__)     
        return ParallelGen(data_source)


'''Below is all deprecated stuff, with DeprecationWarning'''

def lazy_parallel(data_source):
    raise DeprecationWarning('lazy_parallel has been deprecated, please use "parallel" instead, it has become lazy too :)')
    
class ParallelSeq(object):
    def __init__(self, *args, **kwargs):
        raise DeprecationWarning('{0} has been deprecated, please use the "parallel" factory function instead'.format(self.__class__.__name__))

class ParallelList(ParallelSeq):
    pass

class ParallelDict(ParallelSeq):
    pass

class ParallelString(ParallelSeq):
    pass
