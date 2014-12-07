from concurrent import futures
import multiprocessing
from itertools import chain


Pool = futures.ProcessPoolExecutor()


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
        self.data = [i for i in self.pool.map(func, self)]
        return None
        
    def filter(self, pred):
        _filter = _Filter(pred)
        return self.__class__(i for i in self.pool.map(_filter, self, ) if i)
        
    def flatten(self):
        '''if the data source consists of several sequences, those will be chained in one'''
        return self.__class__(chain(*self))
        
    def map(self, func):
        return self.__class__(self.pool.map(func, self, ))
        
    def flatmap(self, func):
        data = self.flatten()
        return self.__class__(self.pool.map(func, data, ))
        
    def reduce(self, function, init=None):
        _reducer = _Reducer(function, init)
        for i in self.pool.map(_reducer, self, ):
            #need to consume the generator returned by self.pool.map
            pass
        return _reducer.result
        
        
def parallel(data_source):
    if data_source.__class__.__name__ == 'function':
        if data_source().__class__.__name__ == 'generator':
            return ParallelGen(data_source())
    else:
        try:
            iterator = iter(data_source)
        except TypeError:
            raise TypeError('supplied data source must be a generator, a generator function or an iterable, not %s' % data_source.__class__.__name__)     
        return ParallelGen(data_source)