from concurrent import futures
import multiprocessing
from itertools import chain, imap, izip
from UserList import UserList
from UserDict import UserDict
from UserString import UserString


Pool = futures.ProcessPoolExecutor()


def _map(fn, *iterables):
    '''using our own internal map function to avoid evaluation of the generator as done in futures.ProcessPoolExecutor().map'''
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
        
        
def parallel_gen(data_source):
    '''closure implementation of a parallel generator. Unfortunately you need the call to result()
     to access the internal generator after any map/filter/reduce call, because a generator function does
     not directly support the iterator prototocol(you need to call the function to get the generator first)
     Just an experiment for now, guess this is just not how Python works'''
    
    pool = Pool
    
    def inner_gen():
        for item in data_source:
            yield item
            
    def result():
        return inner_gen()
        
    def inner_filter(pred):
        _filter = _Filter(pred)
        return parallel_gen(i for i in _map(_filter, inner_gen()) if i)
        
    def inner_flatten():
        '''if the data source consists of several sequences, those will be chained in one'''
        return parallel_gen(chain(*inner_gen()))
        
    def inner_map(func):
        return parallel_gen(_map(func, inner_gen()))
        
    def inner_flatmap(func):
        return parallel_gen(_map(func, inner_flatten()()))
        
    def inner_reduce(function, init=None):
        _reducer = _Reducer(function, init)
        [i for i in _map(_reducer, inner_gen())]
        return _reducer.result
        
    inner_gen.map = inner_map
    inner_gen.filter = inner_filter
    inner_gen.flatten = inner_flatten
    inner_gen.flatmap = inner_flatmap
    inner_gen.reduce = inner_reduce
    inner_gen.result = result
    return inner_gen
    
        
class ParallelSeq(object):
    '''some sort of base class/interface for the other concrete ones'''
        
    def foreach(self, func):
        self.data = self.__class__(self.pool.map(func, self))
        return None
    
    def filter(self, pred):
        _filter = _Filter(pred)
        return self.__class__(i for i in self.pool.map(_filter, self, ) if i)
        
    def flatten(self):
        '''if the list consists of several sequences, those will be chained in one'''
        return self.__class__(chain(*self))
        
    def map(self, func):
        return self.__class__(self.pool.map(func, self, ))
        
    def flatmap(self, func):
        data = self.flatten()
        return self.__class__(self.pool.map(func, data, ))
        
    def reduce(self, function, init=None):
        _reducer = _Reducer(function, init)
        for i in self.pool.map(_reducer, self, ):
            #need to consume the generator returned by pool.map
            pass
        return _reducer.result
        

class ParallelGen(ParallelSeq):
    
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
        return self.__class__(i for i in _map(_filter, self, ) if i)
        
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
        

class ParallelList(UserList, ParallelSeq):
    
    def __init__(self, *args, **kwargs):
        self.pool = Pool
        super(ParallelList, self).__init__(*args, **kwargs)
        
    def __iter__(self):
        return iter(self.data)
        
        
class ParallelDict(UserDict, ParallelSeq):
    
    def __init__(self, *args, **kwargs):
        self.pool = Pool
        super(ParallelDict, self).__init__(*args, **kwargs)
        
    def __iter__(self):
        for i in self.data.items():
            yield i

    def flatten(self):
        '''if the values of the dict consists of several sequences, those will be chained in one'''
        flat = []
        for k,v in self:
            try:
                flat.append((k,list(chain(*v))))
            except TypeError:
                flat.append((k, v))
        return ParallelDict(flat)
        
    
class ParallelString(UserString, ParallelSeq):
    
    def __init__(self, *args, **kwargs):
        self.pool = Pool
        super(ParallelString, self).__init__(*args, **kwargs)
        
    def __iter__(self):
        return iter(self.data)
        
    def foreach(self, func):
        self.data = self.__class__(''.join(self.pool.map(func, self)))
        return None
        
    def filter(self, pred):
        _filter = _Filter(pred)
        data = (i for i in self.pool.map(_filter, self, ) if i)
        return self.__class__(''.join(data))
        
    def map(self, func):
        data = self.pool.map(func, self)
        return self.__class__(''.join(data))
        
    def flatmap(self, func):
        return self.map(func)
        
    def flatten(self):
        return self
        

def parallel(data_source):
    if data_source.__class__.__name__ == 'generator':
        return ParallelGen(data_source)
    elif data_source.__class__.__name__ == 'function':
        if data_source().__class__.__name__ == 'generator':
            return ParallelGen(data_source())
    else:
        try:
            iterator = iter(data_source)
        except TypeError:
            raise TypeError('supplied data source must be a generator, a generator function or an iterable, not %s' % data_source.__class__.__name__)
        if iterator.__class__.__name__ in ['listiterator', 'tupleiterator', 'setiterator']:
            return ParallelList(data_source)
        elif iterator.__class__.__name__ == 'dictionary-keyiterator':
            return ParallelDict(data_source)
        elif iterator.__class__.__name__ == 'iterator':
            return ParallelString(data_source)
    raise TypeError('supplied data source must be a generator, a generator function or an iterable, not %s' % data_source.__class__.__name__)
    
    
def lazy_parallel(data_source):
    if data_source.__class__.__name__ == 'function':
        if data_source().__class__.__name__ == 'generator':
            return ParallelGen(data_source())
    else:
        try:
            iterator = iter(data_source)
        except TypeError:
            raise TypeError('supplied data source must be a generator, a generator function or an iterable, not %s' % data_source.__class__.__name__)     
        return ParallelGen(data_source)