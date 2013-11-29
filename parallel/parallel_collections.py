from concurrent import futures
from multiprocessing import Manager
from itertools import chain, imap
from UserList import UserList
from UserDict import UserDict
from UserString import UserString


Pool = futures.ProcessPoolExecutor()

class _Filter(object):
    '''Helper for the filter methods, 
    need to use class as closures cannot be pickled and sent to other processes'''
    
    def __init__(self, predicate):
        self.predicate = predicate
        
    def __call__(self, item):
        if self.predicate(item):
            return item
        
            
class _Reducer(object):
    '''Helper for the reducer methods'''
    
    def __init__(self, func, init=None):
        self.func = func
        self.list = Manager().list([init,])
        
    def __call__(self, item):
        init = self.func(self.list[0], item)
        self.list[0] = init
    
    @property
    def result(self):
        return self.list[0]


class ParallelSeq(object):
        
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