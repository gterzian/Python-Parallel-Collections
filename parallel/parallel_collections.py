from concurrent import futures
from itertools import chain, imap
from UserList import UserList
from UserDict import UserDict


Pool = futures.ProcessPoolExecutor()

class _Filter(object):
    '''Helper for the filter methods, 
    need to use class as closures cannot be pickled and sent to other processes'''
    
    def __init__(self, pred):
        self.pred = pred
        
    def __call__(self, item):
        if self.pred(item):
            return item


class ParallelSeq(object):
        
    def foreach(self, func):
        '''operates the func on every item the internal data, returns the same collection'''
        raise(NotImplemented)
    
    def filter(self, pred):
        '''will filter the internal data with the predicate, returns a new collection'''
        raise(NotImplemented)
        
    def map(self, func):
        '''operates the func on every item the internal data, returns a new collection'''
        raise(NotImplemented)
        
    def flatten(self):
        '''this will differ based on the underlying data struct'''
        raise(NotImplemented)
        
    def flatmap(self, func):
        raise(NotImplemented)
        
    def reduce(self, function, initializer=None):
        raise(NotImplemented)
    

class ParallelList(UserList, ParallelSeq):
    
    def __init__(self, *args, **kwargs):
        self.pool = Pool
        super(ParallelList, self).__init__(*args, **kwargs)
        
    def __iter__(self):
        return iter(self.data)
        
    def foreach(self, func):
        self.data = list(self.pool.map(func, self))
        return None
    
    def filter(self, pred):
        _filter = _Filter(pred)
        return ParallelList(i for i in self.pool.map(_filter, self, ) if i)
        
    def map(self, func):
        return ParallelList(self.pool.map(func, self, ))
        
    def flatten(self):
        '''if the list consists of several sequences, those will be chained in one'''
        return ParallelList(chain(*self))
        
    def flatmap(self, func):
        data = self.flatten()
        return ParallelList(self.pool.map(func, data, ))
        
    def reduce(self, function, initializer=None):
        return reduce(function, self, initializer)
        
        
class ParallelDict(UserDict, ParallelSeq):
    
    def __init__(self, *args, **kwargs):
        self.pool = Pool
        super(ParallelDict, self).__init__(*args, **kwargs)
        
    def __iter__(self):
        for i in self.data.items():
            yield i
            
    def foreach(self, func):
        self.data =  dict(self.pool.map(func, self, ))
        return None
    
    def filter(self, pred):
        _filter = _Filter(pred)
        return ParallelDict(i for i in self.pool.map(_filter, self, ) if i)
        
    def map(self, func):
        return ParallelDict(self.pool.map(func, self, ))
        
    def flatten(self):
        '''if the values of the dict consists of several sequences, those will be chained in one'''
        flat = []
        for k,v in self:
            try:
                flat.append((k,list(chain(*v))))
            except TypeError:
                flat.append((k, v))
        return ParallelDict(flat)
        
    def flatmap(self, func):
        data = self.flatten()
        return ParallelDict(self.pool.map(func, data, ))
        
    def reduce(self, function, initializer=None):
        return reduce(function, self, initializer)
