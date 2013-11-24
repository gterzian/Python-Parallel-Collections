from multiprocessing import Pool
from itertools import chain, imap
from functools import partial


class BaseParallelCollection(object):
    
    def __init__(self, inner_collection):
        self.inner_collection = inner_collection or list()
        self.pool = Pool()
    
    def __len__(self):
        return len(self.inner_collection)
        
    def __reversed__(self):
        return reversed(self.inner_collection)
        
    def __iter__(self):
        return iter(self.inner_collection)
    
    def __getitem__(self, key):
        return self.inner_collection[key]
    
    def __setitem__(self, key, value):
        self.inner_collection[key] = value
        
    def __contains__(self, item):
        return item in self.inner_collection
        
    def __delitem__(self, key):
        self.inner_collection.remove(key)


class ParallelSeq(BaseParallelCollection):
    
    def __init__(self, inner_collection):
        self.inner_collection = inner_collection or list()
        self.pool = Pool()
        
    def __iter__(self):
        for i in self.inner_collection:
            yield i
        
    def map(self, func):
        inner_collection = list(self)
        if len(inner_collection)/4 > 1:
            chunksize = len(inner_collection)/4
        else:
            chunksize = 1
        self.inner_collection = self.pool.imap(func, inner_collection, chunksize)
        return self
        
    def flatten(self):
        self.inner_collection = chain(*self.inner_collection)
        return self
        
    def flatmap(self, func):
        inner_collection = list(self.flatten())
        if len(inner_collection)/4 > 1:
            chunksize = len(inner_collection)/4
        else:
            chunksize = 1
        self.inner_collection = self.pool.imap(func, inner_collection, chunksize)
        return self
        
    def reduce(self, function, initializer=None):
        it = iter(self.inner_collection)
        if initializer is None:
            try:
                initializer = next(it)
            except StopIteration:
                raise TypeError('reduce() of empty sequence with no initial value')
        accum_value = initializer
        for x in it:
            accum_value = function(accum_value, x)
        return accum_value

    