###Python Parallel Collections
####Implementations of dict and list which support parallel map/reduce style operations

####Who said Python was not setup for multicore computing? 
In this package you'll find very simple parallel implementations of list and dict. The parallelism uses the [Python 2.7 backport](http://pythonhosted.org/futures/#processpoolexecutor-example) of the [concurrent.futures](http://docs.python.org/dev/library/concurrent.futures.html) package. If you can define your problem in terms of map/reduce/filter/flatten operations, it will run on several parallel Python processes on your machine, taking advantage of multiple cores. 


####Example

```python
>>> def double(i):
...     return i*2
... 
>>> list_of_list =  ParallelList([[1,2,3],[4,5,6]])
>>> flat_list = list_of_list.flatten()
[1, 2, 3, 4, 5, 6]
>>> list_of_list
[[1, 2, 3], [4, 5, 6]]
>>> flat_list.map(double)
[2, 4, 6, 8, 10, 12]
>>> list_of_list.flatmap(double)
[2, 4, 6, 8, 10, 12]
```

As you see every method call returns a new collection, instead of changing the current one.
The exception is the foreach method, which is equivalent to map but instead of returning a new collection it operates directly on the 
current one and returns it.  
```python
>>> flat_list
[1, 2, 3, 4, 5, 6]
>>> flat_list.foreach(double)
[2, 4, 6, 8, 10, 12]
>>> flat_list
[2, 4, 6, 8, 10, 12]
```

Since every operation returns a collection, these can be chained.
```python
>>> list_of_list =  ParallelList([[1,2,3],[4,5,6]])
>>> list_of_list.flatten().map(double)
[2, 4, 6, 8, 10, 12]
>>> list_of_list.flatmap(double).map(str)
['2', '4', '6', '8', '10', '12']
```

Sadly lambdas, closures and partial functions cannot be passed around multiple processes, so every function that you pass to the collection methods needs to be defined using the def statement. If you want the operation to carry extra state, use a class with a `__call__` method defined.
```python
>>> class multiply(object):
...     def __init__(self, factor):
...         self.factor = factor
...     def __call__(self, item):
...         return item * self.factor
... 
>>> multiply(2)(3)
6
>>>list_of_list =  ParallelList([[1,2,3],[4,5,6]])
>>> list_of_list.flatmap(multiply(2))
[2, 4, 6, 8, 10, 12]
```