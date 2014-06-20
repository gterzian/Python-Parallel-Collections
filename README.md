###Python Parallel Collections

####Who said Python was not setup for multicore computing? 
In this package you'll find a convenient interface to map/reduce/filter style operations that can be performed on standard Python data structures and generators using multiple processes. The parallelism uses the [Python 2.7 backport](http://pythonhosted.org/futures/#processpoolexecutor-example) of the [concurrent.futures](http://docs.python.org/dev/library/concurrent.futures.html) package. If you can define your problem in terms of map/reduce/filter operations, it will run on several parallel Python processes on your machine, taking advantage of multiple cores. 

Please note that although the below examples are written in interactive style, due to the nature of multiple processes they might not 
actually work in the interactive interpreter.


####Changes in 0.2
Version 0.2 introduces a simple functional interface to the package which should be favored over using the classes directly. The "parallel"  function returns an object matching the type of the data structure/generator passed as argument to it, which supports the map/flatmap/reduce/filter methods. the "lazy_parallel" returns a similar object expect that the result of any map/flatmap/reduce/filter operation will only be evaluated on demand, allowing you to chain calls while avoiding intermediary evaluation. 

One API change to note:

Where previously one would do:
```python
p = ParallelDict(zip(range(3), ['a','2', '3',]))
```

One should now instead do(note the call to dict):
```python
p = parallel(dict(zip(range(3), ['a','2', '3',])))
```

The classes are still available however the functional interface should be favored for simplicity and future compatibility.

####Getting Started
```python
pip install python-parallel-collections
pip install futures
```
```python
from parallel import parallel, lazy_parallel
```

####Examples

```python
>>> def double(i):
...     return i*2
... 
>>> list_of_list =  parallel([[1,2,3],[4,5,6]])
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
current one and returns `None`.  
```python
>>> flat_list
[1, 2, 3, 4, 5, 6]
>>> flat_list.foreach(double)
None
>>> flat_list
[2, 4, 6, 8, 10, 12]
```

Since every operation (except foreach) returns a collection, these can be chained.
```python
>>> list_of_list =  parallel([[1,2,3],[4,5,6]])
>>> list_of_list.flatmap(double).map(str)
['2', '4', '6', '8', '10', '12']
```

####On being lazy
To avoid the evaluation of an intermittent result, you want to use lazy_parallel on any datastructure, or just pass a generator expression or function to either parallel or lazy_parallel. This will allow you to chain method calls without evaluating the result on every operation(just like you would when building data processing pipelines using a chain of generator functions, see Python Cookbook 3rd 4.13. Creating Data Processing Pipelines). Each element in the datastructure or generator stream will be processed one by one and the final result only evaluated on demand. This is a great way to save memory and work with potentially large or infinite streams of data. 

Please note that beacause lazy_parallel is meant to operate on all data structures passed to it coherently, it will not work well with dictionaries, as only their keys will be iterated over(because that is what happens when you call iter() on dictionaries in Python). If you want to work with the key/values of dictionary in a lazy way, best to just do:

```python
lazy_result = lazy_parallel(your_dict.items()).map(something).filter(something_else)
evaluated_result = dict(lazy_result)
```
Personnally I would recommend just always using lazy_parallel and passing it whatever source of iterable data you want to work with, later evaluating the result into whatever type of value you need. I think it will make your code much easier to read than trying to guess what type of datastructure is returned after a bunch of chained calls using parallel. 

The below examples illustrates lazy evaluation. Each operation on the parallel list results in the entire list being evaluated before the next operation, while the various generators allow every element go through each step before sending the next one in. 
Also note the the generator will not result in anything happening unless you actually do something to evaluate it (such as the list comprehension does in the below example). 

```python
>>> def _print(item):
...     print item 
...     return item
... 
>>> def double(item):
...     return item * 2 
... 
>>> plist = parallel(range(5))
>>> [i for i in plist.map(double).map(_print).map(double).map(_print)]
0
2
4
6
8
0
4
8
12
16
>>> pgen = lazy_parallel(range(5))
>>> [i for i in pgen.map(double).map(_print).map(double).map(_print)]
0
0
2
4
4
8
6
12
8
16
>>> another_pgen = lazy_parallel((num for num in range(5)))
>>> [i for i in pgen.map(double).map(_print).map(double).map(_print)]
0
0
2
4
4
8
6
12
8
16
>>> def some_gen():
...     for num in range(5):
...         yield num
...
>>> another_pgen = lazy_parallel(some_gen)
>>> [i for i in another_pgen.map(double).map(_print).map(double).map(_print)]
0
0
2
4
4
8
6
12
8
16
```

####Regarding lambdas and closures
Sadly lambdas, closures and partial functions cannot be passed around multiple processes, so every function that you pass to the higher order methods needs to be defined using the def statement. If you want the operation to carry extra state, use a class with a `__call__` method defined.
```python
>>> class multiply(object):
...     def __init__(self, factor):
...         self.factor = factor
...     def __call__(self, item):
...         return item * self.factor
... 
>>> multiply(2)(3)
6
>>> list_of_list =  parallel([[1,2,3],[4,5,6]])
>>> list_of_list.flatmap(multiply(2))
[2, 4, 6, 8, 10, 12]
```

###Quick examples of map, reduce and filter

####Map and FlatMap

Functions passed to the map method of a list will be passed every element in the list and should return a single element. For a dict, the function will receive a tuple (key, values) for every key in the dict, and should equally return a two element sequence. Flatmap will first flatten the sequence then apply map to it.
 
```python
>>> def double(item):
...    return item * 2
...
>>> list_of_list =  parallel([[1,2,3],[4,5,6]])
>>> list_of_list.flatmap(double).map(str)
['2', '4', '6', '8', '10', '12']
>>> def double_dict(item):
...     k,v = item
...     try:
...         return [k, [i *2 for i in v]]
...     except TypeError:
...         return [k, v * 2]
... 
>>> d = parallel(dict(zip(range(2), [[[1,2],[3,4]],[3,4]])))
>>> d
{0: [[1, 2], [3, 4]], 1: [3, 4]}
>>> flat_mapped = d.flatmap(double_dict)
>>> flat_mapped
{0: [2, 4, 6, 8], 1: [6, 8]}

>>> def to_upper(item):
...     return item.upper() 
... 
>>> p = parallel('qwerty')
>>> mapped = p.map(to_upper)
'QWERTY'
```

####Reduce
Reduce accepts an optional initializer, which will be passed as the first argument to every call to the function passed as reducer
```python
>>> def group_letters(all, letter):
...     all[letter].append(letter)
...     return all
... 
>>> p = parallel(['a', 'a', 'b'])
>>> reduced = p.reduce(group_letters, defaultdict(list))
{'a': ['a', 'a'], 'b': ['b']}
>>> p = parallel('aab')
>>> p.reduce(group_letters, defaultdict(list))
{'a': ['a', 'a'], 'b': ['b']}
```

####Filter
The Filter method should be passed a predicate, which means a function that will return True or False and will be called once for every element in the list and for every (key, values) in a dict.
```python
>>> def is_digit(item):
...     return item.isdigit()
...
>>> p = parallel(['a','2','3'])
>>> pred = is_digit
>>> filtered = p.filter(pred)
>>> filtered
['2', '3']

>>> def is_digit_dict(item):
...    return item[1].isdigit()
...
>>> p = parallel(dict(zip(range(3), ['a','2', '3',])))
{0: 'a', 1: '2', 2: '3'}
>>> pred = is_digit_dict
>>> filtered = p.filter(pred)
>>> filtered
{1: '2', 2: '3'}
>>> p = parallel('a23')
>>> p.filter(is_digit)
'23'
```
