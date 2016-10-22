###Python Parallel Collections

[![Build Status](https://travis-ci.org/gterzian/Python-Parallel-Collections.svg?branch=master)](https://travis-ci.org/gterzian/Python-Parallel-Collections)
[![Coverage Status](https://coveralls.io/repos/gterzian/Python-Parallel-Collections/badge.svg?branch=master)](https://coveralls.io/r/gterzian/Python-Parallel-Collections?branch=master)

####Who said Python was not setup for multicore computing? :smiley_cat:
In this package you'll find a convenient interface to parallel map/reduce/filter style operations,  internally using the [Python 2.7 backport](http://pythonhosted.org/futures/#processpoolexecutor-example) of the [concurrent.futures](http://docs.python.org/dev/library/concurrent.futures.html) package.

If you can define your problem in terms of map/reduce/filter operations, it will run on several parallel Python processes on your machine, taking advantage of multiple cores.

_Please note that although the below examples are written in interactive style, due to the nature of multiple processes they might not
actually work in the interactive interpreter._

####API changes in 2

Flatmap is now "properly" implemented, mapping first, then flattening the results. This matches how the function is implemented in most places, [for example in Rust](https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.flat_map).

####API changes in 1.2

Special thanks to https://github.com/kogecoo

In version 1.2, the behavior of parallel ``filter`` method has been changed to match that of CPython's built-in ``filter``.
The *pretty much backward compatible* changes are as follows:

* It is now possible to use ``None`` as a predicate for ``filter``.
  * Previously, it would raise a ``TypeError``.
* It is now possible to include ``None`` elements in an iterable.
  * Previously, ``None`` elements were always filtered out regardless of the predicate.

These modifications are based on [Python's Reference](https://docs.python.org/3.5/library/functions.html#filter), and [source of CPython filter method](https://github.com/python/cpython/blob/master/Python/bltinmodule.c).

####Changes in 1.0
Version 1.0 introduces a massive simplification of the code base. No longer do we operate on concrete 'parallel' data structures, rather we work with just one `ParallelGen` object. This object is essentially a generator with parallel map/filter/reduce methods wrapping around whatever data structure (or generator) you instantiate it with. You instantiate one by passing along an iterable to the `parallel` factory function. Every method call returns a new ParallelGen object, allowing you to chain calls and only evaluate the results when you need them.

_API changes to note:
There is no distinction anymore between using `parallel` and `lazy_parallel`, just use `parallel` and everything will be lazy._

####Getting Started
```python
pip install futures
pip install python-parallel-collections
```
```python
from parallel import parallel
```

####Examples
```python
>>>def double_iterables(l):
...    for item in l:
...        return map(double, l)
...
>>> parallel_gen =  parallel([[1,2,3],[4,5,6]])
>>> list(parallel_gen.flatmap(double_iterables))
[2, 4, 6, 8, 10, 12]
```

Every method call returns a new ParallelGen, instead of changing the current one, with the exception of `reduce` and `foreach`. Also note that you will need to evaluate the generator in order to get the resuts, as is done above through the call to `list`. A call to `reduce` will also return the result.

The foreach method is equivalent to map but instead of returning a new ParallelGen it operates directly on the
current one and returns `None`.
```python
>>> list(flat_gen)
[1, 2, 3, 4, 5, 6]
>>> flat_gen.foreach(double)
None
>>> list(flat_gen)
[2, 4, 6, 8, 10, 12]
```

Since every operation (except `foreach` and `reduce`) returns a collection, operations can be chained.
```python
>>> parallel_gen =  parallel([[1,2,3],[4,5,6]])
>>> list(parallel_gen.flatmap(double_iterables).map(str))
['2', '4', '6', '8', '10', '12']
```

####On being lazy
The parallel function returns a ParallelGen instance, as will any subsequent call to `map`, `filter` and `flatmatp`. This allows you to chain method calls without evaluating the results on every operation.
Instead, each element in the initial datastructure or generator will be processed throught the entire pipeline of method calls one at the time, without creating intermittent datastructures. This is a great way to save memory when working with large or infinite streams of data.

_For more on this technique, see the [Python Cookbook](http://shop.oreilly.com/product/0636920027072.do) 3rd 4.13. Creating Data Processing Pipelines._


####Regarding lambdas and closures
Sadly lambdas, closures and partial functions cannot be passed around multiple processes, so every function that you pass to the higher order methods needs to be defined using the def statement. If you want the operation to carry extra state, use a class with a `__call__` method defined.

###Quick examples of map, reduce and filter

####Map and FlatMap

Functions passed to the map method of a list will be passed every element in the sequence and should return one element. Flatmap will first flatten the sequence then apply map to it.

```python
>>>def double_iterables(l):
...    for item in l:
...        return map(double, l)
...
>>> parallel_gen =  parallel([[1,2,3],[4,5,6]])
>>> parallel_gen.flatmap(double_iterables).map(str)
['2', '4', '6', '8', '10', '12']
>>> def to_upper(item):
...     return item.upper()
...
>>> p = parallel('qwerty')
>>> mapped = p.map(to_upper)
>>> ''.join(mapped)
'QWERTY'
```

####Reduce
Reduce accepts an optional initializer, which will be passed as the first argument to every call to the function passed as reducer and returned by the method.
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
The Filter method should be passed a predicate, which means a function that will return True or False and will be called once for every element in the sequence.
```python
>>> def is_digit(item):
...     return item.isdigit()
...
>>> p = parallel(['a','2','3'])
>>> pred = is_digit
>>> filtered = p.filter(pred)
>>> list(filtered)
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
>>> filtered = p.filter(is_digit)
>>>''.join(filtered)
'23'
```
