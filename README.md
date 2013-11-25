###Python Parallel Collections
####Implementations of dict and list which support parallel map/reduce style operations

####Who said Python was not setup for multicore computing? 
In this package you'll find very simple parallel implementations of list and dict. The parallelism uses the [Python 2.7 backport](http://pythonhosted.org/futures/#processpoolexecutor-example) of the [concurrent.futures](http://docs.python.org/dev/library/concurrent.futures.html) package. If you can define your problem in terms of map/reduce/filter/flatten operations, it will run on several parallel Python processes on your machine, taking advantage of multiple cores. 


####Example

assuming a function called double that doubles a number passed to it:

```python
list_of_list =  ParallelList([[1,2,3],[4,5,6]])
flat_list = list_of_list.flatten()
[1, 2, 3, 4, 5, 6]
list_of_list
[[1, 2, 3], [4, 5, 6]]
flat_list.map(double)
[2, 4, 6, 8, 10, 12]
list_of_list.flatmap(double)
[2, 4, 6, 8, 10, 12]
```

As you see every method call returns a new collection. Flatmap is the equivalent of flattening and then mapping. 
