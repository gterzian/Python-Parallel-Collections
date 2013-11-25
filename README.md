####Python Parallel Collections
#####Implementations of dict and list which support parallel map/reduce style operations

#####Who said Python was not setup for multicore computing? 
In this package you'll find very simple parallel implementations of list and dict. The parallelism uses the [Python 2.7 backport](http://pythonhosted.org/futures/#processpoolexecutor-example) of the [concurrent.futures](http://docs.python.org/dev/library/concurrent.futures.html) package. If you can define your problem in terms of map/reduce/filter/flatten operations, it will run on several parallel Python processes on your machine, taking advantage of multiple cores. 