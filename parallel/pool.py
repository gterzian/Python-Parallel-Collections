from bisect import bisect
from itertools import chain
from multiprocessing import pool


def mapfilter(args):
    return filter(*args)


class FPPool(pool.Pool):
    def filter(self, func, iterable, chunksize=None):
        return self._filter_async(func, iterable, mapfilter, chunksize).get()

    def _filter_async(self, func, iterable, mapper, chunksize=None, callback=None, error_callback=None):
        '''
        Helper function to implement filter.
        '''
        self._check_running()
        if not hasattr(iterable, '__len__'):
            iterable = list(iterable)

        if chunksize is None:
            chunksize, extra = divmod(len(iterable), len(self._pool) * 4)
            if extra:
                chunksize += 1
        if len(iterable) == 0:
            chunksize = 0

        task_batches = FPPool._get_tasks(func, iterable, chunksize)
        result = FilterResult(self, chunksize, len(iterable), callback, error_callback=error_callback)
        self._taskqueue.put(
            (
                self._guarded_task_generation(result._job,
                                              mapper,
                                              task_batches),
                None
            )
        )
        return result


class FilterResult(pool.MapResult):

    def __init__(self, pool_, chunksize, length, callback, error_callback):
        pool.ApplyResult.__init__(self, pool_, callback, error_callback=error_callback)
        self._success = True
        self._value = []
        self._mask = []
        self._chunksize = chunksize
        if chunksize <= 0:
            self._number_left = 0
            self._event.set()
            del self._cache[self._job]
        else:
            self._number_left = length//chunksize + bool(length % chunksize)

    def get(self, timeout=None):
        self.wait(timeout)
        if not self.ready():
            raise TimeoutError
        if self._success:
            return chain(*self._value)
        else:
            raise self._value

    def _set(self, i, success_result):
        self._number_left -= 1
        success, result = success_result
        if success and self._success:
            idx = bisect(self._mask, i)
            self._mask.insert(idx, i)
            self._value.insert(idx, result)
            if self._number_left == 0:
                if self._callback:
                    self._callback(self._value)
                del self._cache[self._job]
                self._event.set()
                self._pool = None
        else:
            if not success and self._success:
                # only store first exception
                self._success = False
                self._value = result
            if self._number_left == 0:
                # only consider the result ready once all jobs are done
                if self._error_callback:
                    self._error_callback(self._value)
                del self._cache[self._job]
                self._event.set()
                self._pool = None
