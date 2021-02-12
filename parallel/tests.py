import unittest
from itertools import chain
from collections import defaultdict

from parallel_collections import parallel, _Reducer


class TestHelpers(unittest.TestCase):

    def test_reducer_two_items(self):
        reducer = _Reducer(group_letters, defaultdict(list))
        reducer('a')
        reducer('a')
        self.assertEqual(reducer.result, {'a': ['a', 'a']})

    def test_reducer_three_items(self):
        reducer = _Reducer(group_letters, defaultdict(list))
        reducer('a')
        reducer('a')
        reducer('b')
        self.assertEqual(reducer.result, {'a': ['a', 'a'], 'b': ['b']})


class TestGen(unittest.TestCase):

    def test_foreach(self):
        p = parallel((d for d in [list(range(10)), list(range(10))]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p.foreach(double)
        self.assertEqual(list(p), list(map(double, [list(range(10)), list(range(10))])))
        self.assertTrue(p.foreach(double) is None)

    def test_map(self):
        p = parallel((d for d in [list(range(10)), list(range(10))]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        mapped = p.map(double)
        self.assertEqual(list(mapped), list(map(double, [list(range(10)), list(range(10))])))
        self.assertFalse(mapped is p)

    def test_filter(self):
        p = parallel((d for d in ['a', '2', '3']))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        pred = is_digit
        filtered = p.filter(pred)
        self.assertEqual(list(filtered), list(['2', '3']))
        self.assertFalse(filtered is p)

    def test_filter_with_ret_none_func(self):
        p = parallel((d for d in [True, False]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        pred = ret_none
        filtered = p.filter(pred)
        self.assertEqual(list(filtered), list([]))
        self.assertFalse(filtered is p)

    def test_filter_with_none_as_func(self):
        p = parallel((d for d in [False, True]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        pred = None
        filtered = p.filter(pred)
        self.assertEqual(list(filtered), list([True]))
        self.assertFalse(filtered is p)

    def test_filter_for_none_false_elements(self):
        p = parallel((d for d in [False, True, None]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        pred = is_none_or_false
        filtered = p.filter(pred)
        self.assertEqual(list(filtered), list([False, None]))
        self.assertFalse(filtered is p)

    def test_flatmap(self):
        p = parallel([list(range(10)), list(range(10))])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEqual(
            list(
                p.flatmap(double_iterables)
            ),
            list(
                map(double, chain(*[list(range(10)), list(range(10))]))
            )
        )
        self.assertFalse(p.flatmap(double) is p)

    def test_chaining(self):
        p = parallel((d for d in chain(range(10), range(10))))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEqual(
            list(
                p.filter(is_even).map(double)
            ),
            list(
                parallel([range(10), range(10)]).flatmap(double_evens_in_iterables)
            )
        )

    def test_reduce(self):
        p = parallel((d for d in ['a', 'a', 'b']))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        reduced = p.reduce(group_letters, defaultdict(list))
        self.assertEqual(dict(a=['a', 'a'], b=['b']), dict(reduced))
        self.assertFalse(reduced is p)

    def test_gen_func_foreach(self):
        def inner_gen():
            for d in [list(range(10)), list(range(10))]:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p.foreach(double)
        self.assertEqual(list(p), list(map(double, [list(range(10)), list(range(10))])))
        self.assertTrue(p.foreach(double) is None)

    def test_gen_func_map(self):
        def inner_gen():
            for d in [list(range(10)), list(range(10))]:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        mapped = p.map(double)
        self.assertEqual(list(mapped), list(map(double, [list(range(10)), list(range(10))])))
        self.assertFalse(mapped is p)

    def test_gen_func_filter(self):
        def inner_gen():
            for d in ['a', '2', '3']:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        pred = is_digit
        filtered = p.filter(pred)
        self.assertEqual(list(filtered), list(['2', '3']))
        self.assertFalse(filtered is p)

    def test_gen_func_flatmap(self):
        def inner_gen():
            for d in [range(10), range(10)]:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEqual(list(p.flatmap(double_iterables)), list(map(double, chain(*[range(10), range(10)]))))
        self.assertFalse(p.flatmap(double) is p)

    def test_gen_func_chaining(self):
        def inner_gen():
            for d in chain(*[range(10), range(10)]):
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEqual(
            list(
                p.filter(is_even).map(double)
            ),
            list(
                parallel([range(10), range(10)]).flatmap(double_evens_in_iterables)
            )
        )

    def test_gen_func_reduce(self):
        def inner_gen():
            for d in ['a', 'a', 'b']:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        reduced = p.reduce(group_letters, defaultdict(list))
        self.assertEqual(dict(a=['a', 'a'], b=['b']), dict(reduced))
        self.assertFalse(reduced is p)

    def test_foreach_lambda(self):
        p = parallel((d for d in [list(range(10)), list(range(10))]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p.foreach(lambda x: 2 * x)
        self.assertEqual(list(p), list(map(lambda x: 2 * x, [list(range(10)), list(range(10))])))
        self.assertTrue(p.foreach(double) is None)

    def test_map_lambda(self):
        p = parallel((d for d in [list(range(10)), list(range(10))]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        mapped = p.map(lambda x: 2 * x)
        self.assertEqual(list(mapped), list(map(lambda x: 2 * x, [list(range(10)), list(range(10))])))
        self.assertFalse(mapped is p)

    def test_filter_lambda(self):
        p = parallel((d for d in ['a', '2', '3']))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        filtered = p.filter(lambda s: s.isdigit())
        self.assertEqual(list(filtered), list(['2', '3']))
        self.assertFalse(filtered is p)

    def test_flatmap_lambda(self):
        p = parallel([list(range(10)), list(range(10))])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEqual(
            list(
                p.flatmap(double_iterables)
            ),
            list(
                map(lambda x: 2 * x, chain(*[list(range(10)), list(range(10))]))
            )
        )
        self.assertFalse(p.flatmap(lambda x: 2 * x) is p)

    def test_reduce_lambda(self):
        p = parallel(range(1, 6))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        reduced = p.reduce(lambda x, y: x * y, 1)
        self.assertEqual(120, reduced)
        self.assertFalse(reduced is p)


class TestFactories(unittest.TestCase):

    def test_returns_gen(self):
        p = parallel((d for d in [range(10), range(10)]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')

        def inner_gen():
            for i in range(10):
                yield i

        p = parallel(inner_gen())
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p = parallel(list())
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p = parallel(tuple())
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p = parallel(set())
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')

    def test_raises_exception(self):
        def inner_func():
            return 1

        class InnerClass(object):
            pass

        with self.assertRaises(TypeError):
            parallel(inner_func())
        with self.assertRaises(TypeError):
            parallel(1)
        with self.assertRaises(TypeError):
            parallel(InnerClass())


def _print(item):
    print(item)
    return item


def to_upper(item):
    return item.upper()


def is_digit(item):
    return item.isdigit()


def is_digit_dict(item):
    return item[1].isdigit()


def add_up(x, y):
    return x + y


def group_letters(all, letter):
    all[letter].append(letter)
    return all


def group_letters_dict(all, letter):
    letter = letter[1]
    all[letter].append(letter)
    return all


def chain_iterables(its):
    return chain(*its)


def double(item):
    return item * 2


def double_iterables(lst):
    for _ in lst:
        return list(map(double, lst))


def is_even(i):
    if i & 1:
        return False
    else:
        return True


def double_evens(i):
    if is_even(i):
        return i * 2


def double_evens_in_iterables(lst):
    for _ in lst:
        return map(double, filter(is_even, lst))


def double_dict(item):
    k, v = item
    try:
        return [k, [i * 2 for i in v]]
    except TypeError:
        return [k, v * 2]


def ret_none(_):
    return None


def is_none_or_false(item):
    return item is None or item is False


if __name__ == '__main__':
    unittest.main()
