#   Copyright 2018 Braxton Mckee
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from typed_python import (
    Bool,
    Int8, Int16, Int32, Int64,
    UInt8, UInt16, UInt32, UInt64,
    Float32, Float64,
    NoneType, TupleOf, ListOf, OneOf, Tuple, NamedTuple, Dict,
    ConstDict, Alternative, serialize, deserialize, Class, Member,
    TypeFilter, Function
)

from typed_python.test_util import currentMemUsageMb
import typed_python._types as _types
import psutil
import unittest
import time
import numpy
import os


def typeFor(t):
    assert not isinstance(t, list), t
    return type(t)


def typeForSeveral(t):
    ts = set(typeFor(a) for a in t)
    if len(ts) == 1:
        return list(ts)[0]
    return OneOf(*ts)


def makeTupleOf(*args):
    if not args:
        return TupleOf(int)()
    return TupleOf(typeForSeveral(args))(args)


def makeNamedTuple(**kwargs):
    if not kwargs:
        return NamedTuple()()
    return NamedTuple(**{k: typeFor(v) for k, v in kwargs.items()})(**kwargs)


def makeTuple(*args):
    if not args:
        return Tuple()()
    return Tuple(*[typeFor(v) for v in args])(args)


def makeDict(d):
    if not d:
        return ConstDict(int, int)()

    return ConstDict(typeForSeveral(d.keys()), typeForSeveral(d.values()))(d)


def makeAlternative(severalDicts):
    types = list(
        set(
            tuple(
                (k, typeFor(v)) for k, v in ntDict.items()
            )
            for ntDict in severalDicts
        )
    )

    alt = Alternative("Alt", **{
        "a_%s" % i: dict(types[i]) for i in range(len(types))
    })

    res = []
    for thing in severalDicts:
        did = False
        for i in range(len(types)):
            try:
                res.append(getattr(alt, "a_%s" % i)(**thing))
                did = True
            except Exception:
                pass

            if did:
                break
    assert len(res) == len(severalDicts)

    return res


def choice(x):
    # numpy.random.choice([1,(1,2)]) blows up because it looks 'multidimensional'
    # so we have to pick from a list of indices
    if not isinstance(x, list):
        x = list(x)
    return x[numpy.random.choice(list(range(len(x))))]


class RandomValueProducer:
    def __init__(self):
        self.levels = {0: [b'1', b'', '2', '', 0, 1, 0.0, 1.0, None, False, True, "a ", "a string", "b string", "b str"]}

    def addEvenly(self, levels, count):
        for level in range(1, levels+1):
            self.addValues(level, count)

    def all(self):
        res = []
        for valueList in self.levels.values():
            res.extend(valueList)
        return res

    def addValues(self, level, count, sublevels=None):
        assert level > 0

        if sublevels is None:
            sublevels = list(range(level))
        sublevels = [x for x in sublevels if x in self.levels]

        assert sublevels

        def picker():
            whichLevel = choice(sublevels)
            try:
                return choice(self.levels[whichLevel])
            except Exception:
                print(self.levels[whichLevel])
                raise

        for _ in range(count):
            val = self.randomValue(picker)
            if not isinstance(val, list):
                val = [val]
            self.levels.setdefault(level, []).extend(val)

    def randomValue(self, picker):
        def randomTuple():
            return makeTuple(*[picker() for i in range(choice([0, 1, 2, 3, 4]))])

        def randomNamedTupleDict():
            return {"x_%s" % i: picker() for i in range(choice([0, 1, 2, 3, 4]))}

        def randomNamedTuple():
            return makeNamedTuple(**randomNamedTupleDict())

        def randomDict():
            return makeDict({picker(): picker() for i in range(choice([0, 1, 2, 3, 4]))})

        def randomTupleOf():
            return makeTupleOf(*[picker() for i in range(choice([0, 1, 2, 3, 4]))])

        def randomAlternative():
            return makeAlternative([randomNamedTupleDict() for i in range(choice([1, 2, 3, 4]))])

        return choice([randomTuple, randomNamedTuple, randomDict, randomTupleOf, randomAlternative, picker])()

    def pickRandomly(self):
        return choice(self.levels[choice(list(self.levels))])


class NativeTypesTests(unittest.TestCase):

    def check_expected_performance(self, elapsed, expected=1.0):
        if os.environ.get('TRAVIS_CI', None) is not None:
            expected = 2 * expected

        self.assertTrue(
            elapsed < expected,
            "Slow Performance: expected to take {expected} sec, but took {elapsed}"
            .format(expected=expected, elapsed=elapsed)
        )

    def test_objects_are_singletons(self):
        self.assertTrue(Int8 is Int8)
        self.assertTrue(NoneType is NoneType)

    def test_object_binary_compatibility(self):
        ibc = _types.isBinaryCompatible

        self.assertTrue(ibc(NoneType, NoneType))
        self.assertTrue(ibc(Int8, Int8))

        NT = NamedTuple(a=int, b=int)

        class X(NamedTuple(a=int, b=int)):
            pass

        class Y(NamedTuple(a=int, b=int)):
            pass

        self.assertTrue(ibc(X, X))
        self.assertTrue(ibc(X, Y))
        self.assertTrue(ibc(X, NT))
        self.assertTrue(ibc(Y, NT))
        self.assertTrue(ibc(NT, Y))

        self.assertFalse(ibc(OneOf(int, float), OneOf(float, int)))
        self.assertTrue(ibc(OneOf(int, X), OneOf(int, Y)))

    def test_binary_compatibility_incompatible_alternatives(self):
        ibc = _types.isBinaryCompatible

        A1 = Alternative("A1", X={'a': int}, Y={'b': float})
        A2 = Alternative("A2", X={'a': int}, Y={'b': str})

        self.assertTrue(ibc(A1, A1.X))
        self.assertTrue(ibc(A1, A1.Y))
        self.assertTrue(ibc(A1.Y, A1.Y))
        self.assertTrue(ibc(A1.Y, A1))
        self.assertTrue(ibc(A1.X, A1))
        self.assertFalse(ibc(A1.X, A1.Y))

        self.assertFalse(ibc(A1, A2))
        self.assertFalse(ibc(A1.X, A2.X))
        self.assertFalse(ibc(A1.Y, A2.Y))

    def test_binary_compatibility_compatible_alternatives(self):
        ibc = _types.isBinaryCompatible

        A1 = Alternative("A1", X={'a': int}, Y={'b': float})
        A2 = Alternative("A2", X={'a': int}, Y={'b': float})

        self.assertTrue(ibc(A1.X, A2.X))
        self.assertTrue(ibc(A1.Y, A2.Y))

        self.assertFalse(ibc(A1.X, A2.Y))
        self.assertFalse(ibc(A1.Y, A2.X))

    def test_callable_alternatives(self):
        def myCall(self, *args, **kwargs):
            return 42

        alt = Alternative("alts", One={}, Two={}, __call__=myCall)

        one = alt.One()
        self.assertEqual(one(), 42)

        two = alt.Two()
        self.assertEqual(two(), 42)

        alt = Alternative("alts", One={}, Two={}, myCall=myCall)
        with self.assertRaises(TypeError):
            one = alt.One()
            one()

        with self.assertRaises(TypeError):
            two = alt.Two()
            two()

    def test_object_bytecounts(self):
        self.assertEqual(_types.bytecount(NoneType), 0)
        self.assertEqual(_types.bytecount(Int8), 1)
        self.assertEqual(_types.bytecount(Int64), 8)

    def test_type_stringification(self):
        for t in ['Int8', 'NoneType']:
            self.assertEqual(str(getattr(_types, t)()), "<class '%s'>" % t)

    def test_tuple_of(self):
        tupleOfInt = TupleOf(int)
        i = tupleOfInt(())
        i = tupleOfInt((1, 2, 3))

        self.assertEqual(len(i), 3)
        self.assertEqual(tuple(i), (1, 2, 3))

        for x in range(10):
            self.assertEqual(
                tuple(tupleOfInt(tuple(range(x)))),
                tuple(range(x))
            )

        with self.assertRaisesRegex(AttributeError, "do not accept attributes"):
            tupleOfInt((1, 2, 3)).x = 2

    def test_one_of_alternative(self):
        X = Alternative("X", V={'a': int})
        Ox = OneOf(None, X)

        self.assertEqual(Ox(X.V(a=10)), X.V(a=10))

    def test_one_of_py_subclass(self):
        class X(NamedTuple(x=int)):
            def f(self):
                return self.x

        Ox = OneOf(None, X)

        self.assertEqual(NamedTuple(x=int)(x=10).x, 10)
        self.assertEqual(X(x=10).f(), 10)
        self.assertEqual(Ox(X(x=10)).f(), 10)

    def test_one_of_distinguishes_py_subclasses(self):
        class X(NamedTuple(x=int)):
            def f(self):
                return self.x

        class X2(NamedTuple(x=int)):
            def f(self):
                return self.x + 2

        XorX2 = OneOf(X, X2)

        self.assertTrue(isinstance(XorX2(X()), X))
        self.assertTrue(isinstance(XorX2(X2()), X2))

    def test_tuple_of_tuple_of(self):
        tupleOfInt = TupleOf(int)
        tupleOfTupleOfInt = TupleOf(tupleOfInt)

        pyVersion = (1, 2, 3), (1, 2, 3, 4)
        nativeVersion = tupleOfTupleOfInt(pyVersion)

        self.assertEqual(len(nativeVersion), 2)
        self.assertEqual(len(nativeVersion[0]), 3)
        self.assertEqual(tuple(tuple(x) for x in nativeVersion), pyVersion)

        bigTup = tupleOfInt(list(range(1000)))

        t0 = time.time()
        t = (bigTup, bigTup, bigTup, bigTup, bigTup)
        for i in range(1000000):
            tupleOfTupleOfInt(t)

        elapsed = time.time() - t0
        print("Took ", elapsed, " to do 1mm")
        self.check_expected_performance(elapsed)

    def test_default_initializer_oneof(self):
        x = OneOf(None, int)
        self.assertTrue(x() is None, repr(x()))

    def test_tuple_of_various_things(self):
        for thing, typeOfThing in [("hi", str), (b"somebytes", bytes),
                                   (1.0, float), (2, int),
                                   (None, type(None))
                                   ]:
            tupleType = TupleOf(typeOfThing)
            t = tupleType((thing,))
            self.assertTrue(type(t[0]) is typeOfThing)
            self.assertEqual(t[0], thing)

    def test_tuple_assign_fails(self):
        with self.assertRaisesRegex(TypeError, "does not support item assignment"):
            (1, 2, 3)[10] = 20
        with self.assertRaisesRegex(TypeError, "does not support item assignment"):
            TupleOf(int)((1, 2, 3))[10] = 20

    def test_list_of(self):
        L = ListOf(int)
        self.assertEqual(L.__qualname__, "ListOfType(Int64)")

        l1 = L([1, 2, 3, 4])

        self.assertEqual(l1[0], 1)
        self.assertEqual(l1[-1], 4)

        l1[0] = 10
        self.assertEqual(l1[0], 10)

        l1[-1] = 11
        self.assertEqual(l1[3], 11)

        with self.assertRaisesRegex(IndexError, "index out of range"):
            l1[100] = 20

        l2 = L((10, 2, 3, 11))

        self.assertEqual(l1, l2)
        self.assertNotEqual(l1, (10, 2, 3, 11))
        self.assertEqual(l1, [10, 2, 3, 11])

        self.assertEqual(str(l1), str([10, 2, 3, 11]))

        l3 = l1 + l2
        self.assertEqual(l3, [10, 2, 3, 11, 10, 2, 3, 11])

        l3.append(23)
        self.assertEqual(l3, [10, 2, 3, 11, 10, 2, 3, 11, 23])

    def test_list_resize(self):
        l1 = ListOf(TupleOf(int))()

        l1.resize(10)
        self.assertEqual(l1.reserved(), 10)
        self.assertEqual(len(l1), 10)

        emptyTup = TupleOf(int)()
        aTup = TupleOf(int)((1, 2, 3))

        self.assertEqual(list(l1), [emptyTup] * 10)
        l1.resize(20, aTup)
        self.assertEqual(list(l1), [emptyTup] * 10 + [aTup] * 10)

        self.assertEqual(_types.refcount(aTup), 11)

        self.assertEqual(l1.pop(15), aTup)
        self.assertEqual(l1.pop(5), emptyTup)

        self.assertEqual(_types.refcount(aTup), 10)

        l1.resize(15)

        with self.assertRaises(IndexError):
            l1.pop(100)

        self.assertEqual(_types.refcount(aTup), 7)  # 6 in the list because we popped at '5'

        l1.pop()

        self.assertEqual(_types.refcount(aTup), 6)

        # this pops one of the empty tuples
        l1.pop(-10)

        self.assertEqual(_types.refcount(aTup), 6)

        l1.clear()
        self.assertEqual(len(l1), 0)

    def test_one_of(self):
        o = OneOf(None, str)

        self.assertEqual(o("hi"), "hi")
        self.assertTrue(o(None) is None)

        o = OneOf(None, "hi", 1.5, 1, True, b"hi2")

        self.assertTrue(o(None) is None)
        self.assertTrue(o("hi") == "hi")
        self.assertTrue(o(b"hi2") == b"hi2")
        self.assertTrue(o(1.5) == 1.5)
        self.assertTrue(o(1) == 1)
        self.assertIs(o(True), True)

        with self.assertRaises(TypeError):
            o("hi2")
        with self.assertRaises(TypeError):
            o(b"hi")
        with self.assertRaises(TypeError):
            o(3)
        with self.assertRaises(TypeError):
            o(False)

    def test_ordering(self):
        o = OneOf(None, "hi", 1.5, 1, True, b"hi2")

        self.assertIs(o(True), True)

    def test_one_of_flattening(self):
        self.assertEqual(OneOf(OneOf(None, 1.0), OneOf(2.0, 3.0)), OneOf(None, 1.0, 2.0, 3.0))

    def test_one_of_order_matters(self):
        self.assertNotEqual(OneOf(1.0, 2.0), OneOf(2.0, 1.0))

    def test_type_filter(self):
        EvenInt = TypeFilter(int, lambda i: i % 2 == 0)

        self.assertTrue(isinstance(2, EvenInt))
        self.assertFalse(isinstance(1, EvenInt))
        self.assertFalse(isinstance(2.0, EvenInt))

        EvenIntegers = TupleOf(EvenInt)

        e = EvenIntegers(())
        e2 = e + (2, 4, 0)

        with self.assertRaises(TypeError):
            EvenIntegers((1,))

        with self.assertRaises(TypeError):
            e2 + (1,)

    def test_tuple_of_one_of_fixed_size(self):
        t = TupleOf(OneOf(0, 1, 2, 3, 4))

        ints = tuple([x % 5 for x in range(1000000)])

        typedInts = t(ints)

        self.assertEqual(len(serialize(t, typedInts)), len(ints) + 8)  # 4 bytes for size of list, 4 bytes for frame size
        self.assertEqual(tuple(typedInts), ints)

    def test_tuple_of_one_of_multi(self):
        t = TupleOf(OneOf(int, bool))

        someThings = tuple([100 + x % 5 if x % 17 != 0 else bool(x%19) for x in range(1000000)])

        typedThings = t(someThings)

        self.assertEqual(
            len(serialize(t, typedThings)),
            sum(2 if isinstance(t, bool) else 9 for t in someThings) + 8
        )

        self.assertEqual(tuple(typedThings), someThings)

    def test_compound_oneof(self):
        producer = RandomValueProducer()
        producer.addEvenly(1000, 2)

        for _ in range(1000):
            vals = (producer.pickRandomly(), producer.pickRandomly(), producer.pickRandomly())

            a = OneOf(vals[0], vals[1], type(vals[2]))

            for v in vals:
                self.assertEqual(a(v), v, (a(v), v))

            tup = TupleOf(a)
            tupInst = tup(vals)

            for i in range(len(vals)):
                self.assertEqual(tupInst[i], vals[i], vals)

    def test_one_of_conversion_failure(self):
        o = OneOf(None, str)

        with self.assertRaises(TypeError):
            o(b"bytes")

    def test_one_of_in_tuple(self):
        t = Tuple(OneOf(None, str), str)

        self.assertEqual(t(("hi", "hi2"))[0], "hi")
        self.assertEqual(t(("hi", "hi2"))[1], "hi2")
        self.assertEqual(t((None, "hi2"))[1], "hi2")
        self.assertEqual(t((None, "hi2"))[0], None)
        with self.assertRaises(TypeError):
            t((None, None))
        with self.assertRaises(IndexError):
            t((None, "hi2"))[2]

    def test_one_of_composite(self):
        t = OneOf(TupleOf(str), TupleOf(float))

        self.assertIsInstance(t((1.0, 2.0)), TupleOf(float))
        self.assertIsInstance(t(("1.0", "2.0")), TupleOf(str))

        with self.assertRaises(TypeError):
            t((1.0, "2.0"))

    def test_named_tuple(self):
        t = NamedTuple(a=int, b=int)

        with self.assertRaisesRegex(AttributeError, "object has no attribute"):
            t().asdf

        with self.assertRaisesRegex(AttributeError, "immutable"):
            t().a = 1

        self.assertEqual(t()[0], 0)
        self.assertEqual(t().a, 0)
        self.assertEqual(t()[1], 0)

        self.assertEqual(t(a=1, b=2).a, 1)
        self.assertEqual(t(a=1, b=2).b, 2)

    def test_named_tuple_construction(self):
        t = NamedTuple(a=int, b=int)

        self.assertEqual(t(a=10).a, 10)
        self.assertEqual(t(a=10).b, 0)
        self.assertEqual(t(a=10, b=2).a, 10)
        self.assertEqual(t(a=10, b=2).b, 2)
        self.assertEqual(t({'a': 10, 'b': 2}).a, 10)
        self.assertEqual(t({'a': 10, 'b': 2}).b, 2)

        self.assertEqual(t({'b': 2}).a, 0)
        self.assertEqual(t({'b': 2}).b, 2)

        with self.assertRaises(TypeError):
            t({'c': 10})
        with self.assertRaises(TypeError):
            t(c=10)

    def test_named_tuple_str(self):
        t = NamedTuple(a=str, b=str)

        self.assertEqual(t(a='1', b='2').a, '1')
        self.assertEqual(t(a='1', b='2').b, '2')

        self.assertEqual(t(b='2').a, '')
        self.assertEqual(t(b='2').b, '2')
        self.assertEqual(t().a, '')
        self.assertEqual(t().b, '')

    def test_tuple_of_string_perf(self):
        t = NamedTuple(a=str, b=str)

        t0 = time.time()
        for i in range(1000000):
            t(a="a", b="b").a

        elapsed = time.time() - t0
        print("Took ", elapsed, " to do 1mm")
        self.check_expected_performance(elapsed)

    def test_comparisons_in_one_of(self):
        t = OneOf(None, float)

        def map(x):
            if x is None:
                return -1000000.0
            else:
                return x

        lt = lambda a, b: map(a) < map(b)
        le = lambda a, b: map(a) <= map(b)
        eq = lambda a, b: map(a) == map(b)
        ne = lambda a, b: map(a) != map(b)
        gt = lambda a, b: map(a) > map(b)
        ge = lambda a, b: map(a) >= map(b)

        funcs = [lt, le, eq, ne, gt, ge]
        ts = [None, 1.0, 2.0, 3.0]

        for f in funcs:
            for t1 in ts:
                for t2 in ts:
                    self.assertTrue(f(t1, t2) is f(t(t1), t(t2)))

    def test_comparisons_equivalence(self):
        t = TupleOf(OneOf(None, str, bytes, float, int, bool, TupleOf(int)),)

        def lt(a, b):
            return a < b

        def le(a, b):
            return a <= b

        def eq(a, b):
            return a == b

        def ne(a, b):
            return a != b

        def gt(a, b):
            return a > b

        def ge(a, b):
            return a >= b

        funcs = [lt, le, eq, ne, gt, ge]

        tgroups = [
            [1.0, 2.0, 3.0],
            [1, 2, 3],
            [True, False],
            ["a", "b", "ab", "bb", "ba", "aaaaaaa", "", "asdf"],
            ["1", "2", "3", "12", "13", "23", "24", "123123", "0", ""],
            [b"a", b"b", b"ab", b"bb", b"ba", b"aaaaaaa", b"", b"asdf"],
            [(1, 2), (1, 2, 3), (), (1, 1), (1,)]
        ]

        for ts in tgroups:
            for f in funcs:
                for t1 in ts:
                    for t2 in ts:
                        self.assertTrue(
                            f(t1, t2) is f(t((t1,)), t((t2,))),
                            (f, t1, t2, f(t1, t2), f(t((t1,)), t((t2,))))
                        )

    def test_const_dict(self):
        t = ConstDict(str, str)

        self.assertEqual(len(t()), 0)
        self.assertEqual(len(t({})), 0)
        self.assertEqual(len(t({'a': 'b'})), 1)
        self.assertEqual(t({'a': 'b'})['a'], 'b')
        self.assertEqual(t({'a': 'b', 'b': 'c'})['b'], 'c')

        self.assertTrue("a" in deserialize(t, serialize(t, t({'a': 'b'}))))

        self.assertTrue("a" in deserialize(t, serialize(t, t({'a': 'b', 'b': 'c'}))))
        self.assertTrue("a" in deserialize(t, serialize(t, t({'a': 'b', 'b': 'c', 'c': 'd'}))))
        self.assertTrue("a" in deserialize(t, serialize(t, t({'a': 'b', 'b': 'c', 'c': 'd', 'd': 'e'}))))
        self.assertTrue("c" in deserialize(t, serialize(t, t({'a': 'b', 'b': 'c', 'c': 'd', 'def': 'e'}))))
        self.assertTrue("def" in deserialize(t, serialize(t, t({'a': 'b', 'b': 'c', 'c': 'd', 'def': 'e'}))))

    def test_const_dict_get(self):
        a = ConstDict(str, str)({'a': 'b', 'c': 'd'})

        self.assertEqual(a.get('a'), 'b')
        self.assertEqual(a.get('asdf'), None)
        self.assertEqual(a.get('asdf', 20), 20)

    def test_const_dict_items_keys_and_values(self):
        a = ConstDict(str, str)({'a': 'b', 'c': 'd'})

        self.assertEqual(sorted(a.items()), [('a', 'b'), ('c', 'd')])
        self.assertEqual(sorted(a.keys()), ['a', 'c'])
        self.assertEqual(sorted(a.values()), ['b', 'd'])

    def test_empty_string(self):
        a = ConstDict(str, str)({'a': ''})

        print(a['a'])

    def test_dict_to_oneof(self):
        t = ConstDict(str, OneOf("A", "B", "ABCDEF"))
        a = t({'a': 'A', 'b': 'ABCDEF'})

        self.assertEqual(a['a'], "A")
        self.assertEqual(a['b'], "ABCDEF")

        self.assertEqual(a, deserialize(t, serialize(t, a)))

    def test_deserialize_primitive(self):
        x = deserialize(str, serialize(str, "a"))
        self.assertTrue(isinstance(x, str))

    def test_dict_containment(self):
        for _ in range(100):
            producer = RandomValueProducer()
            producer.addEvenly(20, 2)

            values = producer.all()

            for v in values:
                if str(type(v))[:17] == "<class 'ConstDict":
                    v = deserialize(type(v), serialize(type(v), v))
                    for k in v:
                        self.assertTrue(k in v)

    def test_named_tuple_from_dict(self):
        N = NamedTuple(x=int, y=str, z=OneOf(None, "hihi"))
        self.assertEqual(N().x, 0)
        self.assertEqual(N().y, "")
        self.assertEqual(N().z, None)

        self.assertEqual(N({}).x, 0)
        self.assertEqual(N({}).y, "")
        self.assertEqual(N({}).z, None)

        self.assertEqual(N({'x': 20}).x, 20)
        self.assertEqual(N({'x': 20, 'y': "30"}).y, "30")
        self.assertEqual(N({'y': "30", 'x': 20}).y, "30")
        self.assertEqual(N({'z': "hihi"}).z, "hihi")

        with self.assertRaises(Exception):
            N({'r': 'hi'})
            N({'y': 'hi', 'z': "not hihi"})
            N({'a': 0, 'b': 0, 'c': 0, 'd': 0})

    def test_const_dict_mixed(self):
        t = ConstDict(str, int)
        self.assertTrue(t({"a": 10})["a"] == 10)

        t = ConstDict(int, str)
        self.assertTrue(t({10: "a"})[10] == "a")

    def test_const_dict_comparison(self):
        t = ConstDict(str, str)

        self.assertEqual(t({'a': 'b'}), t({'a': 'b'}))
        self.assertLess(t({}), t({'a': 'b'}))

    def test_const_dict_lookup(self):
        for type_to_use, vals in [(int, list(range(20))),
                                  (bytes, [b'1', b'2', b'3', b'4', b'5'])]:
            t = ConstDict(type_to_use, type_to_use)

            for _ in range(10):
                ks = list(vals)
                vs = list(vals)

                numpy.random.shuffle(ks)
                numpy.random.shuffle(vs)

                py_d = {}
                for i in range(len(ks)):
                    py_d[ks[i]] = vs[i]

                typed_d = t(py_d)

                for k in py_d:
                    self.assertEqual(py_d[k], typed_d[k])

                last_k = None
                for k in typed_d:
                    assert last_k is None or k > last_k, (k, last_k)
                    last_k = k

    def test_const_dict_lookup_time(self):
        int_dict = ConstDict(int, int)

        d = int_dict({k: k for k in range(1000000)})

        for k in range(1000000):
            self.assertTrue(k in d)
            self.assertTrue(d[k] == k)

    def test_const_dict_of_dict(self):
        int_dict = ConstDict(int, int)
        int_dict_2 = ConstDict(int_dict, int_dict)

        d = int_dict({1: 2})
        d2 = int_dict({1: 2, 3: 4})

        big = int_dict_2({d: d2})

        self.assertTrue(d in big)
        self.assertTrue(d2 not in big)
        self.assertTrue(big[d] == d2)

    def test_dict_hash_perf(self):
        str_dict = ConstDict(str, str)

        s = str_dict({'a' * 1000000: 'b' * 1000000})

        t0 = time.time()
        for k in range(1000000):
            hash(s)

        elapsed = time.time() - t0
        print(elapsed, " to do 1mm")
        self.check_expected_performance(elapsed)

    def test_const_dict_str_perf(self):
        t = ConstDict(str, str)

        t0 = time.time()
        for i in range(100000):
            t({str(k): str(k+1) for k in range(10)})

        elapsed = time.time() - t0
        print("Took ", elapsed, " to do 1mm")
        self.check_expected_performance(elapsed)

    def test_const_dict_int_perf(self):
        t = ConstDict(int, int)

        t0 = time.time()
        for i in range(100000):
            t({k: k+1 for k in range(10)})

        elapsed = time.time() - t0
        print("Took ", elapsed, " to do 1mm")
        self.check_expected_performance(elapsed)

    def test_const_dict_iter_int(self):
        t = ConstDict(int, int)

        aDict = t({k: k+1 for k in range(100)})
        for k in aDict:
            self.assertEqual(aDict[k], k+1)

    def test_const_dict_iter_str(self):
        t = ConstDict(str, str)

        aDict = t({str(k): str(k+1) for k in range(100)})
        for k in aDict:
            self.assertEqual(aDict[str(k)], str(int(k)+1))

    def test_alternatives_with_Bytes(self):
        alt = Alternative(
            "Alt",
            x_0={'a': bytes}
        )
        self.assertEqual(alt.x_0(a=b''), alt.x_0(a=b''))

    def test_alternatives_with_str_func(self):
        alt = Alternative(
            "Alt",
            x_0={'a': bytes},
            f=lambda self: 1,
            __str__=lambda self: "not_your_usual_str"
        )

        self.assertEqual(alt.x_0().f(), 1)
        self.assertEqual(str(alt.x_0()), "not_your_usual_str")

    def test_named_tuple_subclass_magic_methods(self):
        class X(NamedTuple(x=int, y=int)):
            def __str__(self):
                return "str override"

            def __repr__(self):
                return "repr override"

        self.assertEqual(repr(X()), "repr override")
        self.assertEqual(str(X()), "str override")

    def test_empty_alternatives(self):
        a = Alternative(
            "Alt",
            A={},
            B={}
        )

        self.assertEqual(a.A(), a.A())
        self.assertIsInstance(deserialize(a, serialize(a, a.A())), a.A)
        self.assertEqual(a.A(), deserialize(a, serialize(a, a.A())))

        self.assertEqual(a.B(), a.B())
        self.assertNotEqual(a.A(), a.B())
        self.assertNotEqual(a.B(), a.A())

    def test_extracted_alternatives_have_correct_type(self):
        Alt = Alternative(
            "Alt",
            A={},
            B={}
        )
        tOfAlt = TupleOf(Alt)

        a = Alt.A()
        aTup = tOfAlt((a,))

        self.assertEqual(a, aTup[0])
        self.assertTrue(type(a) is type(aTup[0]))  # noqa: F721

    def test_alternatives(self):
        alt = Alternative(
            "Alt",
            child_ints={'x': int, 'y': int},
            child_strings={'x': str, 'y': str}
        )

        self.assertTrue(issubclass(alt.child_ints, alt))
        self.assertTrue(issubclass(alt.child_strings, alt))

        a = alt.child_ints(x=10, y=20)
        a2 = alt.child_ints(x=10, y=20)

        self.assertEqual(a, a2)

        self.assertTrue(isinstance(a, alt))
        self.assertTrue(isinstance(a, alt.child_ints))

        self.assertEqual(a.x, 10)
        self.assertEqual(a.y, 20)
        self.assertTrue(a.matches.child_ints)
        self.assertFalse(a.matches.child_strings)

        with self.assertRaisesRegex(AttributeError, "immutable"):
            a.x = 20

    def test_alternatives_comparison(self):
        empty = Alternative("X", A={}, B={})

        self.assertEqual(empty.A(), empty.A())
        self.assertEqual(empty.B(), empty.B())
        self.assertNotEqual(empty.A(), empty.B())

        a = Alternative(
            "X",
            A={'a': int},
            B={'b': int},
            C={'c': str},
            D={'d': bytes},
        )

        self.assertEqual(a.A(a=10), a.A(a=10))
        self.assertNotEqual(a.A(a=10), a.A(a=11))

        self.assertNotEqual(a.C(c=""), a.C(c="hi"))
        self.assertFalse(a.C(c="") == a.C(c="hi"))
        self.assertNotEqual(a.D(d=b""), a.D(d=b"hi"))

    def test_alternatives_add_operator(self):
        alt = Alternative(
            "Alt",
            child_ints={'x': int, 'y': int},
            __add__=lambda lhs, rhs: (lhs, rhs)
        )

        a = alt.child_ints(x=0, y=2)

        self.assertEqual(a+a, (a, a))

    def test_alternatives_perf(self):
        alt = Alternative(
            "Alt",
            child_ints={'x': int, 'y': int},
            child_strings={'x': str, 'y': str}
        )

        t0 = time.time()

        for i in range(1000000):
            a = alt.child_ints(x=10, y=20)
            a.matches.child_ints
            a.x

        elapsed = time.time() - t0
        print("Took ", elapsed, " to do 1mm")
        self.check_expected_performance(elapsed, expected=2.0)

    def test_object_hashing_and_equality(self):
        for _ in range(100):
            producer = RandomValueProducer()
            producer.addEvenly(20, 2)

            values = producer.all()

            for v1 in values:
                for v2 in values:
                    if hash(v1) != hash(v2) and v1 == v2:
                        print(v1, v2, type(v1), type(v2))

            for v1 in values:
                for v2 in values:
                    if type(v1) == type(v2) and v1 == v2:
                        self.assertEqual(hash(v1), hash(v2), (v1, v2))
                        if type(v1) is type(v2):
                            self.assertEqual(repr(v1), repr(v2), (v1, v2, type(v1), type(v2)))

            values = sorted([makeTuple(v) for v in values])

            for i in range(len(values)-1):
                self.assertTrue(values[i] <= values[i+1])
                self.assertTrue(values[i+1] >= values[i])

    def test_bytes_repr(self):
        for _ in range(100000):
            # always start with a '"' because otherwise python keeps chosing different
            # initial characters.
            someBytes = b'"' + numpy.random.uniform(size=2).tostring()
            self.assertEqual(repr(makeTuple(someBytes)), repr((someBytes,)))

    def test_equality_with_native_python_objects(self):
        tups = [(1, 2, 3), (), ("2",), (b"2",), (1, 2, 3, "b"), (2,), (None,)]

        for tup1 in tups:
            self.assertEqual( makeTuple(*tup1), tup1 )

            for tup2 in tups:
                if tup1 != tup2:
                    self.assertNotEqual( makeTuple(*tup1), tup2 )

        for tup1 in tups:
            self.assertEqual( makeTupleOf(*tup1), tup1 )

            for tup2 in tups:
                if tup1 != tup2:
                    self.assertNotEqual( makeTupleOf(*tup1), tup2 )

    def test_add_tuple_of(self):
        tupleOfInt = TupleOf(int)

        tups = [(), (1, 2), (1,), (1, 2, 3, 4)]

        for tup1 in tups:
            for tup2 in tups:
                self.assertEqual(tupleOfInt(tup1) + tupleOfInt(tup2), tupleOfInt(tup1+tup2))
                self.assertEqual(tupleOfInt(tup1) + tup2, tupleOfInt(tup1+tup2))

    def test_slice_tuple_of(self):
        tupleOfInt = TupleOf(int)

        ints = tuple(range(20))
        aTuple = tupleOfInt(ints)

        for i in range(-21, 21):
            for i2 in range(-21, 21):
                for step in range(-3, 3):
                    if step != 0:
                        self.assertEqual(aTuple[i:i2:step], ints[i:i2:step])

            try:
                ints[i]
                self.assertEqual(aTuple[i], ints[i])
            except IndexError:
                with self.assertRaises(IndexError):
                    aTuple[i]

    def test_dictionary_subtraction_basic(self):
        intDict = ConstDict(int, int)

        self.assertEqual(intDict({1: 2}) - (1,), intDict({}))
        self.assertEqual(intDict({1: 2, 3: 4}) - (1,), intDict({3: 4}))
        self.assertEqual(intDict({1: 2, 3: 4}) - (3,), intDict({1: 2}))

    def test_dictionary_addition_and_subtraction(self):
        someDicts = [{i: choice([1, 2, 3, 4, 5]) for i in range(choice([4, 6, 10, 20]))} for _ in range(20)]
        intDict = ConstDict(int, int)

        for d1 in someDicts:
            for d2 in someDicts:
                addResult = dict(d1)
                addResult.update(d2)

                self.assertEqual(intDict(d1) + intDict(d2), intDict(addResult))

                res = intDict(addResult)

                while len(res):
                    toRemove = []

                    for i in range(choice(list(range(len(res))))+1):
                        key = choice(list(addResult))
                        del addResult[key]
                        toRemove.append(key)

                    res = res - toRemove

                    self.assertEqual(res, intDict(addResult))

    def test_subclassing(self):
        BaseTuple = NamedTuple(x=int, y=float)

        class NTSubclass(BaseTuple):
            def f(self):
                return self.x + self.y

            def __repr__(self):
                return "ASDF"

        inst = NTSubclass(x=10, y=20)

        self.assertTrue(isinstance(inst, BaseTuple))
        self.assertTrue(isinstance(inst, NTSubclass))
        self.assertTrue(type(inst) is NTSubclass)

        self.assertEqual(repr(inst), "ASDF")
        self.assertNotEqual(BaseTuple.__repr__(inst), "ASDF")

        self.assertEqual(inst.x, 10)
        self.assertEqual(inst.f(), 30)

        TupleOfSubclass = TupleOf(NTSubclass)

        instTup = TupleOfSubclass((inst, BaseTuple(x=20, y=20.0)))

        self.assertTrue(isinstance(instTup[0], NTSubclass))
        self.assertTrue(isinstance(instTup[1], NTSubclass))
        self.assertEqual(instTup[0].f(), 30)
        self.assertEqual(instTup[1].f(), 40)

        self.assertEqual(BaseTuple(inst).x, 10)

        self.assertTrue(OneOf(None, NTSubclass)(None) is None)
        self.assertTrue(OneOf(None, NTSubclass)(inst) == inst)

    def test_serialization(self):
        ints = TupleOf(int)((1, 2, 3, 4))

        self.assertEqual(
            len(serialize(TupleOf(int), ints)),
            40
        )

        while len(ints) < 1000000:
            ints = ints + ints
            t0 = time.time()
            self.assertEqual(len(serialize(TupleOf(int), ints)), len(ints) * 8 + 8)
            print(time.time() - t0, " for ", len(ints))

    def test_serialization_roundtrip(self):
        for _ in range(100):
            producer = RandomValueProducer()
            producer.addEvenly(30, 3)

            values = producer.all()
            for v in values:
                ser = serialize(type(v), v)
                v2 = deserialize(type(v), ser)
                ser2 = serialize(type(v), v2)

                self.assertTrue(type(v2) is type(v))
                self.assertEqual(ser, ser2)
                self.assertEqual(str(v), str(v2))
                self.assertEqual(v, v2)

    def test_serialize_doesnt_leak(self):
        T = TupleOf(int)

        def getMem():
            return psutil.Process().memory_info().rss / 1024 ** 2

        m0 = getMem()

        for passIx in range(100):
            for i in range(1000):
                t = T(list(range(i)))
                deserialize(T, serialize(T, t))

            self.assertTrue(getMem() < m0 + 100)

    def test_const_dict_of_tuple(self):
        K = NamedTuple(a=OneOf(float, int), b=OneOf(float, int))
        someKs = [K(a=0, b=0), K(a=1), K(a=10), K(b=10), K()]

        T = ConstDict(K, K)

        indexDict = {}
        x = T()

        numpy.random.seed(42)

        for _ in range(100):
            i1 = numpy.random.choice(len(someKs))
            i2 = numpy.random.choice(len(someKs))
            add = numpy.random.choice([False, True])

            if add:
                indexDict[i1] = i2
                x = x + {someKs[i1]: someKs[i2]}
            else:
                if i1 in indexDict:
                    del indexDict[i1]
                    x = x - (someKs[i1],)

            self.assertEqual(x, T({someKs[i]: someKs[v] for i, v in indexDict.items()}))
            for k in x:
                self.assertTrue(k in x)
                x[k]

    def test_conversion_of_binary_compatible(self):
        class T1(NamedTuple(a=int)):
            pass

        class T2(NamedTuple(a=int)):
            pass

        class T1Comp(NamedTuple(d=ConstDict(str, T1))):
            pass

        class T2Comp(NamedTuple(d=ConstDict(str, T1))):
            pass

        self.assertTrue(_types.isBinaryCompatible(T1Comp, T2Comp))
        self.assertTrue(_types.isBinaryCompatible(T1, T2))

    def test_binary_compatible_nested(self):
        def make():
            class Interior(NamedTuple(a=int)):
                pass

            class Exterior(NamedTuple(a=Interior)):
                pass

            return Exterior

        E1 = make()
        E2 = make()

        self.assertTrue(_types.isBinaryCompatible(E1, E2))

    def test_python_objects_in_tuples(self):
        class NormalPyClass(object):
            pass

        class NormalPySubclass(NormalPyClass):
            pass

        NT = NamedTuple(x=NormalPyClass, y=NormalPySubclass)

        nt = NT(x=NormalPyClass(), y=NormalPySubclass())
        self.assertIsInstance(nt.x, NormalPyClass)
        self.assertIsInstance(nt.y, NormalPySubclass)

    def test_construct_alternatives_with_positional_arguments(self):
        a = Alternative("A", HasOne={'a': str}, HasTwo={'a': str, 'b': str})

        with self.assertRaises(TypeError):
            a.HasTwo("hi")

        self.assertEqual(a.HasOne("hi"), a.HasOne(a="hi"))

        hasOne = a.HasOne("hi")
        self.assertEqual(a.HasOne(hasOne), hasOne)

        with self.assertRaises(TypeError):
            a.HasOne(a.HasTwo(a='1', b='b'))

    def test_recursive_classes_repr(self):
        class ASelfRecursiveClass(Class):
            x = Member(OneOf(None, lambda: ASelfRecursiveClass))

        a = ASelfRecursiveClass()
        a.x = a

        b = ASelfRecursiveClass()
        b.x = b

        print(repr(a))

    def test_unsafe_pointers_to_list_internals(self):
        x = ListOf(int)()
        x.resize(100)
        for i in range(len(x)):
            x[i] = i

        aPointer = x.pointerUnsafe(0)
        self.assertTrue(str(aPointer).startswith("(Int64*)0x"))

        self.assertEqual(aPointer.get(), x[0])
        aPointer.set(100)
        self.assertEqual(aPointer.get(), 100)
        self.assertEqual(x[0], 100)

        aPointer = aPointer + 10

        self.assertEqual(aPointer.get(), x[10])
        self.assertEqual(aPointer[10], x[20])
        aPointer.set(20)
        self.assertEqual(aPointer.get(), 20)
        self.assertEqual(x[10], 20)

        # this is OK because ints are POD.
        aPointer.initialize(30)
        self.assertEqual(x[10], 30)

    def test_unsafe_pointers_to_uninitialized_list_items(self):
        # because this is testing unsafe operations, the test is
        # really just that we don't segfault!
        for _ in range(100):
            x = ListOf(TupleOf(int))()
            x.reserve(10)

            for i in range(x.reserved()):
                x.pointerUnsafe(i).initialize((i,))

            x.setSizeUnsafe(10)

        # now check that if we fail to set the size we'll leak the tuple
        aLeakedTuple = TupleOf(int)((1, 2, 3))
        x = ListOf(TupleOf(int))()
        x.reserve(1)
        x.pointerUnsafe(0).initialize(aLeakedTuple)
        x = None

        self.assertEqual(_types.refcount(aLeakedTuple), 2)

    def test_list_copy_operation_duplicates_list(self):
        T = ListOf(int)

        x = T([1, 2, 3])
        y = T(x)

        x[0] = 100

        self.assertNotEqual(y[0], 100)

    def test_list_and_tuple_conversion_to_numpy(self):
        for T in [ListOf(bool), TupleOf(bool)]:
            for arr in [
                    numpy.array([]),
                    numpy.array([0, 1, 2, 3, 4, 5]),
                    numpy.array([0, 1, 2, 3, 4, 5], 'int32'),
                    numpy.array([0, 1, 2, 3, 4, 5], 'int16'),
                    numpy.array([0, 1, 2, 3, 4, 5], 'bool')
            ]:
                self.assertEqual(T(arr), T(arr.tolist()))
                self.assertEqual(T(arr).toArray().tolist(), [bool(x) for x in arr.tolist()])

        for T in [ListOf(int), TupleOf(int)]:
            for arr in [
                    numpy.array([]),
                    numpy.array([1, 2, 3, 4, 5]),
                    numpy.array([1, 2, 3, 4, 5], 'int32'),
                    numpy.array([1, 2, 3, 4, 5], 'int16')
            ]:
                self.assertEqual(T(arr), T(arr.tolist()))
                self.assertEqual(T(arr).toArray().tolist(), arr.tolist())

        for T in [ListOf(float), TupleOf(float)]:
            for arr in [
                    numpy.array([]),
                    numpy.array([1, 2, 3, 4, 5], 'float64'),
                    numpy.array([1, 2, 3, 4, 5], 'float32')
            ]:
                self.assertEqual(T(arr), T(arr.tolist()))
                self.assertEqual(T(arr).toArray().tolist(), arr.tolist())

    def test_list_of_equality(self):
        x = ListOf(int)([1, 2, 3, 4])
        y = ListOf(int)([1, 2, 3, 5])

        self.assertEqual(x, x)
        self.assertNotEqual(x, y)

    def test_tuple_r_add(self):
        self.assertEqual(
            (1, 2, 4, 5, 6) + TupleOf(int)([1, 2]),
            (1, 2, 4, 5, 6, 1, 2)
        )

        self.assertEqual(
            [1, 2, 4, 5, 6] + TupleOf(int)([1, 2]),
            (1, 2, 4, 5, 6, 1, 2)
        )

        with self.assertRaises(TypeError):
            [1, 2, "hi", 5, 6] + TupleOf(int)([1, 2])

    def test_tuple_r_cmp(self):
        self.assertEqual(
            (1, 2, 3), TupleOf(int)([1, 2, 3])
        )

    def test_other_bitness_types(self):
        # verify we can cast around non-64-bit values in a way that matches numpy
        typeAndNumpyType = [
            (Bool, numpy.bool),
            (Int8, numpy.int8),
            (Int16, numpy.int16),
            (Int32, numpy.int32),
            (Int64, numpy.int64),
            (UInt8, numpy.uint8),
            (UInt16, numpy.uint16),
            (UInt32, numpy.uint32),
            (UInt64, numpy.uint64),
            (Float32, numpy.float32),
            (Float64, numpy.float64)
        ]

        for ourType, numpyType in typeAndNumpyType:
            for candValue in [-1, 0, 1, 10, 100, 1000, 100000, 10000000, 10000000000]:
                self.assertEqual(int(ourType(candValue)), int(numpyType(candValue)), (ourType, candValue))
                self.assertEqual(float(ourType(candValue)), float(numpyType(candValue)), (ourType, candValue))

            for ourType2, numpyType2 in typeAndNumpyType:
                zeroOrTwoFloatTypes = sum([1 if 'float' in str(t) else 0 for t in [numpyType, numpyType2]]) in [0, 2]

                if zeroOrTwoFloatTypes:
                    for candValue in [-1, 0, 1, 10, 100, 1000, 100000, 10000000, 10000000000]:
                        self.assertEqual(
                            int(ourType(ourType2(candValue))),
                            int(numpyType(numpyType2(candValue))),
                            (ourType, ourType2, candValue)
                        )
                        self.assertEqual(
                            float(ourType(ourType2(candValue))),
                            float(numpyType(numpyType2(candValue))),
                            (ourType, ourType2, candValue)
                        )
                else:
                    # we convert from float to int as c++, which is different than numpy, which clips
                    # floats in a bizarre way. e.g.
                    #  numpy.int16(numpy.int64(numpy.float64(10000000000)))
                    # is not
                    #  numpy.int16(numpy.float64(10000000000))
                    pass

    def test_other_bitness_types_operators(self):

        def add(x, y):
            return x+y

        def div(x, y):
            return x/y

        def mul(x, y):
            return x*y

        def sub(x, y):
            return x-y

        def bitand(x, y):
            return x&y

        def bitor(x, y):
            return x|y

        def bitxor(x, y):
            return x^y

        otherTypes = [Bool, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64]
        for t1 in otherTypes:
            for t2 in otherTypes:
                for op in [add, mul, div, sub, bitand, bitor, bitxor]:
                    if not ((t1.IsFloat or t2.IsFloat) and op in (bitand, bitor, bitxor)):
                        res = op(t1(10), t2(10))
                        resType = type(res)
                        resType = {bool: Bool, int: Int64, float: Float64}.get(resType, resType)

                        if t1.IsFloat and t2.IsFloat:
                            self.assertTrue(resType.IsFloat)
                            self.assertEqual(resType.Bits, max(t1.Bits, t2.Bits))
                            self.assertEqual(res, op(10, 10))
                        elif t1.IsFloat or t2.IsFloat:
                            self.assertTrue(resType.IsFloat)
                            self.assertEqual(resType.Bits, t1.Bits if t1.IsFloat else t2.Bits)
                            if t1.Bits > 1 and t2.Bits > 1:
                                self.assertEqual(res, op(10, 10))
                        elif t1 is Bool and t2 is Bool:
                            self.assertEqual(resType, Bool if op in (bitor, bitand, bitxor) else Int64 if op is not div else Float64)
                        else:
                            self.assertEqual(resType.Bits, max(t1.Bits, t2.Bits))

                            if op is not div:
                                self.assertEqual(resType.IsSignedInt, t1.IsSignedInt or t2.IsSignedInt)

                            if t1.Bits > 1 and t2.Bits > 1:
                                self.assertEqual(res, op(10, 10))

    def test_comparing_arbitrary_objects(self):
        x = TupleOf(object)(["a"])
        y = TupleOf(object)([1])

        with self.assertRaises(TypeError):
            x < y

        self.assertEqual(x, x)
        self.assertEqual(y, y)
        self.assertNotEqual(x, y)

    def test_list_of_indexing_with_numpy_ints(self):
        x = ListOf(ListOf(int))([[1, 2, 3], [4, 5, 6]])
        self.assertEqual(x[numpy.int64(0)][numpy.int64(0)], 1)

    def test_error_message_on_bad_dispatch(self):
        @Function
        def f(x: int):
            return x

        with self.assertRaisesRegex(TypeError, "str"):
            f("hi")

        with self.assertRaisesRegex(TypeError, "argname="):
            f(argname=1)

    def test_named_tuple_comparison(self):
        N = NamedTuple(x=OneOf(None, int), y=OneOf(None, int))

        class S(N):
            pass

        self.assertEqual(N(x=1, y=2), N(x=1, y=2))
        self.assertNotEqual(N(x=1, y=2), N(x=1, y=3))
        self.assertFalse(N(x=1, y=2) == N(x=1, y=3))

        self.assertEqual(S(x=1, y=2), S(x=1, y=2))
        self.assertNotEqual(S(x=1, y=2), S(x=1, y=3))
        self.assertFalse(S(x=1, y=2) == S(x=1, y=3))

    def test_const_dict_comparison_more(self):
        N = NamedTuple(x=OneOf(None, int), y=OneOf(None, int))
        D = ConstDict(str, N)

        n1 = N(x=1, y=2)
        n2 = N(x=1, y=3)

        self.assertEqual(D({'a': n1}), D({'a': n1}))
        self.assertNotEqual(D({'a': n1}), D({'a': n2}))

    def test_mutable_dict(self):
        T = Dict(int, int)

        d = T()

        self.assertEqual(len(d), 0)

        with self.assertRaises(KeyError):
            d[0]

        d[0] = 10

        self.assertEqual(len(d), 1)
        self.assertEqual(d[0], 10)

        d[0] = 20

        self.assertEqual(len(d), 1)
        self.assertEqual(d[0], 20)

        for i in range(2000):
            d[i] = i

            if i % 100 == 0 and i:
                for x in range(len(d)):
                    self.assertEqual(d[x], x)

        for i in range(2000):
            if i % 100 == 0 and i:
                for x in range(i):
                    assert x not in d

                for x in range(i, 2000):
                    self.assertEqual(d[x], x)

            del d[i]

        # verify that adding and removing elements doesn't leak memory
        usage = currentMemUsageMb()
        for i in range(1000000):
            d[0] = i
            del d[0]
        self.assertLess(currentMemUsageMb(), usage+1)

    def test_mutable_dict_fuzz(self):
        native_d = Dict(int, int)()
        py_d = {}

        for dictSize in [10, 100, 1000, 10000]:
            for i in range(100000):
                z = numpy.random.choice(dictSize)

                self.assertEqual(z in py_d, z in native_d)

                if i % 3 == 0 or (i % 1000) > 900:
                    if z in py_d:
                        del py_d[z]
                        del native_d[z]
                else:
                    py_d[z] = i
                    native_d[z] = i

            for i in range(dictSize):
                self.assertEqual(z in py_d, z in native_d)

    def test_mutable_dict_refcounts(self):
        native_d = Dict(str, ListOf(int))()
        i = ListOf(int)()

        native_d["a"] = i

        self.assertEqual(_types.refcount(i), 2)

        native_d["b"] = i

        self.assertEqual(_types.refcount(i), 3)

        del native_d["a"]

        self.assertEqual(_types.refcount(i), 2)

        native_d["b"] = ListOf(int)()

        self.assertEqual(_types.refcount(i), 1)

        native_d["b"] = i

        self.assertEqual(_types.refcount(i), 2)

        native_d = None

        self.assertEqual(_types.refcount(i), 1)

    def test_mutable_dict_create_many(self):
        for ct in range(100):
            d = Dict(int, int)()
            for i in range(ct):
                d[i] = i + 1

    def test_mutable_dict_methods(self):
        d = Dict(int, int)({i: i+1 for i in range(10)})

        self.assertEqual(list(d.keys()), list(range(10)))
        self.assertEqual(list(d.values()), list(range(1, 11)))
        self.assertEqual(list(d.items()), [(i, i+1) for i in range(10)])

        for i in range(10):
            self.assertEqual(d.get(i), i+1)
            self.assertEqual(d.get(i, None), i+1)

        self.assertEqual(d.get(1000), None)
        self.assertEqual(d.get(1000, 123), 123)

        with self.assertRaises(TypeError):
            self.assertEqual(d.get("1000"), None)

    def test_mutable_dict_iteration_order(self):
        d = Dict(int, int)()

        d[10] = 10
        d[1] = 1
        d[2] = 2

        self.assertEqual(list(d), [10, 1, 2])
        del d[1]
        self.assertEqual(list(d), [10, 2])
