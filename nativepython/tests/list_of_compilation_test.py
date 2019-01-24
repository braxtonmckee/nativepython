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

from typed_python import *
import typed_python._types as _types
from nativepython.runtime import Runtime
import unittest
import time
import numpy
import psutil

def Compiled(f):
    f = TypedFunction(f)
    return Runtime.singleton().compile(f)

class TestListOfCompilation(unittest.TestCase):
    def checkFunction(self, f, argsToCheck):
        r  = Runtime.singleton()

        f_fast = r.compile(f)

        t_py = 0.0
        t_fast = 0.0
        for a in argsToCheck:
            t0 = time.time()
            fastval = f_fast(*a)
            t1 = time.time()
            slowval = f(*a)
            t2 = time.time()

            t_py += t2-t1
            t_fast += t1-t0

            self.assertEqual(fastval, slowval)
        return t_py, t_fast

    def test_list_of_float(self):
        def f(x: ListOf(float), y:ListOf(float)) -> float:
            j = 0
            res = 0.0
            i = 0

            while j < len(y):
                i = 0
                while i < len(x):
                    res = res + x[i] * y[j]
                    i = i + 1
                j = j + 1

            return res

        aListOfFloat = ListOf(float)(list(range(1000)))
        aListOfFloat2 = ListOf(float)(list(range(1000)))

        self.assertEqual(_types.refcount(aListOfFloat),1)

        t_py, t_fast = self.checkFunction(f, [(aListOfFloat,aListOfFloat2)])

        self.assertEqual(_types.refcount(aListOfFloat),1)

        #I get around 150x
        self.assertTrue(t_py / t_fast > 50.0)

        print(t_py / t_fast, " speedup")

    def test_list_passing(self):
        @Compiled
        def f(x: ListOf(int)) -> int:
            return 0

        self.assertEqual(f((1,2,3)), 0)

    def test_list_len(self):
        @Compiled
        def f(x: ListOf(int)) -> int:
            return len(x)

        self.assertEqual(f((1,2,3)), 3)

    def test_list_assign(self):
        @Compiled
        def f(x: ListOf(int)) -> ListOf(int):
            y = x
            return y

        t = ListOf(int)((1,2,3))

        self.assertEqual(f(t), t)

        self.assertEqual(_types.refcount(t),1)

    def test_list_indexing(self):
        @Compiled
        def f(x: ListOf(int), y:int) -> int:
            return x[y]

        self.assertEqual(f((1,2,3),1), 2)

        with self.assertRaises(Exception):
            f((1,2,3),1000000000)

    def test_list_refcounting(self):
        @TypedFunction
        def f(x: ListOf(int), y: ListOf(int)) -> ListOf(int):
            return x

        for compileIt in [False, True]:
            if compileIt:
                Runtime.singleton().compile(f)

            intTup = ListOf(int)(list(range(1000)))

            self.assertEqual(_types.refcount(intTup),1)

            res = f(intTup, intTup)

            self.assertEqual(_types.refcount(intTup),2)

            res = None

            self.assertEqual(_types.refcount(intTup),1)

    def test_list_of_adding(self):
        T = ListOf(int)

        @Compiled
        def f(x: T, y: T) -> T:
            return x + y

        t1 = T((1,2,3))
        t2 = T((3,4))

        res = f(t1, t2)

        self.assertEqual(_types.refcount(res), 1)
        self.assertEqual(_types.refcount(t1), 1)
        self.assertEqual(_types.refcount(t2), 1)

        self.assertEqual(res, t1+t2)

    def test_list_of_list_refcounting(self):
        T = ListOf(int)
        TT = ListOf(T)

        @Compiled
        def f(x: TT) -> TT:
            return x + x + x

        t1 = T((1,2,3))
        t2 = T((4,5,5))

        aTT = TT((t1,t2))

        fRes = f(aTT)

        self.assertEqual(fRes, aTT+aTT+aTT)
        self.assertEqual(_types.refcount(aTT), 1)
        self.assertEqual(_types.refcount(fRes), 1)

        fRes = None
        aTT = None
        self.assertEqual(_types.refcount(t1), 1)

    def test_list_creation_doesnt_leak(self):
        T = ListOf(int)

        @Compiled
        def f(x: T, y: T) -> T:
            return x + y

        t1 = T(tuple(range(10000)))

        initMem = psutil.Process().memory_info().rss / 1024 ** 2

        for i in range(10000):
            f(t1,t1)

        finalMem = psutil.Process().memory_info().rss / 1024 ** 2

        self.assertTrue(finalMem < initMem + 5)

    def test_list_reserve(self):
        T = ListOf(TupleOf(int))

        @Compiled
        def f(x: T):
            x.reserve(x.reserved() + 10)

        aList = T()
        aList.resize(10)

        oldReserved = aList.reserved()
        f(aList)
        self.assertEqual(oldReserved+10, aList.reserved())

    def test_list_resize(self):
        T = ListOf(TupleOf(int))

        aTup = TupleOf(int)((1,2,3))
        @Compiled
        def f(x: T, y: TupleOf(int)):
            x.resize(len(x) + 10, y)

        aList = T()
        aList.resize(10)

        f(aList, aTup)

        self.assertEqual(_types.refcount(aTup), 11)

    def test_list_append(self):
        T = ListOf(int)

        @Compiled
        def f(x: T):
            i = 0
            ct = len(x)
            while i < ct:
                x.append(x[i])
                i = i + 1

        aList = T([1,2,3,4])

        f(aList)

        self.assertEqual(aList, [1,2,3,4,1,2,3,4])

    def test_list_pop(self):
        T = ListOf(int)

        @Compiled
        def f(x: T):
            i = 0
            while i < len(x):
                x.pop(i)
                i = i + 1

        aList = T([1,2,3,4])

        f(aList)

        self.assertEqual(aList, [2,4])

    def test_lists_add_perf(self):
        T = ListOf(int)

        @Compiled
        def range(x: int):
            out = T()
            out.resize(x)

            i = 0
            while i < x:
                out[i] = i
                i = i + 1

            return out

        @Compiled
        def add(x: T, y: T):
            out = T()
            out.reserve(len(x))

            i = 0
            ct = len(x)

            while i < ct:
                out.initializeUnsafe(i, x.getUnsafe(i) + y.getUnsafe(i))
                i = i + 1

            out.setSizeUnsafe(ct)

            return out

        x = range(1000000)
        y = range(1000000)

        xnumpy = numpy.arange(1000000)
        ynumpy = numpy.arange(1000000)

        t0 = time.time()
        for _ in range(10):
            y = add(x,y)
        t1 = time.time()
        for _ in range(10):
            ynumpy = xnumpy+ynumpy
        t2 = time.time()

        slowerThan = (t1 - t0) / (t2 - t1)

        #right now, about 1.8x
        print(slowerThan, "times slower than numpy.")

        self.assertEqual(y[10], x[10] * 11)
        self.assertLess(slowerThan, 2.5)




