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

from typed_python import Function, OneOf, TupleOf
from nativepython.runtime import Runtime
import unittest
import time


def Compiled(f):
    f = Function(f)
    return Runtime.singleton().compile(f)


class TestCompilationStructures(unittest.TestCase):
    def checkFunctionOfIntegers(self, f):
        r = Runtime.singleton()

        f_fast = r.compile(f)

        for i in range(100):
            self.assertEqual(f_fast(i), f(i))

    def test_simple_loop(self):
        def f(x: int) -> int:
            y = 0
            while x > 0:
                x = x - 1
                y = y + x
            return y

        self.checkFunctionOfIntegers(f)

    def test_returning(self):
        def f(x: int) -> int:
            return x

        self.checkFunctionOfIntegers(f)

    def test_basic_arithmetic(self):
        def f(x: int) -> int:
            y = x+1
            return y

        self.checkFunctionOfIntegers(f)

    def test_boolean_and(self):
        @Compiled
        def f(x: int, y: int, z: int) -> bool:
            return x and y and z

        self.assertEqual(f(0, 0, 0), False)
        self.assertEqual(f(0, 0, 1), False)
        self.assertEqual(f(0, 1, 0), False)
        self.assertEqual(f(0, 1, 1), False)
        self.assertEqual(f(1, 0, 0), False)
        self.assertEqual(f(1, 0, 1), False)
        self.assertEqual(f(1, 1, 0), False)
        self.assertEqual(f(1, 1, 1), True)

    def test_boolean_or(self):
        @Compiled
        def f(x: int, y: int, z: int) -> bool:
            return x or y or z

        self.assertEqual(f(0, 0, 0), False)
        self.assertEqual(f(0, 0, 1), True)
        self.assertEqual(f(0, 1, 0), True)
        self.assertEqual(f(0, 1, 1), True)
        self.assertEqual(f(1, 0, 0), True)
        self.assertEqual(f(1, 0, 1), True)
        self.assertEqual(f(1, 1, 0), True)
        self.assertEqual(f(1, 1, 1), True)

    def test_boolean_operators(self):
        @Compiled
        def f(x: int, y: int, z: str) -> bool:
            return x and y and z

        self.assertEqual(f(0, 1, "s"), False)
        with self.assertRaises(TypeError):
            f(1, 1, "s")

    def test_object_to_int_conversion(self):
        @Compiled
        def f(x: int) -> int:
            return int(object(x))

        self.assertEqual(f(10), 10)

    def test_variable_type_changes_make_sense(self):
        @Compiled
        def f(x: int) -> float:
            y = x
            y = 1.2
            return y

        self.assertEqual(f(10), 1.2)

    def test_call_other_typed_function(self):
        def g(x: int) -> int:
            return x+1

        def f(x: int) -> int:
            return g(x+2)

        self.checkFunctionOfIntegers(f)

    def test_call_untyped_function(self):
        @Function
        def f(x):
            return x

        Runtime.singleton().compile(f)

        x = []

        self.assertIs(f(x), x)

    def test_call_other_untyped_function(self):
        def g(x):
            return x

        @Function
        def f(x):
            return g(x)

        Runtime.singleton().compile(f)

        x = []

        self.assertIs(f(x), x)

    def test_integers_in_closures(self):
        y = 2

        def f(x: int) -> int:
            return x+y

        self.checkFunctionOfIntegers(f)

    def test_loop_variable_changing_type(self):
        @Compiled
        def f(top: float) -> OneOf(int, float):
            y = 0
            x = 0
            while x < top:
                x += 1.0
                y += x

            return y

        self.assertEqual(f(3.5), 10.0)

    def test_unassigned_variables(self):
        @Compiled
        def f(switch: int, t: TupleOf(int)) -> TupleOf(int):
            if switch:
                x = t
            return x

        self.assertEqual(f(1, (1, 2, 3)), (1, 2, 3))

        with self.assertRaisesRegex(Exception, "local variable 'x' referenced before assignment"):
            self.assertEqual(f(0, (1, 2, 3)), (1, 2, 3))

    def test_return_from_function_without_return_value_specified(self):
        @Compiled
        def f(t: TupleOf(int)):
            return t

        self.assertEqual(f((1, 2, 3)), (1, 2, 3))

    def test_return_from_function_with_bad_convert_throws(self):
        @Compiled
        def f(t: TupleOf(int)) -> None:
            return t

        with self.assertRaisesRegex(Exception, "Can't convert"):
            f((1, 2, 3))

    def test_mutually_recursive_untyped_functions(self):
        def q(x):
            return x-1

        def z(x):
            return q(x)+1

        def f(x):
            return z(g(x - 1)) + z(g(x - 2)) + z(x)

        def g(x):
            if x > 0:
                return z(f(x-1)) * z(2) + f(x-2)
            return 1

        g_typed = Function(g)

        Runtime.singleton().compile(g_typed, {'x': int})
        Runtime.singleton().compile(g_typed, {'x': float})

        self.assertEqual(g(10), g_typed(10))

        for input in [18, 18.0]:
            t0 = time.time()
            g(input)
            untyped_duration = time.time() - t0

            t0 = time.time()
            g_typed(input)
            typed_duration = time.time() - t0

            # I get around 50x for ints and 12 for floats
            speedup = untyped_duration / typed_duration
            self.assertGreater(speedup, 20 if isinstance(input, int) else 4)

            print("for ", input, " speedup is ", speedup)

    def test_call_typed_function(self):
        @Function
        def f(x):
            return x

        @Function
        def g(x: int):
            return f(x+1)

        Runtime.singleton().compile(g)
        Runtime.singleton().compile(f)

        self.assertEqual(g(10), 11)

    def test_adding_with_nones_throws(self):
        @Function
        def g():
            return None + None

        Runtime.singleton().compile(g)

        with self.assertRaisesRegex(Exception, "Can't apply op Add.. to expressions of type NoneType"):
            g()

    def test_exception_before_return_propagated(self):
        @Function
        def g():
            None+None
            return None

        Runtime.singleton().compile(g)

        with self.assertRaisesRegex(Exception, "Can't apply op Add.. to expressions of type NoneType"):
            g()

    def test_call_function_with_none(self):
        @Function
        def g(x: None):
            return None

        Runtime.singleton().compile(g)

        self.assertEqual(g(None), None)

    def test_call_other_function_with_none(self):
        def f(x):
            return x

        @Function
        def g(x: int):
            return f(None)

        Runtime.singleton().compile(g)

        self.assertEqual(g(1), None)

    def test_interleaving_nones(self):
        def f(x, y, z):
            x+z
            return y

        @Function
        def works(x: int):
            return f(x, None, x)

        @Function
        def throws(x: int):
            return f(None, None, x)

        Runtime.singleton().compile(works)
        Runtime.singleton().compile(throws)

        self.assertEqual(works(1), None)
        with self.assertRaisesRegex(Exception, "Can't apply op Add.. to expressions of type NoneType"):
            throws(1)

    def test_assign_with_none(self):
        def f(x):
            return x

        @Function
        def g(x: int):
            y = f(None)
            z = y
            return z

        Runtime.singleton().compile(g)

        self.assertEqual(g(1), None)

    def test_nonexistent_variable(self):
        @Compiled
        def f():
            return this_variable_name_is_undefined  # noqa: F821

        with self.assertRaisesRegex(Exception, "name 'this_variable_name_is_undefined' is not defined"):
            f()

    def test_iterating(self):
        @Compiled
        def sumDirectly(x: int):
            y = 0.0
            i = 0
            while i < x:
                y += i
                i += 1
            return y

        @Compiled
        def sumUsingRange(x: int):
            y = 0.0
            for i in range(x):
                y += i
            return y

        for i in range(10):
            self.assertEqual(sumDirectly(i), sumUsingRange(i))

        t0 = time.time()
        sumDirectly(1000000)
        t1 = time.time()
        sumUsingRange(1000000)
        t2 = time.time()

        print("Range is %.2f slower than nonrange." % ((t2-t1)/(t1-t0)))  # I get 1.00
        self.assertTrue((t1-t0) < (t2 - t1) * 1.1)
