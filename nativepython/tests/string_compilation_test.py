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

from typed_python import Function, ListOf
from typed_python.test_util import currentMemUsageMb
from nativepython.runtime import Runtime

import unittest


def Compiled(f):
    f = Function(f)
    return Runtime.singleton().compile(f)


someStrings = [
    "",
    "a",
    "as\x00df",
    "\u00F1",
    "\u007F\u0080\u0081\u07FF",
    "\u0800\u0801\uFFFF",
    "\U00010000\U00010001\U0010FFFF"
]

for s1 in list(someStrings):
    for s2 in list(someStrings):
        someStrings.append(s1+s2)
someStrings = sorted(set(someStrings))


def callOrExcept(f, *args):
    try:
        return ("Normal", f(*args))
    except Exception as e:
        return ("Exception", str(e))


def callOrExceptType(f, *args):
    try:
        return ("Normal", f(*args))
    except Exception as e:
        return ("Exception", str(type(e)))


def callOrExceptNoType(f, *args):
    try:
        return ("Normal", f(*args))
    except Exception:
        return ("Exception", )


class TestStringCompilation(unittest.TestCase):
    def test_string_passing_and_refcounting(self):
        @Compiled
        def takeFirst(x: str, y: str):
            return x

        @Compiled
        def takeSecond(x: str, y: str):
            return y

        for s in someStrings:
            for s2 in someStrings:
                self.assertEqual(s, takeFirst(s, s2))
                self.assertEqual(s2, takeSecond(s, s2))

    def test_string_len(self):
        @Compiled
        def compiledLen(x: str):
            return len(x)

        for s in someStrings:
            self.assertEqual(len(s), compiledLen(s))

    def test_string_concatenation(self):
        @Compiled
        def concat(x: str, y: str):
            return x + y

        @Compiled
        def concatLen(x: str, y: str):
            return len(x + y)

        for s in someStrings:
            for s2 in someStrings:
                self.assertEqual(s+s2, concat(s, s2))
                self.assertEqual(len(s+s2), concatLen(s, s2))

    def test_string_comparison(self):
        @Compiled
        def lt(x: str, y: str):
            return x < y

        @Compiled
        def lte(x: str, y: str):
            return x <= y

        @Compiled
        def gte(x: str, y: str):
            return x >= y

        @Compiled
        def gt(x: str, y: str):
            return x > y

        @Compiled
        def eq(x: str, y: str):
            return x == y

        @Compiled
        def neq(x: str, y: str):
            return x != y

        for s in someStrings:
            for s2 in someStrings:
                self.assertEqual(eq(s, s2), s == s2)
                self.assertEqual(neq(s, s2), s != s2)
                self.assertEqual(gte(s, s2), s >= s2)
                self.assertEqual(lte(s, s2), s <= s2)
                self.assertEqual(gt(s, s2), s > s2)
                self.assertEqual(lt(s, s2), s < s2)

    def test_string_constants(self):
        def makeConstantConcatenator(s):
            def returner():
                return s
            return returner

        for s in someStrings:
            f = Compiled(makeConstantConcatenator(s))
            s_from_code = f()

            self.assertEqual(s, s_from_code, (repr(s), repr(s_from_code)))

    def test_string_getitem(self):
        @Compiled
        def getitem(x: str, y: int):
            return x[y]

        for s in someStrings:
            for i in range(-20, 20):
                self.assertEqual(callOrExcept(getitem, s, i), callOrExcept(lambda s, i: s[i], s, i), (s, i))

    def test_string_lower_upper(self):

        @Compiled
        def c_lower(s: str):
            return s.lower()

        @Compiled
        def c_lower2(s: str, t: str):
            return s.lower(t)

        @Compiled
        def c_upper(s: str):
            return s.upper()

        @Compiled
        def c_upper2(s: str, t: str):
            return s.upper(t)

        some_lu_strings = [
            "abc"
            "Abc",
            "aBc",
            "abC",
            "ABC",
            "aBcDefGHiJkLm" * 10000,
            "\u00CA\u00F1\u011A\u1E66\u3444\u1E67\u1EEA\1F04",
            "\u00CA\u00F1\u011A\u1E66\u3444\u1E67\u1EEA\1F04" * 10000,
            "XyZ\U0001D471",
            "XyZ\U0001D471" * 10000,
            "\u007F\u0080\u0081\u07FF\u0800\u0801\uFFFF\U00010000\U00010001\U0010FFFF"
        ]
        for s in some_lu_strings:
            self.assertEqual(c_lower(s), s.lower())
            self.assertEqual(c_upper(s), s.upper())

        for s in some_lu_strings:
            self.assertEqual(callOrExceptType(c_lower2, s, s), callOrExceptType(s.lower, s))
            self.assertEqual(callOrExceptType(c_upper2, s, s), callOrExceptType(s.upper, s))

    def test_string_find(self):

        @Compiled
        def c_find(s: str, sub: str, start: int, end: int):
            return s.find(sub, start, end)

        @Compiled
        def c_find_3(s: str, sub: str, start: int):
            return s.find(sub, start)

        @Compiled
        def c_find_2(s: str, sub: str):
            return s.find(sub)

        def test_find(t):
            substrings = ["", "x", "xyz", "a"*100, t[0:-2] + t[-1] if len(t) > 2 else ""]
            for start in range(0, min(len(t), 8)):
                for end in range(start+1, min(len(t), 8)+1):
                    substrings.append(t[start:end])

            indexrange = [
                -(len(t)+1)*256,
                -len(t)-1, -len(t), -len(t)+1,
                -len(t)//2-1, -len(t)//2, -len(t)//2+1,
                -2, -1, 0, 1, 2,
                len(t)//2-1, len(t)//2, len(t)//2+1,
                len(t)-3, len(t)-2, len(t)-1, len(t), len(t)+1,
                (len(t)+1)*256
            ]
            indexrange = sorted(set(indexrange))
            for sub in substrings:
                for start in indexrange:
                    for end in indexrange:
                        i = t.find(sub, start, end)
                        c_i = c_find(t, sub, start, end)
                        if i != c_i:
                            self.assertEqual(i, c_i)
                        self.assertEqual(i, c_i)
            for sub in substrings:
                for start in indexrange:
                    i = t.find(sub, start)
                    c_i = c_find_3(t, sub, start)
                    self.assertEqual(i, c_i)
            for sub in substrings:
                i = t.find(sub)
                c_i = c_find_2(t, sub)
                self.assertEqual(i, c_i)

        test_find("")
        test_find("a")
        test_find("abcdef")
        test_find("baaaab")
        test_find(("a"*99 + "b")*100)
        test_find("\u00CA\u00D1\u011A\u1E66\u1EEA")
        test_find("\u00DD\U00012EEE\U0001D471\u00AA\U00011234")

        @Compiled
        def c_find_1(s: str):
            return s.find()

        @Compiled
        def c_find_5(s: str, sub: str, start: int, end: int, extra: int):
            return s.find(str, start, end, extra)

        for s in ["a", ""]:
            self.assertEqual(callOrExceptType(c_find_5, s, s, 0, 1, 2), callOrExceptType(s.find, s, 0, 1, 2))
            self.assertEqual(callOrExceptType(c_find_1, s), callOrExceptType(s.find))

    def test_string_is_something(self):

        @Compiled
        def c_isalpha(s: str):
            return s.isalpha()

        @Compiled
        def c_isalnum(s: str):
            return s.isalnum()

        @Compiled
        def c_isdecimal(s: str):
            return s.isdecimal()

        @Compiled
        def c_isdigit(s: str):
            return s.isdigit()

        @Compiled
        def c_islower(s: str):
            return s.islower()

        @Compiled
        def c_isnumeric(s: str):
            return s.isnumeric()

        @Compiled
        def c_isprintable(s: str):
            return s.isprintable()

        @Compiled
        def c_isspace(s: str):
            return s.isspace()

        @Compiled
        def c_istitle(s: str):
            return s.istitle()

        @Compiled
        def c_isupper(s: str):
            return s.isupper()

        def perform_comparison(s: str):
            self.assertEqual(c_isalpha(s), s.isalpha(), [hex(ord(c)) for c in s])
            self.assertEqual(c_isalnum(s), s.isalnum(), [hex(ord(c)) for c in s])
            self.assertEqual(c_isdecimal(s), s.isdecimal(), [hex(ord(c)) for c in s])
            self.assertEqual(c_isdigit(s), s.isdigit(), [hex(ord(c)) for c in s])
            self.assertEqual(c_islower(s), s.islower(), [hex(ord(c)) for c in s])
            self.assertEqual(c_isnumeric(s), s.isnumeric(), [hex(ord(c)) for c in s])
            self.assertEqual(c_isprintable(s), s.isprintable(), [hex(ord(c)) for c in s])
            self.assertEqual(c_isspace(s), s.isspace(), [hex(ord(c)) for c in s])
            self.assertEqual(c_istitle(s), s.istitle(), [hex(ord(c)) for c in s])
            self.assertEqual(c_isupper(s), s.isupper(), [hex(ord(c)) for c in s])

        perform_comparison("")
        for i in range(0, 0x1000):
            perform_comparison(chr(i))
        for i in range(0x1000, 0x110000, 47):
            perform_comparison(chr(i))
        for i in range(0, 0x1000):
            perform_comparison(chr(i) + "a")
            perform_comparison(chr(i) + "1")
            perform_comparison(chr(i) + "A")
            perform_comparison(chr(i) + "/")
            perform_comparison(chr(i) + " ")
            perform_comparison(chr(i) + "\u01C5")
            perform_comparison(chr(i) + "\x00")
            perform_comparison(chr(i) + "\U00010401")
            perform_comparison(chr(i) + "\U00010428")

        titlestrings = [
            "Title Case", "TitleCa)se", "2Title/Case", "2title/case",
            "\u01C5a", "\u01C5A", "\u01C5\u1F88",
            "\u01C5 \u1F88", "\u01C5 \u1F88a", "\u01C5 \u1F88A",
            "\U00010401 \u1F88", "\U00010401 \u1F88a", "\U00010401 \u1F88A",
            "\U00010428 \u1F88", "\U00010428 \u1F88a", "\U00010428 \u1F88A"
        ]
        for s in titlestrings:
            self.assertEqual(c_istitle(s), s.istitle(), s)

    def test_string_split(self):
        @Compiled
        def c_split(s: str, sep: str, max: int) -> ListOf(str):
            r = ListOf(str)()
            s.split(r, s, sep, max)
            return r

        @Compiled
        def c_split_2(s: str) -> ListOf(str):
            r = ListOf(str)()
            s.split(r, s)
            return r

        @Compiled
        def c_split_3(s: str, sep: str) -> ListOf(str):
            r = ListOf(str)()
            s.split(r, s, sep)
            return r

        @Compiled
        def c_split_3max(s: str, max: int) -> ListOf(str):
            r = ListOf(str)()
            s.split(r, s, max)
            return r

        @Compiled
        def c_split_initialized(s: str, lst: ListOf(str)) -> ListOf(str):
            r = ListOf(str)(lst)
            s.split(r, s)
            return r

        # unexpected standard behavior:
        #   "   abc   ".split(maxsplit=0) = "abc   "  *** not "abc" nor "   abc   " ***
        split_strings = [
            "  abc  ",
            "ahjdfashjkdfsj ksdjkhsfhjdkf",
            "ahjdfashjkdfsj ksdjkhsfhjdkf" * 100,
            "",
            "a",
            " one two  three   \tfour    \n\nfive\r\rsix\n",
            " one two  three   \tfour    \n\nfive\r\rsix\n" * 100
        ]
        for s in split_strings:
            result = callOrExceptNoType(c_split_2, s)
            baseline = callOrExceptNoType(lambda: s.split())
            self.assertEqual(result, baseline, "{} -> {}".format(s, result))
            result = callOrExceptNoType(c_split_initialized, s, ["a", "b", "c"])
            self.assertEqual(result, baseline, "{} -> {}".format(s, result))
            for m in range(-2, 10):
                result = callOrExceptNoType(c_split_3max, s, m)
                baseline = callOrExceptNoType(lambda: s.split(maxsplit=m))
                self.assertEqual(result, baseline, "{},{}-> {}".format(s, m, result))

            for sep in ["", "j", "s", "d", "t", " ", "as", "jks"]:
                result = callOrExceptNoType(c_split_3, s, sep)
                baseline = callOrExceptNoType(lambda: s.split(sep))
                self.assertEqual(result, baseline, "{},{}-> {}".format(s, sep, result))
                for m in range(-2, 10):
                    result = callOrExceptNoType(c_split, s, sep, m)
                    baseline = callOrExceptNoType(lambda: s.split(sep, m))
                    self.assertEqual(result, baseline, "{},{},{}-> {}".format(s, sep, m, result))

        startusage = currentMemUsageMb()
        for i in range(1000):
            result = c_split_2(split_strings[2])
        endusage = currentMemUsageMb()
        self.assertLess(endusage, startusage + 1, f"{startusage}->{endusage}")
        """
        def rep_find(s, subs):
            result = 0
            for i in range(1000):
                for sub in subs:
                    result += s.find(sub)
            return result

        @Compiled
        def c_rep_find(s: str, subs: ListOf(str)) -> int:
            result = 0
            for i in range(1000):
                for sub in subs:
                    result += s.find(sub)
            return result

        def test_find_perf(f, s, subs):
            t0 = time.time()
            f(s,subs)
            return time.time() - t0

        s = "a" * 100000 + "b" + "a" * 100000
        subs = ["aaa", "aba","aab","baa", "bbb"]
        c_elapsed = test_find_perf(c_rep_find, s, subs)
        elapsed = test_find_perf(rep_find, s, subs)
        self.assertTrue(c_elapsed < elapsed,
                "Slow Performance: compiled took {0} sec versus baseline {1}"
                .format(c_elapsed, elapsed)
        )
        """
