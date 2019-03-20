#   Copyright 2019 Nativepython Authors
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

from typed_python import TupleOf
from object_database._types import VersionedIdSet, VersionedIdSets

import unittest
import numpy


class VersionedIdSetsTest(unittest.TestCase):
    def test_create(self):
        sets = VersionedIdSets()
        with self.assertRaises(TypeError):
            sets.get(0, "blah")

        idSet = sets.get(0, b"blah")

        assert isinstance(idSet, VersionedIdSet)

    def test_objects_at_transaction(self):
        s = VersionedIdSet()

        tid = 100
        oid = 10
        oid2 = 20

        self.assertEqual(s.lookupOne(tid), -1)
        self.assertEqual(s.lookupFirst(tid), -1)
        self.assertEqual(s.lookupNext(tid, oid), -1)

        s.add(tid, oid)

        self.assertFalse(s.isActive(tid-1, oid))
        self.assertTrue(s.isActive(tid, oid))
        self.assertTrue(s.isActive(tid+1, oid))

        self.assertEqual(s.transactionCount(), 1)
        self.assertEqual(s.transactionCount(), 1)

        self.assertEqual(s.lookupOne(tid-1), -1)
        self.assertEqual(s.lookupOne(tid), oid)
        self.assertEqual(s.lookupOne(tid+1), oid)

        self.assertEqual(s.lookupFirst(tid-1), -1)
        self.assertEqual(s.lookupFirst(tid), oid)
        self.assertEqual(s.lookupFirst(tid+1), oid)

        self.assertEqual(s.lookupNext(tid-1, oid), -1)
        self.assertEqual(s.lookupNext(tid, oid), -1)
        self.assertEqual(s.lookupNext(tid+1, oid), -1)

        s.add(tid, oid2)
        self.assertEqual(s.lookupOne(tid-1), -1)
        self.assertEqual(s.lookupOne(tid), -1)
        self.assertEqual(s.lookupOne(tid+1), -1)

        self.assertEqual(s.lookupFirst(tid-1), -1)
        self.assertEqual(s.lookupFirst(tid), oid)
        self.assertEqual(s.lookupFirst(tid+1), oid)

        self.assertEqual(s.lookupNext(tid-1, oid), -1)
        self.assertEqual(s.lookupNext(tid, oid), oid2)
        self.assertEqual(s.lookupNext(tid+1, oid), oid2)

        self.assertEqual(s.lookupNext(tid-1, oid2), -1)
        self.assertEqual(s.lookupNext(tid, oid2), -1)
        self.assertEqual(s.lookupNext(tid+1, oid2), -1)

    def test_add_and_remove(self):
        s = VersionedIdSet()

        tid = 100
        oid = 20

        for tid in range(100, 120):
            if tid % 2 == 0:
                s.add(tid, oid)
            else:
                s.remove(tid, oid)

        for tid in range(100, 120):
            self.assertEqual(s.isActive(tid, oid), tid % 2 == 0)

    def test_moving_forward(self):
        # test that moving the guarantee forward doesn't change semantics.

        s1 = VersionedIdSet()
        s2 = VersionedIdSet()

        oids = range(10, 30)

        numpy.random.seed(42)

        for tid in range(100, 200):
            # pick a random rate of adding/removing so we get
            # some variation in the test.
            selector = numpy.random.uniform()

            for oid in oids:
                if numpy.random.uniform() < selector:
                    s1.add(tid, oid)
                    s2.add(tid, oid)
                else:
                    s1.remove(tid, oid)
                    s2.remove(tid, oid)

            s2.moveGuaranteedLowestIdForward(tid - 10)

            for tid in range(tid-10, tid+10):
                activeCount = 0

                for oid in oids:
                    activeCount += 1 if s1.isActive(tid, oid) else 0

                    self.assertEqual(s1.isActive(tid, oid), s2.isActive(tid, oid))
                    self.assertEqual(s1.lookupNext(tid, oid), s2.lookupNext(tid, oid))

        self.assertTrue(s2.transactionCount() < s1.transactionCount())
        self.assertTrue(s2.totalEntryCount() < s1.totalEntryCount())

    def test_add_transaction(self):
        for inType in [list, set, tuple, TupleOf(int)]:
            s = VersionedIdSet()

            s.addTransaction(100, inType([10]), inType([]))
            s.addTransaction(101, inType([]), inType([10]))

            self.assertTrue(s.isActive(100, 10))
            self.assertFalse(s.isActive(101, 10))
