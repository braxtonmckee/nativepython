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
from typed_python._types import refcount

from object_database._types import VersionedObjectsOfType
import unittest
import numpy
import time


class VersionedObjectsOfTypeTest(unittest.TestCase):
    def test_create(self):
        VersionedObjectsOfType(int)
        VersionedObjectsOfType(str)

    def test_single_object(self):
        v = VersionedObjectsOfType(int)

        oid = 0

        # by default, we shouldn't have an object
        self.assertEqual(v.best(100, 0), (False, None, -1))

        # we can add something, and it's always the best
        self.assertTrue(v.add(oid, 0, 100))
        self.assertEqual(v.best(oid, -1), (False, None, -1))
        self.assertEqual(v.best(oid, 0), (True, 100, 0))
        self.assertEqual(v.best(oid, 100000), (True, 100, 0))

        # add something else
        self.assertTrue(v.add(oid, 10, 101))

        # can't add it again
        self.assertFalse(v.add(oid, 10, 101))

        # now we should know the versions
        self.assertEqual(v.best(oid, 0), (True, 100, 0))
        self.assertEqual(v.best(oid, 9), (True, 100, 0))
        self.assertEqual(v.best(oid, 10), (True, 101, 10))
        self.assertEqual(v.best(oid, 15), (True, 101, 10))

        # asking for the best of a nonexistant object should fail
        self.assertEqual(v.best(oid + 100, 0), (False, None, -1))

        # add a deletion
        self.assertTrue(v.markDeleted(oid, 20))

        # can't add a deletion again, it's already marked
        self.assertFalse(v.markDeleted(oid, 19))
        self.assertFalse(v.markDeleted(oid, 21))

        self.assertEqual(v.best(oid, 19), (True, 101, 10))
        self.assertEqual(v.best(oid, 20), (False, None, 20))
        self.assertEqual(v.best(oid, 21), (False, None, 20))

        v.moveGuaranteedLowestIdForward(1)

        self.assertEqual(v.best(oid, 0), (False, None, -1))
        self.assertEqual(v.best(oid, 9), (True, 100, 0))
        self.assertEqual(v.best(oid, 10), (True, 101, 10))
        self.assertEqual(v.best(oid, 15), (True, 101, 10))

        # now remove the next one
        v.moveGuaranteedLowestIdForward(10)

        # can't add a value before the guaranteed id
        self.assertFalse(v.add(oid, 9, 10))

        self.assertEqual(v.best(oid, 9), (False, None, -1))
        self.assertEqual(v.best(oid, 10), (True, 101, 10))
        self.assertEqual(v.best(oid, 15), (True, 101, 10))

        # this is a no-op
        v.moveGuaranteedLowestIdForward(15)
        self.assertEqual(v.best(oid, 15), (True, 101, 10))

        v.moveGuaranteedLowestIdForward(20)
        self.assertEqual(v.best(oid, 19), (False, None, -1))
        self.assertEqual(v.best(oid, 20), (False, None, 20))
        self.assertEqual(v.best(oid, 21), (False, None, 20))

        v.moveGuaranteedLowestIdForward(21)
        self.assertEqual(v.best(oid, 20), (False, None, -1))
        self.assertEqual(v.best(oid, 21), (False, None, -1))

    def test_add_reverse_order(self):
        v = VersionedObjectsOfType(int)

        v.add(1, 10, 10)
        v.add(1, 9, 9)
        self.assertEqual(v.best(1, 10), (True, 10, 10))
        self.assertEqual(v.best(1, 9), (True, 9, 9))

        v.add(1, 8, 8)
        v.add(1, 7, 7)

        self.assertEqual(v.best(1, 10), (True, 10, 10))
        self.assertEqual(v.best(1, 9), (True, 9, 9))
        self.assertEqual(v.best(1, 8), (True, 8, 8))
        self.assertEqual(v.best(1, 7), (True, 7, 7))

    def test_add_in_between(self):
        v = VersionedObjectsOfType(int)

        v.add(1, 100, 0)
        v.add(1, 0, 0)
        v.add(1, 10, 0)
        self.assertEqual(v.best(1, 0), (True, 0, 0))
        self.assertEqual(v.best(1, 10), (True, 0, 10))
        self.assertEqual(v.best(1, 100), (True, 0, 100))

    def test_add_in_between_2(self):
        v = VersionedObjectsOfType(int)

        v.add(1, 0, 0)
        v.add(1, 100, 0)
        v.add(1, 10, 0)
        self.assertEqual(v.best(1, 0), (True, 0, 0))
        self.assertEqual(v.best(1, 10), (True, 0, 10))
        self.assertEqual(v.best(1, 100), (True, 0, 100))

    def test_adding_and_removing_at_random(self):
        objectsOfType = VersionedObjectsOfType(int)
        objectsOfType2 = VersionedObjectsOfType(int)

        min_tid = 0
        for passIx in range(10000):
            if numpy.random.choice(100) == 0:
                min_tid += 1
                objectsOfType.moveGuaranteedLowestIdForward(min_tid)
            else:
                oid = numpy.random.choice(1000)
                tid = numpy.random.choice(1000) + min_tid
                if numpy.random.choice(100) == 0:
                    objectsOfType.markDeleted(oid, tid)
                    objectsOfType2.markDeleted(oid, tid)
                else:
                    objectsOfType.add(oid, tid, passIx)
                    objectsOfType2.add(oid, tid, passIx)

            oid = numpy.random.choice(1000)
            tid = numpy.random.choice(1000) + min_tid

            self.assertEqual(objectsOfType.best(oid, tid), objectsOfType2.best(oid, tid))

    def test_perf(self):
        objectsOfType = VersionedObjectsOfType(int)
        t0 = time.time()

        for i in range(1000):
            objectsOfType.add(i, 0, i)

        count = 0
        while time.time() - t0 < 1:
            for i in range(1000):
                objectsOfType.best(i, 0)
            count += 1000
        print(count, " in ", time.time() - t0)

        self.assertTrue(count > 500)

    def test_removing_prior(self):
        objectsOfType = VersionedObjectsOfType(int)
        objectsOfType.add(objectId=10, versionId=0, instance=100)
        objectsOfType.add(objectId=10, versionId=1, instance=100)

        self.assertEqual(objectsOfType.best(10, 0)[2], 0)
        self.assertEqual(objectsOfType.best(10, 1)[2], 1)
        self.assertEqual(objectsOfType.best(10, 2)[2], 1)

        objectsOfType.moveGuaranteedLowestIdForward(1)

        self.assertEqual(objectsOfType.best(10, 0), (False, None, -1))
        self.assertEqual(objectsOfType.best(10, 1)[2], 1)
        self.assertEqual(objectsOfType.best(10, 2)[2], 1)

    def test_refcounts_basic(self):
        T = TupleOf(int)

        tup = T((1, 2, 3))

        objectsOfType = VersionedObjectsOfType(T)

        objectsOfType.add(objectId=10, versionId=10, instance=tup)
        objectsOfType.markDeleted(objectId=10, versionId=20)
        self.assertEqual(refcount(tup), 2)

        objectsOfType.moveGuaranteedLowestIdForward(20)

        self.assertEqual(refcount(tup), 1)

    def test_refcounts_with_whole_destructor(self):
        T = TupleOf(int)

        tup = T((1, 2, 3))

        objectsOfType = VersionedObjectsOfType(T)
        objectsOfType.add(objectId=10, versionId=10, instance=tup)
        self.assertEqual(refcount(tup), 2)
        objectsOfType = None

        self.assertEqual(refcount(tup), 1)

    def test_refcounts_with_consumption_fuzz(self):
        T = TupleOf(int)

        tup = T((1, 2, 3))

        numpy.random.seed(42)

        for passIx in range(10):
            objectsOfType = VersionedObjectsOfType(T)

            for version in range(100):
                for oid in numpy.random.choice(10, size=5):
                    objectsOfType.add(objectId=oid, versionId=version, instance=tup)
                for oid in numpy.random.choice(10, size=5):
                    objectsOfType.markDeleted(objectId=oid, versionId=version)

            for version in range(100):
                objectsOfType.moveGuaranteedLowestIdForward(version)

            self.assertEqual(refcount(tup), 1)

    def test_adding_and_deleting(self):
        objectsOfType = VersionedObjectsOfType(int)
        objectsOfType.add(objectId=10, versionId=0, instance=100)
        objectsOfType.add(objectId=10, versionId=1, instance=100)
