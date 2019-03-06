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

        #by default, we shouldn't have an object
        self.assertIs(v.best(100, 0), None)

        #we can add something, and it's always the best
        self.assertTrue(v.add(0,0,100))
        self.assertEqual(v.best(0, -1), None)
        self.assertEqual(v.best(0, 0), (100, 0))
        self.assertEqual(v.best(0, 100000), (100, 0))

        #add something else
        self.assertTrue(v.add(0,10,101))

        #can't add it again
        self.assertFalse(v.add(0,10,101))

        #now we should know the versions
        self.assertEqual(v.best(100, 0), None)

        self.assertEqual(v.best(0, 0), (100, 0))
        self.assertEqual(v.best(0, 9), (100, 0))
        self.assertEqual(v.best(0, 10), (101, 10))
        self.assertEqual(v.best(0, 100), (101, 10))

        #can't remove things that don't exist
        self.assertFalse(v.remove(100,0))
        self.assertFalse(v.remove(0,123))
        self.assertFalse(v.remove(0,5))

        #now remove '0'
        self.assertTrue(v.remove(0,0))

        self.assertEqual(v.best(0, 0), None)
        self.assertEqual(v.best(0, 9), None)
        self.assertEqual(v.best(0, 10), (101, 10))
        self.assertEqual(v.best(0, 100), (101, 10))

        #now remove the next one
        self.assertTrue(v.remove(0,10))

        self.assertEqual(v.best(0, 0), None)
        self.assertEqual(v.best(0, 9), None)
        self.assertEqual(v.best(0, 10), None)
        self.assertEqual(v.best(0, 100), None)

    def test_add_reverse_order(self):
        v = VersionedObjectsOfType(int)

        v.add(1,10,10)
        v.add(1,9,9)
        self.assertEqual(v.best(1,10), (10,10))
        self.assertEqual(v.best(1,9), (9,9))

        v.add(1,8,8)
        v.add(1,7,7)

        self.assertEqual(v.best(1,10), (10,10))
        self.assertEqual(v.best(1,9), (9,9))
        self.assertEqual(v.best(1,8), (8,8))
        self.assertEqual(v.best(1,7), (7,7))

    def test_add_in_between(self):
        v = VersionedObjectsOfType(int)

        v.add(1,100,0)
        v.add(1,0,0)
        v.add(1,10,0)
        self.assertEqual(v.best(1,0), (0, 0))
        self.assertEqual(v.best(1,10), (0, 10))
        self.assertEqual(v.best(1,100), (0, 100))

    def test_add_in_between_2(self):
        v = VersionedObjectsOfType(int)

        v.add(1,0,0)
        v.add(1,100,0)
        v.add(1,10,0)
        self.assertEqual(v.best(1,0), (0, 0))
        self.assertEqual(v.best(1,10), (0, 10))
        self.assertEqual(v.best(1,100), (0, 100))

    def test_adding_and_removing_at_random(self):
        objectsOfType = VersionedObjectsOfType(int)

        added = set()

        pairs = []
        for o in range(10):
            for v in range(10):
                pairs.append((o,v))

        def calculatedBest(o,v):
            while v >= 0:
                if (o,v) in added:
                    return (v, v)
                v -= 1
            return None

        toAdd = list(pairs)
        numpy.random.shuffle(toAdd)

        for objectId,versionId in toAdd:
            self.assertTrue(objectsOfType.add(objectId, versionId, versionId))

            added.add((objectId, versionId))

            for (o,v) in pairs:
                self.assertEqual(objectsOfType.best(o,v), calculatedBest(o,v))

        numpy.random.shuffle(toAdd)
        for objectId,versionId in toAdd:
            self.assertTrue(objectsOfType.remove(objectId, versionId))
            added.discard((objectId, versionId))

            for (o,v) in pairs:
                self.assertEqual(objectsOfType.best(o,v), calculatedBest(o,v))

    def test_perf(self):
        objectsOfType = VersionedObjectsOfType(int)
        t0 = time.time()

        for i in range(1000):
            objectsOfType.add(i,0,i)

        count = 0
        while time.time() - t0 < 1:
            for i in range(1000):
                objectsOfType.best(i,0)
            count += 1000
        print(count, " in ", time.time() - t0)

        self.assertTrue(count > 500)
