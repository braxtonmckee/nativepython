#   Copyright 2017-2019 Nativepython authors
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

import redis
import time
import threading
import logging

from object_database.schema import ObjectFieldId, IndexId, FieldId
from typed_python import serialize, deserialize, OneOf

KeyType = OneOf(ObjectFieldId, IndexId, FieldId, "identityRoot", "types")

class InMemoryPersistence(object):
    def __init__(self, db=0):
        self.values = {}
        self.lock = threading.RLock()

    def get(self, key):
        with self.lock:
            if key not in self.values:
                return None

            val = self.values.get(key)

            assert isinstance(val, bytes), key

            return val

    def set(self, key, value):
        assert isinstance(value, bytes) or value is None, (key, value)

        with self.lock:
            if value is None:
                if key in self.values:
                    del self.values[key]
            else:
                self.values[key] = value

    def _setAdd(self, key, values):
        if not values:
            return

        with self.lock:
            if key not in self.values:
                self.values[key] = set()
            for value in values:
                self.values[key].add(value)

    def _setRemove(self, key, values):
        if not values:
            return

        with self.lock:
            s = self.values.get(key, set())

            for value in values:
                assert value in s, (key, value, s)
                s.remove(value)
            if not s:
                del self.values[key]

    def storedStringCount(self):
        return len([x for x in self.values.values() if isinstance(x, str)])

    def getSetMembers(self, key):
        with self.lock:
            s = self.values.get(key, None)
            if s is None:
                return set()

            assert isinstance(s, set)

            return s

    def getSeveralAsDictionary(self, keys):
        keys = list(keys)
        return {keys[i]: value for i, value in enumerate(self.getSeveral(keys))}

    def getSeveral(self, keys):
        with self.lock:
            return [self.get(k) for k in keys]

    def setSeveral(self, kvs, adds=None, removes=None):
        new_sets, dropped_sets = set(), set()

        with self.lock:
            for k in (adds or []):
                assert not isinstance(self.values.get(k, None), str), k + " is already a string"
            for k in (removes or []):
                assert not isinstance(self.values.get(k, None), str), k + " is already a string"

            for k, v in kvs.items():
                self.set(k, v)

            if adds:
                for k, to_add in adds.items():
                    if k not in self.values:
                        new_sets.add(k)
                    self._setAdd(k, to_add)

            if removes:
                for k, to_remove in removes.items():
                    self._setRemove(k, to_remove)
                    if k not in self.values:
                        dropped_sets.add(k)

        return new_sets, dropped_sets

    def exists(self, key):
        with self.lock:
            return key in self.values

    def delete(self, key):
        with self.lock:
            if key in self.values:
                del self.values[key]


class RedisPersistence(object):
    def __init__(self, db=0, port=None):
        self.lock = threading.RLock()
        kwds = {}

        if port is not None:
            kwds['port'] = port

        self.redis = redis.StrictRedis(db=db, **kwds)
        self.cache = {}

        self._logger = logging.getLogger(__name__)

    def get(self, key):
        """Get the value stored in a value-style key, or None if no key exists.

        Throws an exception if the value is a set.
        """
        with self.lock:
            if key in self.cache:
                assert not isinstance(self.cache[key], set), "item is a set, not a string"
                return self.cache[key]

            success = False
            while not success:
                try:
                    result = self.redis.get(serialize(KeyType, key))
                    success = True
                except redis.exceptions.BusyLoadingError:
                    self._logger.info("Redis is still loading. Waiting...")
                    time.sleep(1.0)

            if result is None:
                return result

            assert isinstance(result, bytes)

            self.cache[key] = result

            return result

    def getSeveralAsDictionary(self, keys):
        keys = list(keys)
        return {keys[i]: value for i, value in enumerate(self.getSeveral(keys))}

    def getSeveral(self, keys):
        """Get the values (or None) stored in several value-style keys."""

        with self.lock:
            needed_keys = [serialize(KeyType, k) for k in keys if k not in self.cache]

            if needed_keys:
                success = False
                while not success:
                    try:
                        vals = self.redis.mget(needed_keys)
                        success = True
                    except redis.exceptions.BusyLoadingError:
                        self._logger.info("Redis is still loading. Waiting...")
                        time.sleep(1.0)

                for ix in range(len(needed_keys)):
                    if vals[ix] is not None:
                        self.cache[deserialize(KeyType, needed_keys[ix])] = vals[ix]

            return [self.cache.get(k, None) for k in keys]

    def getSetMembers(self, key):
        with self.lock:
            if key in self.cache:
                assert isinstance(self.cache[key], set), "item is a string, not a set"
                return self.cache[key]

            success = False
            while not success:
                try:
                    vals = self.redis.smembers(serialize(KeyType, key))
                    success = True
                except redis.exceptions.BusyLoadingError:
                    self._logger.info("Redis is still loading. Waiting...")
                    time.sleep(1.0)

            if vals:
                self.cache[key] = set([k for k in vals])
                return self.cache[key]
            else:
                return set()

    def setSeveral(self, kvs, setAdds=None, setRemoves=None):
        new_sets, dropped_sets = set(), set()
        with self.lock:
            pipe = self.redis.pipeline()

            for key, value in kvs.items():
                if value is None:
                    pipe.delete(serialize(KeyType, key))
                else:
                    pipe.set(serialize(KeyType, key), value)

            for key, to_add in (setAdds or {}).items():
                if key not in self.cache:
                    self.getSetMembers(key)

                for to_add_val in to_add:
                    pipe.sadd(serialize(KeyType, key), to_add_val)

            for key, to_remove in (setRemoves or {}).items():
                if key not in self.cache:
                    self.getSetMembers(key)

                for to_remove_val in to_remove:
                    pipe.srem(serialize(KeyType, key), to_remove_val)

            pipe.execute()

            # update our cache _after_ executing the pipe
            for key, value in kvs.items():
                if value is None:
                    if key in self.cache:
                        del self.cache[key]
                else:
                    self.cache[key] = value

            for key, to_add in (setAdds or {}).items():
                if to_add:
                    if key not in self.cache or not self.cache[key]:
                        self.cache[key] = set()
                        new_sets.add(key)

                    for val in to_add:
                        self.cache[key].add(val)

            for key, to_remove in (setRemoves or {}).items():
                if to_remove:
                    assert self.cache.get(key)

                    for val in to_remove:
                        self.cache[key].discard(val)

                    if not self.cache[key]:
                        dropped_sets.add(key)
                        del self.cache[key]

        return new_sets, dropped_sets

    def set(self, key, value):
        with self.lock:
            if value is None:
                self.redis.delete(serialize(KeyType, key))
                if key in self.cache:
                    del self.cache[key]
            else:
                self.redis.set(serialize(KeyType, key), value)
                self.cache[key] = value

    def exists(self, key):
        with self.lock:
            if key in self.cache:
                return True
            return self.redis.exists(serialize(KeyType, key))

    def delete(self, key):
        with self.lock:
            if key in self.cache:
                del self.cache[key]
            self.redis.delete(serialize(KeyType, key))
