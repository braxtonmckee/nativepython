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

from object_database.view import _cur_view, coerce_value

from typed_python.hash import sha_hash

from typed_python import NamedTuple


_base = NamedTuple(_identity=int)


class DatabaseObject(_base):
    __types__ = None
    __schema__ = None

    def __ne__(self, other):
        return not (self == other)

    def __eq__(self, other):
        if not isinstance(other, DatabaseObject):
            return False
        if not type(self) is type(other):
            return False
        return self._identity == other._identity

    def __hash__(self):
        return hash(self._identity)

    @classmethod
    def fromIdentity(cls, identity):
        assert isinstance(identity, int), type(identity)
        cls.__schema__.freeze()

        return _base.__new__(cls, _identity=identity)

    def __new__(cls, *args, **kwds):
        if not hasattr(_cur_view, "view"):
            raise Exception("Please create new objects from within a transaction.")

        return _cur_view.view._new(cls, args, kwds)

    def __repr__(self):
        return type(self).__qualname__ + f"({self._identity})"

    @classmethod
    def lookupOne(cls, **kwargs):
        if not hasattr(_cur_view, "view"):
            raise Exception("Please lookup in indices from within a transaction.")

        return _cur_view.view.indexLookupOne(cls, **kwargs or {" exists": True})

    @classmethod
    def lookupAll(cls, **kwargs):
        if not hasattr(_cur_view, "view"):
            raise Exception("Please lookup in indices from within a transaction.")

        return _cur_view.view.indexLookup(cls, **kwargs or {" exists": True})

    @classmethod
    def lookupAny(cls, **kwargs):
        if not hasattr(_cur_view, "view"):
            raise Exception("Please lookup in indices from within a transaction.")

        return _cur_view.view.indexLookupAny(cls, **kwargs or {" exists": True})

    def exists(self):
        if not hasattr(_cur_view, "view"):
            raise Exception("Please access properties from within a view or transaction.")

        return _cur_view.view._exists(self, self._identity)

    def __getattr__(self, name):
        if name not in self.__types__:
            raise AttributeError("Object of type %s has no field '%s'" % (type(self).__qualname__, name))

        if not hasattr(_cur_view, "view"):
            raise Exception("Please access properties from within a view or transaction.")

        return _cur_view.view._get(self, self._identity, name, self.__types__[name])

    def __setattr__(self, name, val):
        if name not in self.__types__:
            raise AttributeError(
                f"Database object of type {type(self).__qualname__} "
                f"has no attribute '{name}', only {self.__types__.keys()}"
            )

        if not hasattr(_cur_view, "view"):
            raise Exception("Please access properties from within a view or transaction.")

        coerced_val = coerce_value(val, self.__types__[name])

        _cur_view.view._set(self, self._identity, name, self.__types__[name], coerced_val)

    def delete(self):
        _cur_view.view._delete(self, self._identity, self.__types__.keys())

    @classmethod
    def _define(cls, **types):
        assert cls.__types__ is None, "'{}' already defined".format(cls)
        assert isinstance(types, dict)

        cls.__types__ = types

        return cls

    @classmethod
    def to_json(cls, obj):
        return obj.__dict__['_identity']

    @classmethod
    def from_json(cls, obj):
        assert isinstance(obj, str), obj

        return cls.fromIdentity(obj)

    def __sha_hash__(self):
        return sha_hash(self._identity) + sha_hash(type(self).__qualname__)


class Indexed:
    def __init__(self, obj):
        assert isinstance(obj, type)
        self.obj = obj


class Index:
    def __init__(self, *names):
        self.names = names

    def __call__(self, instance):
        return tuple(getattr(instance, x) for x in self.names)
