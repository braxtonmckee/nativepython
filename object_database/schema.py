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

from object_database.object import _base, DatabaseObject, Index, Indexed
from object_database._types import bytesToIndexValue
from types import FunctionType
from typed_python import ConstDict, NamedTuple, Tuple, TupleOf, sha_hash


ObjectId = int
FieldId = int
IndexValue = Tuple(int,int,int,int,int)
ObjectFieldId = NamedTuple(objId=int, fieldId=int, isIndexValue=bool)
IndexId = NamedTuple(fieldId=int, indexValue=IndexValue)

TypeDefinition = NamedTuple(fields=TupleOf(str), indices=TupleOf(str))
SchemaDefinition = ConstDict(str, TypeDefinition)

FieldDefinition = NamedTuple(schema=str, typename=str, fieldname=str)

def SubscribeLazilyByDefault(t):
    t.__object_database_lazy_subscription__ = True
    return t

def index_value_to_hash(value, serializationContext=None):
    if isinstance(value, _base):
        return IndexValue((0,0,value._identity,0,0))
    if isinstance(value, int):
        return IndexValue((0,value,0,0,0))
    if isinstance(value, (bytes, str)):
        key = b"i" + str(value).encode("utf8")
        if len(key) <= 20:
            return bytesToIndexValue(key)
    return bytesToIndexValue(b"h" + sha_hash(value, serializationContext).digest[:-1])


class Schema:
    """A collection of types that can be used to access data in a database."""

    def __init__(self, name):
        self._name = name
        # Map: typename:str -> cls(DatabaseObject)
        self._types = {}
        self._supportingTypes = {}
        # class -> indexname -> fun(object->value)
        self._indices = {}
        # class -> fieldname -> set(indices)
        self._indexed_fields = {}
        self._indexTypes = {}
        self._frozen = False
        # Map: cls(DatabaseObject) -> original_cls
        self._types_to_original = {}

    def toDefinition(self):
        return SchemaDefinition({
            tname: self.typeToDef(t)
            for tname, t in self._types.items()
            if issubclass(t, DatabaseObject)
        })

    def __repr__(self):
        return "Schema(%s)" % self.name

    def lookupFullyQualifiedTypeByName(self, name):
        if not name.startswith(self.name + "."):
            return None
        return self._types.get(name[len(self.name)+1:])

    def typeToDef(self, t):
        return TypeDefinition(
            fields=tuple(t.__types__.keys()) + (" exists",),
            indices=tuple(self._indices.get(t, {}).keys())
        )

    @property
    def name(self):
        return self._name

    def freeze(self):
        if not self._frozen:
            for tname, t in self._types.items():
                if issubclass(t, DatabaseObject) and t.__types__ is None:
                    raise Exception("Database subtype %s is not defined." % tname)

            self._frozen = True

    def __setattr__(self, typename, val):
        if typename.startswith("_"):
            self.__dict__[typename] = val
            return

        assert not self._frozen, "Schema is already frozen."

        assert isinstance(val, type)
        assert not issubclass(val, DatabaseObject)

        self._supportingTypes[typename] = val

    def __getattr__(self, typename):
        assert '.' not in typename

        if typename.startswith("_"):
            return self.__dict__[typename]

        if typename in self._supportingTypes:
            return self._supportingTypes[typename]

        if typename not in self._types:
            if self._frozen:
                raise AttributeError(typename)

            class cls(DatabaseObject):
                __object_database_source_class__ = None

            cls.__name__ = typename
            cls.__qualname__ = typename
            cls.__schema__ = self

            self._types[typename] = cls
            self._indices[cls] = {" exists": lambda e: True}
            self._indexTypes[cls] = {" exists": bool}
            self._indexed_fields.setdefault(cls, {}).setdefault(" exists", set()).add(' exists')

        return self._types[typename]

    def _addIndex(self, type, prop):
        assert issubclass(type, DatabaseObject)

        if type not in self._indices:
            self._indices[type] = {}
            self._indexTypes[type] = {}
            self._indexed_fields[type] = {}

        fun = lambda o: getattr(o, prop)
        index_type = type.__types__[prop]

        self._indices[type][prop] = fun
        self._indexTypes[type][prop] = index_type
        self._indexed_fields[type].setdefault(prop, set()).add(prop)

    def _addTupleIndex(self, type, name, props, indexType):
        assert issubclass(type, DatabaseObject)

        if type not in self._indices:
            self._indices[type] = {}
            self._indexTypes[type] = {}
            self._indexed_fields[type] = set()

        fun = lambda o: indexType(tuple(getattr(o, prop) for prop in props))

        self._indices[type][name] = fun
        self._indexTypes[type][name] = indexType

        for prop in props:
            self._indexed_fields[type].setdefault(prop, set()).add(name)

    def define(self, cls):
        assert not cls.__name__.startswith("_"), "Illegal to use _ for first character in database classnames."
        assert not self._frozen, "Schema is already frozen"

        # get a type stub
        t = getattr(self, cls.__name__)
        self._types_to_original[t] = cls

        # compute baseClasses in order to collect the type's attributes
        baseClasses = list(cls.__mro__)
        for i in range(len(baseClasses)):
            if baseClasses[i] is DatabaseObject:
                baseClasses = baseClasses[:i]
                break

        properBaseClasses = [self._types_to_original.get(b, b) for b in baseClasses]

        # Collect the type's attributes and populate (_define) the type object
        # Map: name -> type
        types = {}

        for base in reversed(properBaseClasses):
            for name, val in base.__dict__.items():
                if not name.startswith('__'):
                    if isinstance(val, type):
                        types[name] = val
                    elif isinstance(val, Indexed) and isinstance(val.obj, type):
                        types[name] = val.obj

        t._define(**types)

        for base in reversed(properBaseClasses):
            if base not in (DatabaseObject, _base, object):
                for name, val in base.__dict__.items():
                    if isinstance(val, Index):
                        self._addTupleIndex(t, name, val.names, Tuple(*tuple(types[k] for k in val.names)))
                    if not name.startswith('__') and isinstance(val, Indexed):
                        self._addIndex(t, name)
                    elif (not name.startswith("__") or name in ["__str__", "__repr__"]):
                        if isinstance(val, (FunctionType, staticmethod, property)):
                            setattr(t, name, val)
                    elif name == "__init__":
                        setattr(t, "__odb_initializer__", val)
                    elif name == "__del__":
                        setattr(t, "__odb_destructor__", val)

        if hasattr(cls, '__object_database_lazy_subscription__'):
            t.__object_database_lazy_subscription__ = cls.__object_database_lazy_subscription__

        return t
