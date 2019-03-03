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

from typed_python._types import serialize, deserialize
from typed_python.python_ast import convertFunctionToAlgebraicPyAst, evaluateFunctionPyAst, Expr, Statement
from typed_python.hash import sha_hash
from typed_python.type_function import ConcreteTypeFunction, isTypeFunctionType, reconstructTypeFunctionType
from types import FunctionType, ModuleType
import numpy
import datetime
import pytz
import lz4.frame
import logging

_reconstruct = numpy.array([1, 2, 3]).__reduce__()[0]
_ndarray = numpy.ndarray


def createEmptyFunction(ast):
    return evaluateFunctionPyAst(ast)


_builtin_name_to_value = {
    ".builtin." + k: v for k, v in __builtins__.items()
    if isinstance(v, type) or 'builtin_function_or_method' in str(type(v))
}
_builtin_name_to_value[".builtin.createEmptyFunction"] = createEmptyFunction
_builtin_name_to_value[".builtin._reconstruct"] = _reconstruct
_builtin_name_to_value[".builtin._ndarray"] = _ndarray
_builtin_name_to_value[".builtin.numpy.scalar"] = numpy.int64(10).__reduce__()[0]  # the 'scalar' function
_builtin_name_to_value[".builtin.dtype"] = numpy.dtype
_builtin_name_to_value[".builtin.numpy"] = numpy
_builtin_name_to_value[".builtin.datetime.datetime"] = datetime.datetime
_builtin_name_to_value[".builtin.datetime.date"] = datetime.date
_builtin_name_to_value[".builtin.datetime.time"] = datetime.time
_builtin_name_to_value[".builtin.datetime.timedelta"] = datetime.timedelta
_builtin_name_to_value[".builtin.pytz"] = pytz
_builtin_name_to_value[".ast.Expr.Lambda"] = Expr.Lambda
_builtin_name_to_value[".ast.Statement.FunctionDef"] = Statement.FunctionDef

_builtin_value_to_name = {id(v): k for k, v in _builtin_name_to_value.items()}


class SerializationContext(object):
    """Represents a collection of types with well-specified names that we can use to serialize objects."""
    def __init__(self, nameToObject=None, compressionEnabled=True, encodeLineInformationForCode=True):
        super().__init__()

        self.nameToObject = nameToObject or {}
        self.objToName = {}

        for k in self.nameToObject:
            assert isinstance(k, str), (
                "nameToObject keys must be strings (This one was not: {})"
                .format(k)
            )

        assert '' not in self.nameToObject, (
            "Empty object/type name not allowed: {}"
            .format(self.nameToObject[''])
        )

        # take the lexically lowest name, so that we're not dependent on ordering.
        for k, v in self.nameToObject.items():
            if id(v) not in self.objToName or k < self.objToName[id(v)]:
                self.objToName[id(v)] = k

        self.compressionEnabled = compressionEnabled
        self.encodeLineInformationForCode = encodeLineInformationForCode

    def compress(self, bytes):
        if self.compressionEnabled:
            res = lz4.frame.compress(bytes)
            return res
        else:
            return bytes

    def decompress(self, bytes):
        if self.compressionEnabled:
            return lz4.frame.decompress(bytes)
        else:
            return bytes

    @staticmethod
    def FromModules(modules):
        """Given a list of modules, produce a serialization context by walking the objects."""
        nameToObject = {}

        for module in modules:
            modulename = module.__name__

            for membername, member in module.__dict__.items():
                if isinstance(member, (type, FunctionType, ConcreteTypeFunction)):
                    nameToObject[modulename + "." + membername] = member
                elif isinstance(member, ModuleType):
                    nameToObject[".modules." + member.__name__] = member

            # also add the module itself so we can serialize it
            nameToObject[".modules." + modulename] = module

        for module in modules:
            modulename = module.__name__

            for membername, member in module.__dict__.items():
                if isinstance(member, type) and hasattr(member, '__dict__'):
                    for sub_name, sub_obj in member.__dict__.items():
                        if not (sub_name[:2] == "__" and sub_name[-2:] == "__"):
                            if isinstance(sub_obj, (type, FunctionType, ConcreteTypeFunction)):
                                nameToObject[modulename + "." + membername + "." + sub_name] = sub_obj
                            elif isinstance(sub_obj, ModuleType):
                                nameToObject[".modules." + sub_obj.__name__] = sub_obj

        return SerializationContext(nameToObject)

    def union(self, other):
        nameToObject = dict(self.nameToObject)
        nameToObject.update(other.nameToObject)
        return SerializationContext(nameToObject)

    def withPrefix(self, prefix):
        return SerializationContext(
            {prefix + "." + k: v for k, v  in self.nameToObject.items()},
            self.compressionEnabled,
            self.encodeLineInformationForCode
            )

    def withoutLineInfoEncoded(self):
        return SerializationContext(self.nameToObject, self.compressionEnabled, False)

    def withoutCompression(self):
        return SerializationContext(self.nameToObject, False, self.encodeLineInformationForCode)

    def withCompression(self):
        return SerializationContext(self.nameToObject, True, self.encodeLineInformationForCode)

    def nameForObject(self, t):
        ''' Return a name(string) for an input object t, or None if not found. '''
        tid = id(t)
        res = self.objToName.get(tid)

        if res is not None:
            return res

        return _builtin_value_to_name.get(tid)

    def objectFromName(self, name):
        ''' Return an object for an input name(string), or None if not found. '''
        res = self.nameToObject.get(name)

        if res is None:
            res = _builtin_name_to_value.get(name)

        if res is None:
            logging.warn("Failed to find a value for object named %s", name)

        return res

    def sha_hash(self, o):
        return sha_hash(self.serialize(o))

    def serialize(self, instance, serializeType=object):
        return serialize(serializeType, instance, self)

    def deserialize(self, bytes, serializeType=object):
        return deserialize(serializeType, bytes, self)

    def representationFor(self, inst):
        ''' Return the representation of a given instance or None.

            For certain types, we want to special-case how they are serialized.
            For those types, we return a representation object and for other
            types we return None.

            The representation consists of a tuple (factory, args, representation).

            During reconstruction, we call factory(*args) to produce an emnpty
            'skeleton' object, and then call `setInstanceStateFromRepresentation`
            with the resulting object and the 'representation'. The values returned
            for  'factory' and 'args' may not have circular dependencies with the current
            object - we deserialize those first, call factory(*args) to get the
            resulting object, and that object gets returned to any objects inside of
            'representation' that have references to the original object.

            @param inst: an instance to be serialized
            @return a representation object or None
        '''

        if isinstance(inst, type):
            isTF = isTypeFunctionType(inst)
            if isTF is not None:
                return (reconstructTypeFunctionType, isTF, None)

        if isinstance(inst, numpy.ndarray):
            return inst.__reduce__()

        if isinstance(inst, numpy.number):
            return inst.__reduce__() + (None,)

        if isinstance(inst, numpy.dtype):
            return (numpy.dtype, (str(inst),), None)

        if isinstance(inst, datetime.datetime):
            return inst.__reduce__() + (None,)

        if isinstance(inst, datetime.date):
            return inst.__reduce__() + (None,)

        if isinstance(inst, datetime.time):
            return inst.__reduce__() + (None,)

        if isinstance(inst, datetime.timedelta):
            return inst.__reduce__() + (None,)

        if isinstance(inst, datetime.tzinfo):
            return inst.__reduce__() + (None,)

        if isinstance(inst, FunctionType):
            representation = {}
            representation["qualname"] = inst.__qualname__
            representation["name"] = inst.__name__
            representation["module"] = inst.__module__

            all_names = set()

            def walkCodeObject(code):
                all_names.update(code.co_names)
                # there are 'code' objects for embedded list comprehensions.
                for c in code.co_consts:
                    if type(c) is type(code):
                        walkCodeObject(c)

            walkCodeObject(inst.__code__)

            representation["freevars"] = {k: v for k, v in inst.__globals__.items() if k in all_names}

            for ix, x in enumerate(inst.__code__.co_freevars):
                representation["freevars"][x] = inst.__closure__[ix].cell_contents

            args = (convertFunctionToAlgebraicPyAst(inst, keepLineInformation=self.encodeLineInformationForCode),)

            return (createEmptyFunction, args, representation)

        return None

    def setInstanceStateFromRepresentation(self, instance, representation):
        if isinstance(instance, datetime.datetime):
            return True

        if isinstance(instance, datetime.date):
            return True

        if isinstance(instance, datetime.time):
            return True

        if isinstance(instance, datetime.timedelta):
            return True

        if isinstance(instance, datetime.tzinfo):
            return True

        if isinstance(instance, type):
            return True

        if isinstance(instance, numpy.dtype):
            return True

        if isinstance(instance, numpy.number):
            return True

        if isinstance(instance, _ndarray):
            instance.__setstate__(representation)
            return True

        if isinstance(instance, FunctionType):
            instance.__globals__.update(representation['freevars'])
            instance.__name__ = representation['name']
            instance.__qualname__ = representation['qualname']

            return True

        return False
