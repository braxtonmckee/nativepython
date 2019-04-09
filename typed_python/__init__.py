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

__version__ = "0.2"

from typed_python.internals import Class, Member, Function, UndefinedBehaviorException, makeNamedTuple
from typed_python.type_function import TypeFunction
from typed_python.hash import sha_hash
from typed_python.SerializationContext import SerializationContext
from typed_python.type_filter import TypeFilter
from typed_python._types import (
    TupleOf, ListOf, Tuple, NamedTuple, OneOf, ConstDict,
    Alternative, Value, serialize, deserialize,
    PointerTo, Dict
)
from typed_python.generate_types import typed_python_codegen

import typed_python._types as _types

# in the c module, these are functions, but because they're not parametrized,
# we want them to be actual values. Otherwise, we'll have 'Float64()'
# where we would have written 'Float64' etc.
Bool = _types.Bool()
Int8 = _types.Int8()
Int16 = _types.Int16()
Int32 = _types.Int32()
Int64 = _types.Int64()
UInt8 = _types.UInt8()
UInt16 = _types.UInt16()
UInt32 = _types.UInt32()
UInt64 = _types.UInt64()
Float32 = _types.Float32()
Float64 = _types.Float64()
NoneType = _types.NoneType()
String = _types.String()
Bytes = _types.Bytes()

with open("typed_python/GeneratedTypes.hpp", "w") as f:
    f.writelines(typed_python_codegen(
        NamedTupleTwoStrings=NamedTuple(X=str, Y=str),
        NamedTupleIntFloat=NamedTuple(a=OneOf(int,float), b=float),
        NamedTupleBoolListOfInt=NamedTuple(X=Bool, Y=ListOf(int)),
        NamedTupleAttrAndValues=NamedTuple(attributes=TupleOf(str), values=TupleOf(int))
    ))
