#   Copyright 2017 Braxton Mckee
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

import nativepython.native_ast as native_ast

Bool = native_ast.Bool
UInt8Ptr = native_ast.UInt8Ptr
Int64 = native_ast.Int64
Float64 = native_ast.Float64
Void = native_ast.Void


def externalCallTarget(name, output, *inputs):
    return native_ast.CallTarget.Named(
        target=native_ast.NamedCallTarget(
            name=name,
            arg_types=inputs,
            output_type=output,
            external=True,
            varargs=False,
            intrinsic=False,
            can_throw=False
        )
    )


free = externalCallTarget("free", Void, UInt8Ptr)
malloc = externalCallTarget("malloc", UInt8Ptr, Int64)
realloc = externalCallTarget("realloc", UInt8Ptr, UInt8Ptr, Int64)
memcpy = externalCallTarget("memcpy", UInt8Ptr, UInt8Ptr, UInt8Ptr, Int64)
memmove = externalCallTarget("memmove", UInt8Ptr, UInt8Ptr, UInt8Ptr, Int64)

stash_exception_ptr = externalCallTarget(
    "nativepython_runtime_stash_const_char_ptr_for_exception",
    Void,
    UInt8Ptr
)

mod_int64_int64 = externalCallTarget(
    "nativepython_runtime_mod_int64_int64",
    Int64,
    Int64, Int64
)

mod_float64_float64 = externalCallTarget(
    "nativepython_runtime_mod_float64_float64",
    Float64,
    Float64, Float64
)

pow_int64_int64 = externalCallTarget(
    "nativepython_runtime_pow_int64_int64",
    Int64,
    Int64, Int64
)

pow_float64_float64 = externalCallTarget(
    "nativepython_runtime_pow_float64_float64",
    Float64,
    Float64, Float64
)

get_pyobj_None = externalCallTarget(
    "nativepython_runtime_get_pyobj_None",
    Void,
    Void.pointer()
)

incref_pyobj = externalCallTarget(
    "nativepython_runtime_incref_pyobj",
    Void,
    Void.pointer()
)

decref_pyobj = externalCallTarget(
    "nativepython_runtime_decref_pyobj",
    Void,
    Void.pointer()
)

getattr_pyobj = externalCallTarget(
    "nativepython_runtime_getattr_pyobj",
    Void.pointer(),
    Void.pointer(),
    UInt8Ptr
)

int_to_pyobj = externalCallTarget(
    "nativepython_runtime_int_to_pyobj",
    Void.pointer(),
    Int64
)

pyobj_to_int = externalCallTarget(
    "nativepython_runtime_pyobj_to_int",
    Int64,
    Void.pointer()
)

string_concat = externalCallTarget(
    "nativepython_runtime_string_concat",
    Void.pointer(),
    Void.pointer(), Void.pointer()
)

string_cmp = externalCallTarget(
    "nativepython_runtime_string_cmp",
    Int64,
    Void.pointer(), Void.pointer()
)

string_getitem_int64 = externalCallTarget(
    "nativepython_runtime_string_getitem_int64",
    Void.pointer(),
    Void.pointer(), Int64
)

string_from_utf8_and_len = externalCallTarget(
    "nativepython_runtime_string_from_utf8_and_len",
    Void.pointer(),
    UInt8Ptr, Int64
)

string_lower = externalCallTarget(
    "nativepython_runtime_string_lower",
    Void.pointer(),
    Void.pointer()
)

string_upper = externalCallTarget(
    "nativepython_runtime_string_upper",
    Void.pointer(),
    Void.pointer()
)

string_find = externalCallTarget(
    "nativepython_runtime_string_find",
    Int64,
    Void.pointer(), Void.pointer(), Int64, Int64
)

string_find_2 = externalCallTarget(
    "nativepython_runtime_string_find_2",
    Int64,
    Void.pointer(), Void.pointer()
)

string_find_3 = externalCallTarget(
    "nativepython_runtime_string_find_3",
    Int64,
    Void.pointer(), Void.pointer(), Int64
)

string_split = externalCallTarget(
    "nativepython_runtime_string_split",
    Void,
    Void.pointer(), Void.pointer(), Void.pointer(), Int64
)

string_split_2 = externalCallTarget(
    "nativepython_runtime_string_split_2",
    Void,
    Void.pointer(), Void.pointer()
)

string_split_3 = externalCallTarget(
    "nativepython_runtime_string_split_3",
    Void,
    Void.pointer(), Void.pointer(), Void.pointer()
)

string_split_3max = externalCallTarget(
    "nativepython_runtime_string_split_3max",
    Void,
    Void.pointer(), Void.pointer(), Int64
)

string_isalpha = externalCallTarget(
    "nativepython_runtime_string_isalpha",
    Bool,
    Void.pointer()
)

string_isalnum = externalCallTarget(
    "nativepython_runtime_string_isalnum",
    Bool,
    Void.pointer()
)

string_isdecimal = externalCallTarget(
    "nativepython_runtime_string_isdecimal",
    Bool,
    Void.pointer()
)

string_isdigit = externalCallTarget(
    "nativepython_runtime_string_isdigit",
    Bool,
    Void.pointer()
)

string_islower = externalCallTarget(
    "nativepython_runtime_string_islower",
    Bool,
    Void.pointer()
)

string_isnumeric = externalCallTarget(
    "nativepython_runtime_string_isnumeric",
    Bool,
    Void.pointer()
)

string_isprintable = externalCallTarget(
    "nativepython_runtime_string_isprintable",
    Bool,
    Void.pointer()
)

string_isspace = externalCallTarget(
    "nativepython_runtime_string_isspace",
    Bool,
    Void.pointer()
)

string_istitle = externalCallTarget(
    "nativepython_runtime_string_istitle",
    Bool,
    Void.pointer()
)

string_isupper = externalCallTarget(
    "nativepython_runtime_string_isupper",
    Bool,
    Void.pointer()
)

bytes_concat = externalCallTarget(
    "nativepython_runtime_bytes_concat",
    Void.pointer(),
    Void.pointer(), Void.pointer()
)

bytes_from_ptr_and_len = externalCallTarget(
    "nativepython_runtime_bytes_from_ptr_and_len",
    Void.pointer(),
    UInt8Ptr, Int64
)
