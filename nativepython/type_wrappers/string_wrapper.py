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

from nativepython.type_wrappers.refcounted_wrapper import RefcountedWrapper
from typed_python import Int64, Bool, String, NoneType

import nativepython.type_wrappers.runtime_functions as runtime_functions
from nativepython.type_wrappers.bound_compiled_method_wrapper import BoundCompiledMethodWrapper

import nativepython.native_ast as native_ast
import nativepython

from nativepython.native_ast import VoidPtr

typeWrapper = lambda t: nativepython.python_object_representation.typedPythonTypeToTypeWrapper(t)


class StringWrapper(RefcountedWrapper):
    is_pod = False
    is_empty = False
    is_pass_by_ref = True

    def __init__(self):
        super().__init__(String)

        self.layoutType = native_ast.Type.Struct(element_types=(
            ('refcount', native_ast.Int64),
            ('data', native_ast.UInt8)
        ), name='StringLayout').pointer()

    def getNativeLayoutType(self):
        return self.layoutType

    def on_refcount_zero(self, context, instance):
        assert instance.isReference
        return runtime_functions.free.call(instance.nonref_expr.cast(native_ast.UInt8Ptr))

    def convert_bin_op(self, context, left, op, right):
        if right.expr_type == left.expr_type:
            if op.matches.Eq or op.matches.NotEq or op.matches.Lt or op.matches.LtE or op.matches.GtE or op.matches.Gt:
                cmp_res = context.pushPod(
                    int,
                    runtime_functions.string_cmp.call(
                        left.nonref_expr.cast(VoidPtr),
                        right.nonref_expr.cast(VoidPtr)
                    )
                )
                if op.matches.Eq:
                    return context.pushPod(
                        bool,
                        cmp_res.nonref_expr.eq(0)
                    )
                if op.matches.NotEq:
                    return context.pushPod(
                        bool,
                        cmp_res.nonref_expr.neq(0)
                    )
                if op.matches.Lt:
                    return context.pushPod(
                        bool,
                        cmp_res.nonref_expr.lt(0)
                    )
                if op.matches.LtE:
                    return context.pushPod(
                        bool,
                        cmp_res.nonref_expr.lte(0)
                    )
                if op.matches.Gt:
                    return context.pushPod(
                        bool,
                        cmp_res.nonref_expr.gt(0)
                    )
                if op.matches.GtE:
                    return context.pushPod(
                        bool,
                        cmp_res.nonref_expr.gte(0)
                    )

            if op.matches.Add:
                return context.push(
                    str,
                    lambda strRef: strRef.expr.store(
                        runtime_functions.string_concat.call(
                            left.nonref_expr.cast(VoidPtr),
                            right.nonref_expr.cast(VoidPtr)
                        ).cast(self.layoutType)
                    )
                )

        return super().convert_bin_op(context, left, op, right)

    def convert_getitem(self, context, expr, item):
        item = item.toInt64()

        len_expr = self.convert_len(context, expr)

        with context.ifelse((item.nonref_expr.lt(len_expr.nonref_expr.negate()))
                            .bitor(item.nonref_expr.gte(len_expr.nonref_expr))) as (true, false):
            with true:
                context.pushException(IndexError, "string index out of range")

        return context.push(
            str,
            lambda strRef: strRef.expr.store(
                runtime_functions.string_getitem_int64.call(
                    expr.nonref_expr.cast(native_ast.VoidPtr), item.nonref_expr
                ).cast(self.layoutType)
            )
        )

    def convert_len_native(self, expr):
        return native_ast.Expression.Branch(
            cond=expr,
            false=native_ast.const_int_expr(0),
            true=(
                expr.ElementPtrIntegers(0, 1).ElementPtrIntegers(4)
                .cast(native_ast.Int32.pointer()).load().cast(native_ast.Int64)
            )
        )

    def convert_len(self, context, expr):
        return context.pushPod(int, self.convert_len_native(expr.nonref_expr))

    def constant(self, context, s):
        return context.push(
            str,
            lambda strRef: strRef.expr.store(
                runtime_functions.string_from_utf8_and_len.call(
                    native_ast.const_utf8_cstr(s),
                    native_ast.const_int_expr(len(s))
                ).cast(self.layoutType)
            )
        )

    def _d(s):
        return (s, eval("runtime_functions.string_" + s))

    _bool_methods = dict([
        _d("isalpha"),
        _d("isalnum"),
        _d("isdecimal"),
        _d("isdigit"),
        _d("islower"),
        _d("isnumeric"),
        _d("isprintable"),
        _d("isspace"),
        _d("istitle"),
        _d("isupper")
    ])

    _str_methods = dict([
        _d("lower"),
        _d("upper")
    ])

    def convert_attribute(self, context, instance, attr):
        if attr in ("find", "split") or attr in self._str_methods or attr in self._bool_methods:
            return instance.changeType(BoundCompiledMethodWrapper(self, attr))

        return super().convert_attribute(context, instance, attr)

    def convert_method_call(self, context, instance, methodname, args, kwargs):
        if kwargs:
            return super().convert_method_call(context, instance, methodname, args, kwargs)

        if methodname in self._str_methods:
            if len(args) == 0:
                return context.push(
                    str,
                    lambda strRef: strRef.expr.store(
                        self._str_methods[methodname].call(
                            instance.nonref_expr.cast(VoidPtr)
                        ).cast(self.layoutType)
                    )
                )
        elif methodname in self._bool_methods:
            if len(args) == 0:
                return context.push(
                    Bool,
                    lambda bRef: bRef.expr.store(
                        self._bool_methods[methodname].call(
                            instance.nonref_expr.cast(VoidPtr)
                        )
                    )
                )
        elif methodname == "find":
            if len(args) == 1:
                return context.push(
                    Int64,
                    lambda iRef: iRef.expr.store(
                        runtime_functions.string_find_2.call(
                            instance.nonref_expr.cast(VoidPtr),
                            args[0].nonref_expr.cast(VoidPtr)
                        )
                    )
                )
            elif len(args) == 2:
                return context.push(
                    Int64,
                    lambda iRef: iRef.expr.store(
                        runtime_functions.string_find_3.call(
                            instance.nonref_expr.cast(VoidPtr),
                            args[0].nonref_expr.cast(VoidPtr),
                            args[1].nonref_expr
                        )
                    )
                )
            elif len(args) == 3:
                return context.push(
                    Int64,
                    lambda iRef: iRef.expr.store(
                        runtime_functions.string_find.call(
                            instance.nonref_expr.cast(VoidPtr),
                            args[0].nonref_expr.cast(VoidPtr),
                            args[1].nonref_expr,
                            args[2].nonref_expr
                        )
                    )
                )
        elif methodname == "split":
            if len(args) == 2:
                return context.push(
                    NoneType,
                    lambda Ref: Ref.expr.store(
                        runtime_functions.string_split_2.call(
                            args[0].nonref_expr.cast(VoidPtr),
                            args[1].nonref_expr.cast(VoidPtr)
                        )
                    )
                )
            elif len(args) == 3 and args[2].expr_type.typeRepresentation == String:
                return context.push(
                    NoneType,
                    lambda Ref: Ref.expr.store(
                        runtime_functions.string_split_3.call(
                            args[0].nonref_expr.cast(VoidPtr),
                            args[1].nonref_expr.cast(VoidPtr),
                            args[2].nonref_expr.cast(VoidPtr)
                        )
                    )
                )
            elif len(args) == 3 and args[2].expr_type.typeRepresentation == Int64:
                return context.push(
                    NoneType,
                    lambda Ref: Ref.expr.store(
                        runtime_functions.string_split_3max.call(
                            args[0].nonref_expr.cast(VoidPtr),
                            args[1].nonref_expr.cast(VoidPtr),
                            args[2].nonref_expr
                        )
                    )
                )
            elif len(args) == 4:
                return context.push(
                    NoneType,
                    lambda Ref: Ref.expr.store(
                        runtime_functions.string_split.call(
                            args[0].nonref_expr.cast(VoidPtr),
                            args[1].nonref_expr.cast(VoidPtr),
                            args[2].nonref_expr.cast(VoidPtr),
                            args[3].nonref_expr
                        )
                    )
                )

        return super().convert_method_call(context, instance, methodname, args, kwargs)
