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

from typed_python import Bool, Float64, Int64
from typed_python.python_ast import BinaryOp, ComparisonOp, BooleanOp
import nativepython.native_ast as native_ast
import nativepython

from nativepython.type_wrappers.wrapper import Wrapper

typeWrapper = lambda t: nativepython.python_object_representation.typedPythonTypeToTypeWrapper(t)


class TypedExpression(object):
    def __init__(self, context, expr, t, isReference):
        """Initialize a TypedExpression

        context - an ExpressionConversionContext
        expr - a native_ast containing an expression
        t - a subclass of Wrapper, or a type that we'll convert to a wrapper, or None (meaning
            control doesn't return)
        isReference - is this a pointer to a memory location holding the value, or the actual value?
            if it's a reference, the reference is guaranteed to be valid for the lifetime of the
            expression. if its the value, then the value contains an implicit incref which must
            either be transferred or decreffed.
        """
        super().__init__()
        if isinstance(t, type) or hasattr(t, "__typed_python_category__"):
            t = typeWrapper(t)

        assert isinstance(t, Wrapper) or t is None, t
        assert isinstance(expr, native_ast.Expression), expr

        self.context = context
        self.expr = expr
        self.expr_type = t
        self.isReference = isReference

    def changeType(self, newType):
        return TypedExpression(self.context, self.expr, newType, self.isReference)

    def as_native_call_arg(self):
        """Convert this expression to a call-argument form."""
        if self.expr_type.is_pass_by_ref:
            assert self.isReference
            return self.expr
        else:
            return self.nonref_expr

    @property
    def nonref_expr(self):
        """Get our expression (deferenced if necessary) so that it definitely represents the real object, not its location"""
        if self.expr_type is None:
            return self.expr

        if self.isReference:
            if self.expr_type.is_empty:
                return self.expr >> native_ast.nullExpr
            return self.expr.load()
        else:
            return self.expr

    def ensureNonReference(self):
        return self.expr_type.ensureNonReference(self)

    def convert_incref(self):
        return self.expr_type.convert_incref(self.context, self)

    def convert_set_attribute(self, attribute, expr):
        return self.expr_type.convert_set_attribute(self.context, self, attribute, expr)

    def convert_assign(self, toStore):
        return self.expr_type.convert_assign(self.context, self, toStore)

    def convert_initialize_from_args(self, *args):
        return self.expr_type.convert_initialize_from_args(self.context, self, *args)

    def convert_default_initialize(self):
        return self.expr_type.convert_default_initialize(self.context, self)

    def convert_destroy(self):
        return self.expr_type.convert_destroy(self.context, self)

    def convert_copy_initialize(self, toStore):
        return self.expr_type.convert_copy_initialize(self.context, self, toStore)

    def convert_attribute(self, attribute):
        return self.expr_type.convert_attribute(self.context, self, attribute)

    def convert_setitem(self, index, value):
        return self.expr_type.convert_setitem(self.context, self, index, value)

    def convert_getitem(self, item):
        return self.expr_type.convert_getitem(self.context, self, item)

    def convert_getitem_unsafe(self, item):
        return self.expr_type.convert_getitem_unsafe(self.context, self, item)

    def convert_len(self):
        return self.expr_type.convert_len(self.context, self)

    def convert_reserved(self):
        return self.expr_type.convert_reserved(self.context, self)

    def convert_unary_op(self, op):
        return self.expr_type.convert_unary_op(self.context, self, op)

    def convert_bin_op(self, op, rhs):
        return self.expr_type.convert_bin_op(self.context, self, op, rhs)

    def convert_call(self, args, kwargs):
        return self.expr_type.convert_call(self.context, self, args, kwargs)

    def convert_method_call(self, methodname, args, kwargs):
        return self.expr_type.convert_method_call(self.context, self, methodname, args, kwargs)

    def convert_to_type(self, target_type):
        return self.expr_type.convert_to_type(self.context, self, target_type)

    def convert_next(self):
        return self.expr_type.convert_next(self.context, self)

    def toFloat64(self):
        return self.expr_type.convert_to_type(self.context, self, typeWrapper(Float64))

    def toInt64(self):
        return self.expr_type.convert_to_type(self.context, self, typeWrapper(Int64))

    def toBool(self):
        return self.expr_type.convert_to_type(self.context, self, typeWrapper(Bool))

    @staticmethod
    def asBool(typedExpressionOrNone):
        if typedExpressionOrNone is not None:
            return typedExpressionOrNone.toBool()
        else:
            return None

    def __str__(self):
        return "TypedExpression(%s%s)" % (self.expr_type, ",[ref]" if self.isReference else "")

    def __rshift__(self, other):
        return TypedExpression(self.context, self.expr >> other.expr, other.expr_type, other.isReference)

    @staticmethod
    def sugar_operator(left, right, opname):
        if isinstance(right, (int, float, bool)):
            right = left.context.constant(right)

        if hasattr(BinaryOp, opname):
            op = getattr(BinaryOp, opname)()
        elif hasattr(ComparisonOp, opname):
            op = getattr(ComparisonOp, opname)()
        elif hasattr(BooleanOp, opname):
            op = getattr(BooleanOp, opname)()
        else:
            assert False, opname

        return left.convert_bin_op(op, right)

    def __add__(self, other):
        return TypedExpression.sugar_operator(self, other, "Add")

    def __sub__(self, other):
        return TypedExpression.sugar_operator(self, other, "Sub")

    def __mul__(self, other):
        return TypedExpression.sugar_operator(self, other, "Mult")

    def __truediv__(self, other):
        return TypedExpression.sugar_operator(self, other, "Div")

    def __and__(self, other):
        return TypedExpression.sugar_operator(self, other, "BitAnd")

    def __or__(self, other):
        return TypedExpression.sugar_operator(self, other, "BitOr")

    def __lt__(self, other):
        return TypedExpression.sugar_operator(self, other, "Lt")

    def __le__(self, other):
        return TypedExpression.sugar_operator(self, other, "LtE")

    def __gt__(self, other):
        return TypedExpression.sugar_operator(self, other, "Gt")

    def __ge__(self, other):
        return TypedExpression.sugar_operator(self, other, "GtE")

    def __eq__(self, other):
        return TypedExpression.sugar_operator(self, other, "Eq")

    def __ne__(self, other):
        return TypedExpression.sugar_operator(self, other, "NotEq")
