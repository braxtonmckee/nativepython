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

import nativepython
import nativepython.native_ast as native_ast
import nativepython.type_wrappers.runtime_functions as runtime_functions

from nativepython.type_wrappers.none_wrapper import NoneWrapper
from nativepython.python_object_representation import pythonObjectRepresentation
from nativepython.typed_expression import TypedExpression
from nativepython.conversion_exception import ConversionException
from typed_python import NoneType, Alternative, OneOf


NoneExprType = NoneWrapper()

typeWrapper = lambda t: nativepython.python_object_representation.typedPythonTypeToTypeWrapper(t)

ExpressionIntermediate = Alternative(
    "ExpressionIntermediate",
    Effect={"expr": native_ast.Expression},
    Terminal={"expr": native_ast.Expression},
    Simple={"name": str, "expr": native_ast.Expression},
    StackSlot={"name": str, "expr": native_ast.Expression}
)


class ExpressionConversionContext(object):
    """Context class when we're converting a single compound expression.

    This class tracks creation of temporaries so we can destroy them at the end of expression
    evaluation, and provides convenience methods to allow expression generators to stash
    compound expressions and get back simple variable references.
    """

    def __init__(self, functionContext):
        self.functionContext = functionContext
        self.intermediates = []  # list of ExpressionIntermediate objects
        self.teardowns = []

    @property
    def converter(self):
        return self.functionContext.converter

    def isEmpty(self):
        return not self.intermediates

    def inputArg(self, type, name):
        return TypedExpression(self, native_ast.Expression.Variable(name), type, type.is_pass_by_ref)

    def constant(self, x):
        if isinstance(x, bool):
            return TypedExpression(self, native_ast.const_bool_expr(x), bool, False)
        if isinstance(x, int):
            return TypedExpression(self, native_ast.const_int_expr(x), int, False)
        if isinstance(x, float):
            return TypedExpression(self, native_ast.const_float_expr(x), float, False)
        assert False

    def pushVoid(self, t=None):
        if t is None:
            t = typeWrapper(type(None))

        assert t.is_empty, t
        return TypedExpression(self, native_ast.nullExpr, t, False)

    def pushPod(self, typ, expression):
        """stash an expression that generates POD passed as a value

        Parameters
        ----------
        typ: type
            the python type of the expression
        expression: native_type.Expression
            the expression to let-name
        """
        typ = typeWrapper(typ)

        assert typ.is_pod
        assert not typ.is_pass_by_ref

        varname = self.functionContext.let_varname()

        self.intermediates.append(
            ExpressionIntermediate.Simple(name=varname, expr=expression)
        )

        return TypedExpression(self, native_ast.Expression.Variable(varname), typ, False)

    def pushLet(self, type, expression, isReference):
        """Push an arbitrary expression onto the stack."""
        varname = self.functionContext.let_varname()

        self.intermediates.append(
            ExpressionIntermediate.Simple(name=varname, expr=expression)
        )

        return TypedExpression(self, native_ast.Expression.Variable(varname), type, isReference)

    def pushEffect(self, expression):
        """Push a native expression that has a side effect but no value, and that returns control flow."""
        if expression is None:
            return

        if expression == native_ast.nullExpr:
            return

        self.intermediates.append(
            ExpressionIntermediate.Effect(expression)
        )

    def pushTerminal(self, expression):
        """Push a native expression that does not return control flow."""
        self.intermediates.append(
            ExpressionIntermediate.Terminal(expression)
        )

    def pushMove(self, typed_expression):
        """Given a typed expression, allocate space for it on the stack and 'move' it
        (copy its bits, but don't inc or decref it)
        """
        return self.push(
            typed_expression.expr_type,
            lambda other: other.expr.store(typed_expression.nonref_expr),
            wantsTeardown=False
        )

    def let(self, e1, e2):
        v = self.functionContext.let_varname()

        return native_ast.Expression.Let(
            var=v,
            val=e1,
            within=e2(native_ast.Expression.Variable(name=v))
        )

    def pushReferenceCopy(self, type, expression):
        """Given a native expression that returns a reference, duplicate the object
        and return a handle."""
        type = typeWrapper(type)

        toCopy = self.pushReference(type, expression)

        return self.push(type, lambda target: type.convert_copy_initialize(target, toCopy))

    def pushReference(self, type, expression):
        """Push a reference to an object that's guaranteed to be alive for the duration of the expression."""
        type = typeWrapper(type)

        varname = self.functionContext.let_varname()

        self.intermediates.append(
            ExpressionIntermediate.Simple(name=varname, expr=expression)
        )

        return TypedExpression(self, native_ast.Expression.Variable(varname), type, True)

    def allocateUninitializedSlot(self, type):
        type = typeWrapper(type)

        varname = self.functionContext.stack_varname()

        resExpr = TypedExpression(
            self,
            native_ast.Expression.StackSlot(name=varname, type=type.getNativeLayoutType()),
            type,
            True
        )

        if not type.is_pod:
            with self.subcontext() as sc:
                type.convert_destroy(self, resExpr)

            self.teardowns.append(
                native_ast.Teardown.ByTag(
                    tag=varname,
                    expr=sc.result
                )
            )

        return resExpr

    def markUninitializedSlotInitialized(self, slot):
        if slot.expr_type.is_pod:
            return

        assert slot.expr.matches.StackSlot

        self.pushEffect(native_ast.Expression.ActivatesTeardown(slot.expr.name))

    def push(self, type, callback, wantsTeardown=True):
        """Allocate a stackvariable of type 'type' and pass it to 'callback' which should return
        a native_ast.Expression or TypedExpression(None) initializing it.
        """
        type = typeWrapper(type)

        if type.is_pod:
            wantsTeardown = False

        varname = self.functionContext.stack_varname()

        resExpr = TypedExpression(
            self,
            native_ast.Expression.StackSlot(name=varname, type=type.getNativeLayoutType()),
            type,
            True
        )

        expr = callback(resExpr)

        if expr is None:
            expr = native_ast.nullExpr

        if isinstance(expr, TypedExpression):
            assert expr.expr_type.typeRepresentation is NoneType, expr.expr_type
            expr = expr.expr
        else:
            assert isinstance(expr, native_ast.Expression)

        self.intermediates.append(
            ExpressionIntermediate.StackSlot(
                name=varname,
                expr=expr if not wantsTeardown else expr >> native_ast.Expression.ActivatesTeardown(varname)
            )
        )

        if wantsTeardown:
            with self.subcontext() as sc:
                type.convert_destroy(self, resExpr)

            self.teardowns.append(
                native_ast.Teardown.ByTag(
                    tag=varname,
                    expr=sc.result
                )
            )

        return resExpr

    def subcontext(self):
        class Scope:
            def __init__(scope):
                scope.intermediates = None
                scope.teardowns = None

            def __enter__(scope):
                scope.intermediates = self.intermediates
                scope.teardowns = self.teardowns
                self.intermediates = []
                self.teardowns = []

                scope.expr = None

                return scope

            def __exit__(scope, *args):
                scope.result = self.finalize(scope.expr)

                self.intermediates = scope.intermediates
                self.teardowns = scope.teardowns

        return Scope()

    def whileLoop(self, conditionExpr):
        if isinstance(conditionExpr, TypedExpression):
            conditionExpr = conditionExpr.nonref_expr

        class Scope:
            def __init__(scope):
                scope.intermediates = None
                scope.teardowns = None

            def __enter__(scope):
                scope.intermediates = self.intermediates
                scope.teardowns = self.teardowns
                self.intermediates = []
                self.teardowns = []

            def __exit__(scope, *args):
                result = self.finalize(None)
                self.intermediates = scope.intermediates
                self.teardowns = scope.teardowns

                self.pushEffect(
                    native_ast.Expression.While(
                        cond=conditionExpr,
                        while_true=result,
                        orelse=native_ast.nullExpr
                    )
                )
        return Scope()

    def loop(self, countExpr):
        class Scope:
            def __init__(scope):
                scope.intermediates = None
                scope.teardowns = None

            def __enter__(scope):
                scope.counter = self.push(
                    int,
                    lambda counter: counter.expr.store(native_ast.const_int_expr(0))
                )

                scope.intermediates = self.intermediates
                scope.teardowns = self.teardowns
                self.intermediates = []
                self.teardowns = []

                return scope.counter

            def __exit__(scope, *args):
                result = self.finalize(None)
                self.intermediates = scope.intermediates
                self.teardowns = scope.teardowns

                self.pushEffect(
                    native_ast.Expression.While(
                        cond=scope.counter.nonref_expr.lt(countExpr.nonref_expr),
                        while_true=result >> scope.counter.expr.store(
                            scope.counter.nonref_expr.add(native_ast.const_int_expr(1))
                        ),
                        orelse=native_ast.nullExpr
                    )
                )
        return Scope()

    def switch(self, expression, targets, wantsBailout):
        results = {}

        if wantsBailout:
            targets = tuple(targets) + (None,)
        else:
            targets = tuple(targets)

        class Scope:
            def __init__(scope, target):
                scope.intermediates = []
                scope.teardowns = []
                scope.target = target

            def __enter__(scope):
                scope.intermediates, self.intermediates = self.intermediates, scope.intermediates
                scope.teardowns, self.teardowns = self.teardowns, scope.teardowns

            def __exit__(scope, *args):
                results[scope.target] = self.finalize(None)

                scope.intermediates, self.intermediates = self.intermediates, scope.intermediates
                scope.teardowns, self.teardowns = self.teardowns, scope.teardowns

        class MainScope:
            def __init__(scope):
                pass

            def __enter__(scope):
                return [(target, Scope(target)) for target in targets]

            def __exit__(scope, t, v, traceback):
                if t is None:
                    expr = results.get(targets[-1], native_ast.nullExpr)

                    for t in reversed(targets[:-1]):
                        expr = native_ast.Expression.Branch(
                            cond=expression.cast(native_ast.Int64).eq(native_ast.const_int_expr(t)),
                            true=results.get(t, native_ast.nullExpr),
                            false=expr
                        )

                    self.pushEffect(expr)

        return MainScope()

    def ifelse(self, condition):
        if isinstance(condition, TypedExpression):
            condition = condition.toBool().nonref_expr

        results = {}

        class Scope:
            def __init__(scope, isTrue):
                scope.intermediates = []
                scope.teardowns = []
                scope.isTrue = isTrue

            def __enter__(scope):
                scope.intermediates, self.intermediates = self.intermediates, scope.intermediates
                scope.teardowns, self.teardowns = self.teardowns, scope.teardowns

            def __exit__(scope, *args):
                results[scope.isTrue] = self.finalize(None)

                scope.intermediates, self.intermediates = self.intermediates, scope.intermediates
                scope.teardowns, self.teardowns = self.teardowns, scope.teardowns

        class MainScope:
            def __init__(scope):
                pass

            def __enter__(scope):
                return Scope(True), Scope(False)

            def __exit__(scope, t, v, traceback):
                if t is None:
                    true = results.get(True, native_ast.nullExpr)
                    false = results.get(False, native_ast.nullExpr)

                    self.pushEffect(
                        native_ast.Expression.Branch(
                            cond=condition,
                            true=true,
                            false=false
                        )
                    )

        return MainScope()

    def finalize(self, expr):
        if expr is None:
            expr = native_ast.nullExpr
        elif isinstance(expr, native_ast.Expression):
            pass
        else:
            assert isinstance(expr, TypedExpression), type(expr)
            expr = expr.expr

        for i in reversed(self.intermediates):
            if i.matches.Terminal or i.matches.Effect:
                expr = i.expr >> expr
            elif i.matches.StackSlot:
                expr = i.expr >> expr
            elif i.matches.Simple:
                expr = native_ast.Expression.Let(var=i.name, val=i.expr, within=expr)

        if not self.teardowns:
            return expr

        return native_ast.Expression.Finally(expr=expr, teardowns=self.teardowns)

    def call_py_function(self, f, args, kwargs, returnTypeOverload=None):
        if kwargs:
            raise NotImplementedError("Kwargs not implemented for py-function dispatch yet")

        # force arguments to a type appropriate for argpassing
        native_args = [a.as_native_call_arg() for a in args if not a.expr_type.is_empty]

        call_target = self.functionContext.converter.convert(f, [a.expr_type for a in args], returnTypeOverload)

        if call_target is None:
            self.pushException(TypeError, "Function %s was not convertible." % f.__qualname__)
            return

        if call_target.output_type is None:
            # this always throws
            assert len(call_target.named_call_target.arg_types) == len(native_args)

            self.pushTerminal(call_target.call(*native_args))
            self.pushException(TypeError, "Unreachable code.")
            return

        if call_target.output_type.is_pass_by_ref:
            return self.push(
                call_target.output_type,
                lambda output_slot: call_target.call(output_slot.expr, *native_args)
            )
        else:
            assert call_target.output_type.is_pod
            assert len(call_target.named_call_target.arg_types) == len(native_args)

            return self.pushPod(
                call_target.output_type,
                call_target.call(*native_args)
            )

    def isInitializedVarExpr(self, name):
        return TypedExpression(
            self,
            native_ast.Expression.StackSlot(
                name=name + ".isInitialized",
                type=native_ast.Bool
            ),
            bool,
            isReference=True
        )

    def named_var_expr(self, name):
        if self.functionContext._varname_to_type[name] is None:
            raise ConversionException(
                "variable %s is not in scope here" % name
            )

        slot_type = self.functionContext._varname_to_type[name]

        return TypedExpression(
            self,
            native_ast.Expression.StackSlot(
                name=name,
                type=slot_type.getNativeLayoutType()
            ),
            slot_type,
            isReference=True
        )

    def pushComment(self, c):
        self.pushEffect(native_ast.nullExpr.with_comment(c))

    def pushException(self, type, value):
        self.pushEffect(
            # as a short-term hack, use a runtime function to stash this where the callsite can pick it up.
            runtime_functions.stash_exception_ptr.call(
                native_ast.const_utf8_cstr(str(value))
            )
            >> native_ast.Expression.Throw(
                expr=native_ast.Expression.Constant(
                    val=native_ast.Constant.NullPointer(value_type=native_ast.UInt8.pointer())
                )
            )
        )

    def convert_expression_ast(self, ast):
        """ ast: python_ast:Expression"""
        if ast.matches.Attribute:
            attr = ast.attr
            val = self.convert_expression_ast(ast.value)

            if val is None:
                return None

            return val.convert_attribute(attr)

        if ast.matches.Name:
            assert ast.ctx.matches.Load
            if ast.id in self.functionContext._varname_to_type:
                with self.ifelse(self.isInitializedVarExpr(ast.id)) as (true, false):
                    with false:
                        self.pushException(UnboundLocalError, "local variable '%s' referenced before assignment" % ast.id)
                return self.named_var_expr(ast.id)

            if ast.id in self.functionContext._free_variable_lookup:
                return pythonObjectRepresentation(self, self.functionContext._free_variable_lookup[ast.id])

            elif ast.id in __builtins__:
                return pythonObjectRepresentation(self, __builtins__[ast.id])

            if ast.id not in self.functionContext._varname_to_type:
                self.pushException(NameError, "name '%s' is not defined" % ast.id)
                return None

        if ast.matches.Num:
            if ast.n.matches.None_:
                return pythonObjectRepresentation(self, None)
            if ast.n.matches.Boolean:
                return pythonObjectRepresentation(self, bool(ast.n.value))
            if ast.n.matches.Int:
                return pythonObjectRepresentation(self, int(ast.n.value))
            if ast.n.matches.Float:
                return pythonObjectRepresentation(self, float(ast.n.value))

        if ast.matches.Str:
            return pythonObjectRepresentation(self, ast.s)

        if ast.matches.BoolOp:
            def convertBoolOp(depth=0):
                with self.subcontext() as sc:
                    value = self.convert_expression_ast(ast.values[depth])
                    value = TypedExpression.asBool(value)
                    if value is not None:
                        if depth == len(ast.values) - 1:
                            sc.expr = value.expr
                        else:
                            tail_expr = convertBoolOp(depth + 1)

                            if ast.op.matches.And:
                                sc.expr = native_ast.Expression.Branch(
                                    cond=value.expr, true=tail_expr, false=native_ast.falseExpr)
                            elif ast.op.matches.Or:
                                sc.expr = native_ast.Expression.Branch(
                                    cond=value.expr, true=native_ast.trueExpr, false=tail_expr)
                            else:
                                raise Exception(f"Unknown kind of Boolean operator: {ast.op.Name}")

                return sc.result

            return TypedExpression(self, convertBoolOp(), typeWrapper(bool), False)

        if ast.matches.BinOp:
            lhs = self.convert_expression_ast(ast.left)

            if lhs is None:
                return None

            rhs = self.convert_expression_ast(ast.right)

            if rhs is None:
                return None

            return lhs.convert_bin_op(ast.op, rhs)

        if ast.matches.UnaryOp:
            operand = self.convert_expression_ast(ast.operand)

            return operand.convert_unary_op(ast.op)

        if ast.matches.Subscript:
            assert ast.slice.matches.Index

            val = self.convert_expression_ast(ast.value)
            if val is None:
                return None
            index = self.convert_expression_ast(ast.slice.value)
            if index is None:
                return None

            return val.convert_getitem(index)

        if ast.matches.Call:
            lhs = self.convert_expression_ast(ast.func)

            if lhs is None:
                return None

            for a in ast.args:
                assert not a.matches.Starred, "not implemented yet"

            args = []
            kwargs = {}

            for a in ast.args:
                args.append(self.convert_expression_ast(a))
                if args[-1] is None:
                    return None

            for keywordArg in ast.keywords:
                argname = keywordArg.arg

                kwargs[argname] = self.convert_expression_ast(keywordArg.value)
                if kwargs[argname] is None:
                    return None

            return lhs.convert_call(args, kwargs)

        if ast.matches.Compare:
            assert len(ast.comparators) == 1, "multi-comparison not implemented yet"
            assert len(ast.ops) == 1

            lhs = self.convert_expression_ast(ast.left)

            if lhs is None:
                return None

            r = self.convert_expression_ast(ast.comparators[0])

            if r is None:
                return None

            return lhs.convert_bin_op(ast.ops[0], r)

        if ast.matches.Tuple:
            raise NotImplementedError("not implemented yet")

        if ast.matches.IfExp:
            test = self.convert_expression_ast(ast.test)
            if test is None:
                return None
            test = test.toBool()
            if test is None:
                return None

            with self.ifelse(test) as (true_block, false_block):
                with true_block:
                    true_res = self.convert_expression_ast(ast.body)
                with false_block:
                    false_res = self.conversion_exception(ast.orelse)

                if true_res.expr_type != false_res.expr_type:
                    out_type = typeWrapper(OneOf(true_res.expr_type.typeRepresentation, false_res.expr_type.typeRepresentation))
                else:
                    out_type = true_res.expr_type

                out_slot = self.allocateUninitializedSlot(out_type)

                with true_block:
                    true_res = true_res.convert_to_type(out_type)
                    out_slot.convert_copy_initialize(true_res)
                    self.markUninitializedSlotInitialized(out_slot)

                with false_block:
                    false_res = false_res.convert_to_type(out_type)
                    out_slot.convert_copy_initialize(false_res)
                    self.markUninitializedSlotInitialized(out_slot)

            return out_slot

        raise ConversionException("can't handle python expression type %s" % ast._which)
