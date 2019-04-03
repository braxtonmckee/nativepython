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

from typed_python import _types

from nativepython.type_wrappers.exceptions import generateThrowException


class Wrapper(object):
    """Represents a code-generation wrapper for objects of a particular type.

    For each type we can represent in typed python, and for types that have
    an injection into compiled code but don't take on runtime values
    (types whose values are known, or singleton functions like 'len' or 'range'),
    we have a corresponding 'wrapper' type.

    The wrapper type is responsible for controlling how we generate code
    to perform the relevant operations on objects of the wrapped type.
    """

    # is this 'plain old data' with no constructor/destructor semantics?
    # if so, we can dispense with destructors entirely.
    is_pod = False

    # does this boil down to a void type? if so, it will always be excluded
    # from function argument lists (both in the defeinitions and in calls)
    is_empty = False

    # do we pass this as a reference to a stackslot or as registers?
    # if true, then when this is a return value, we also have to pass a pointer
    # to the output location as the first argument (and return void) rather
    # than returning registers.
    is_pass_by_ref = True

    def __repr__(self):
        return "Wrapper(%s)" % str(self)

    def __str__(self):
        rep = self.typeRepresentation

        if isinstance(rep, type):
            return rep.__qualname__
        if isinstance(rep, tuple) and len(rep) == 2 and isinstance(rep[0], type):
            return "(" + rep[0].__qualname__ + "," + str(rep[1]) + ")"
        return str(rep)

    def __init__(self, typeRepresentation):
        super().__init__()

        self.typeRepresentation = typeRepresentation

    def __eq__(self, other):
        if type(self) != type(other):
            return False

        return self.typeRepresentation == other.typeRepresentation

    def __hash__(self):
        return hash(self.typeRepresentation)

    def getNativePassingType(self):
        if self.is_pass_by_ref:
            return self.getNativeLayoutType().pointer()
        else:
            return self.getNativeLayoutType()

    def getBytecount(self):
        if self.is_empty:
            return 0

        return _types.bytecount(self.typeRepresentation)

    def convert_incref(self, context, expr):
        if self.is_pod:
            return

        raise NotImplementedError(self)

    def convert_next(self, context, expr):
        """Return a pair of typed_expressions (next_value, continue_iteration) for the result of __next__.

        If continue_iteration is False, then next_value will be ignored. It should be a reference.
        """
        context.pushTerminal(
            generateThrowException(
                context,
                AttributeError("%s object cannot be iterated" % self)
            )
        )
        return None

    def convert_attribute(self, context, instance, attribute):
        context.pushTerminal(
            generateThrowException(
                context,
                AttributeError("%s object has no attribute %s" % (self, attribute))
            )
        )
        return None

    def convert_set_attribute(self, context, instance, attribute, value):
        context.pushTerminal(
            generateThrowException(
                context,
                AttributeError("%s object has no attribute %s" % (self, attribute))
            )
        )
        return None

    def convert_getitem(self, context, instance, item):
        context.pushTerminal(
            generateThrowException(
                context,
                AttributeError("%s is not subscriptable" % str(self))
            )
        )
        return None

    def convert_setitem(self, context, instance, index, value):
        context.pushTerminal(
            generateThrowException(
                context,
                AttributeError("%s does not support item assignment" % str(self))
            )
        )
        return None

    def convert_assign(self, context, target, toStore):
        if self.is_pod:
            assert target.isReference
            context.pushEffect(
                target.expr.store(toStore.nonref_expr)
            )
            return None
        else:
            raise NotImplementedError()

    def convert_copy_initialize(self, context, target, toStore):
        assert target.isReference
        if self.is_pod:
            context.pushEffect(
                target.expr.store(toStore.nonref_expr)
            )
            return None
        else:
            raise NotImplementedError()

    def convert_destroy(self, context, instance):
        if self.is_pod:
            return None
        else:
            raise NotImplementedError()

    def convert_default_initialize(self, context, target):
        raise NotImplementedError(self)

    def convert_call(self, context, left, args, kwargs):
        context.pushException(
            TypeError,
            "Can't call %s with args of type (%s)"
            % (
                self,
                ",".join(
                    [str(a.expr_type) for a in args]
                    + ["%s=%s" % (k, str(v.expr_type)) for k, v in kwargs.items()]
                )
            )
        )
        return None

    def convert_len(self, context, expr):
        context.pushTerminal(
            generateThrowException(
                context,
                TypeError("Can't take 'len' of instance of type '%s'" % (str(self),))
            )
        )
        return None

    def convert_unary_op(self, context, expr, op):
        context.pushTerminal(
            generateThrowException(
                context,
                TypeError("Can't apply unary op %s to type '%s'" % (op, expr.expr_type))
            )
        )
        return None

    def convert_to_type(self, context, expr, target_type):
        return target_type.convert_to_self(context, expr)

    def convert_to_self(self, context, expr):
        if expr.expr_type == self:
            return expr

        context.pushTerminal(
            generateThrowException(
                context,
                TypeError("Can't convert from type %s to type %s" % (
                          expr.expr_type.typeRepresentation.__name__,
                          self.typeRepresentation.__name__))
            )
        )
        return None

    def convert_bin_op(self, context, lhs, op, rhs):
        return rhs.expr_type.convert_bin_op_reverse(context, rhs, op, lhs)

    def convert_bin_op_reverse(self, context, rhs, op, lhs):
        context.pushTerminal(
            generateThrowException(
                context,
                TypeError("Can't apply op %s to expressions of type %s and %s" %
                          (op, str(lhs.expr_type), str(rhs.expr_type)))
            )
        )
        return None

    def convert_type_call(self, context, typeInst, args, kwargs):
        if len(args) == 0 and not kwargs:
            return context.push(self, lambda x: x.convert_default_initialize())

        if len(args) == 1 and not kwargs:
            return args[0].convert_to_type(self)

        context.pushException(
            TypeError,
            "%s() takes at most 1 positional argument" % str(self)
        )
        return None

    def convert_method_call(self, context, instance, methodname, args, kwargs):
        context.pushException(
            TypeError,
            "Can't call %s.%s with args of type (%s)" % (
                self,
                methodname,
                ",".join(
                    [str(a.expr_type) for a in args] +
                    ["%s=%s" % (k, str(v.expr_type)) for k, v in kwargs.items()]
                )
            )
        )
        return None
