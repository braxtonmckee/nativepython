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

from typed_python import Tuple, TupleOf, Alternative, NamedTuple, OneOf
import textwrap


def indent(x, indentBy="    "):
    return textwrap.indent(str(x), indentBy)


def type_attr_ix(t, attr):
    for i in range(len(t.element_types)):
        if t.element_types[i][0] == attr:
            return i
    return None


def type_str(c):
    if c.matches.Function:
        return "func((%s)->%s%s)" % (
            ",".join([str(x) for x in (c.args + tuple(["..."] if c.varargs else []))]),
            str(c.output),
            ",nothrow" if not c.can_throw else ""
        )
    if c.matches.Float:
        return "float" + str(c.bits)
    if c.matches.Int:
        return ("int" if c.signed else "uint") + str(c.bits)
    if c.matches.Struct:
        if c.name:
            return c.name
        return "(" + ",".join("%s=%s"%(k, v) for k, v in c.element_types) + ")"
    if c.matches.Pointer:
        return "*" + str(c.value_type)
    if c.matches.Void:
        return "void"
    if c.matches.Array:
        return str(c.element_type) + "[" + str(c.count) + "]"

    assert False, type(c)


def raising(e):
    raise e


Type = Alternative(
    "Type",
    Void={},
    Float={'bits': int},
    Int={'bits': int, 'signed': bool},
    Struct={'element_types': TupleOf(Tuple(str, lambda: Type)), 'name': str},
    Array={'element_type': lambda: Type, 'count': int},
    Function={'output': lambda: Type, 'args': TupleOf(lambda: Type), 'varargs': bool, 'can_throw': bool},
    Pointer={'value_type': lambda: Type},
    attr_ix=type_attr_ix,
    __str__=type_str,
    pointer=lambda self: Type.Pointer(value_type=self),
    zero=lambda self: Expression.Constant(
        Constant.Void() if self.matches.Void else
        Constant.Float(val=0.0, bits=self.bits) if self.matches.Float else
        Constant.Int(val=0, bits=self.bits, signed=self.signed) if self.matches.Int else
        Constant.Struct(elements=[(name, t.zero()) for name, t in self.element_types]) if self.matches.Struct else
        Constant.NullPointer(value_type=self.value_type) if self.matches.Pointer else
        raising(Exception("Can't make a zero value from %s" % self))
    )
)


def const_truth_value(c):
    if c.matches.Int:
        return c.val != 0
    return False


def const_str(c):
    if c.matches.Float:
        if c.bits == 64:
            return str(c.val)
        else:
            return str(c.val) + "f32"
    if c.matches.ByteArray:
        return "ByteArray(%s)" % repr(c.val)
    if c.matches.Int:
        if c.bits == 64:
            return str(c.val)
        else:
            return str(c.val) + ("s" if c.signed else "u") + str(c.bits)
    if c.matches.Struct:
        return "(" + ",".join("%s=%s"%(k, v) for k, v in c.elements) + ")"
    if c.matches.NullPointer:
        return "nullptr"
    if c.matches.Void:
        return "void"

    assert False, type(c)


Constant = Alternative(
    "Constant",
    Void={},
    Float={'val': float, 'bits': int},
    Int={'val': int, 'bits': int, 'signed': bool},
    Struct={'elements': TupleOf(Tuple(str, lambda: Constant))},
    ByteArray={'val': bytes},
    NullPointer={'value_type': Type},
    truth_value=const_truth_value,
    __str__=const_str
)

UnaryOp = Alternative(
    "UnaryOp",
    Add={},
    Negate={},
    LogicalNot={},
    BitwiseNot={},
    __str__=lambda o:
        "+" if o.matches.Add else
        "-" if o.matches.Negate else
        "!" if o.matches.LogicalNot else
        "~" if o.matches.BitwiseNot else "unknown unary op"
)

BinaryOp = Alternative(
    "BinaryOp",
    Add={}, Sub={}, Mul={}, Div={}, Eq={},
    NotEq={}, Lt={}, LtE={}, Gt={}, GtE={},
    Mod={}, LShift={}, RShift={},
    BitOr={}, BitAnd={}, BitXor={},
    __str__=lambda o:
        "+" if o.matches.Add else
        "-" if o.matches.Sub else
        "*" if o.matches.Mul else
        "/" if o.matches.Div else
        "==" if o.matches.Eq else
        "!=" if o.matches.NotEq else
        "<" if o.matches.Lt else
        "<=" if o.matches.LtE else
        ">" if o.matches.Gt else
        ">=" if o.matches.GtE else
        "<<" if o.matches.LShift else
        ">>" if o.matches.RShift else
        "|" if o.matches.BitOr else
        "&" if o.matches.BitAnd else
        "^" if o.matches.BitXor else
        "unknown binary op"
)


# loads and stores - no assignments
Expression = lambda: Expression
Teardown = lambda: Teardown

NamedCallTarget = NamedTuple(
    name=str,
    arg_types=TupleOf(Type),
    output_type=Type,
    external=bool,
    varargs=bool,
    intrinsic=bool,
    can_throw=bool
)


def filterCallTargetArgs(args):
    """Given a list of native expressions or typed expressions, filter them down,
    dropping 'empty' arguments, as per our calling convention."""
    res = []
    for a in args:
        if isinstance(a, int):
            res.append(const_int_expr(a))
        elif isinstance(a, float):
            res.append(const_float_expr(a))
        elif isinstance(a, Expression):
            res.append(a)
        elif a.expr_type.is_empty:
            pass
        else:
            if a.expr_type.is_pass_by_ref:
                assert a.isReference
                res.append(a.expr)
            else:
                res.append(a.nonref_expr)
    return res


CallTarget = Alternative(
    "CallTarget",
    Named={'target': NamedCallTarget},
    Pointer={'expr': Expression},
    call=lambda self, *args: Expression.Call(target=self, args=filterCallTargetArgs(args))
)


def teardown_str(self):
    if self.matches.Always:
        return str(self.expr)
    if self.matches.ByTag:
        return "if slot_initialized(name=%s):\n" % self.tag + indent(str(self.expr), "    ")
    assert False, type(self)


Teardown = Alternative(
    "Teardown",
    ByTag={'tag': str, 'expr': Expression},
    Always={'expr': Expression},
    __str__=teardown_str
)


def expr_concatenate(self, other):
    if self.matches.Constant:
        return other

    if self.matches.Sequence and other.matches.Sequence:
        return Expression.Sequence(vals=self.vals + other.vals)
    if self.matches.Sequence:
        return Expression.Sequence(vals=self.vals + (other,))
    if other.matches.Sequence:
        return Expression.Sequence(vals=TupleOf(Expression)((self,)) + other.vals)
    return Expression.Sequence(vals=(self, other))


def expr_str(self):
    if self.matches.Comment:
        e = str(self.expr)
        if "\n" in self.comment or "\n" in e:
            return "\n\t/*" + self.comment + "*/\n"\
                + indent(str(self.expr).rstrip(), "    ") + "\n"
        else:
            return "/*" + str(self.comment) + "*/" + str(self.expr)
    if self.matches.Constant:
        return str(self.val)
    if self.matches.Load:
        return "(" + str(self.ptr) + ").load"
    if self.matches.Store:
        return "(" + str(self.ptr) + ")[0]=" + str(self.val)
    if self.matches.AtomicAdd:
        return "atomic_add(" + str(self.ptr) + "," + str(self.val) + ")"
    if self.matches.Alloca:
        return "alloca(" + str(self.type) + ")"
    if self.matches.Cast:
        return "cast(%s,%s)" % (str(self.left), str(self.to_type))
    if self.matches.Binop:
        return "((%s)%s(%s))" % (str(self.left), str(self.op), str(self.right))
    if self.matches.Unaryop:
        return "(%s(%s))" % (str(self.op), str(self.operand))
    if self.matches.Variable:
        return self.name
    if self.matches.Attribute:
        return "(" + str(self.left) + ")." + self.attr
    if self.matches.ElementPtr:
        return "(" + str(self.left) + ").gep" + \
            "(" + ",".join(str(r) for r in self.offsets) + ")"
    if self.matches.StructElementByIndex:
        return "(" + str(self.left) + ")[%s]" % self.index
    if self.matches.Call:
        if self.target.matches.Named:
            return "call(" + str(self.target.target.name) + "->"\
                + str(self.target.target.output_type) + ")"\
                + "(" + ",".join(str(r) for r in self.args) + ")"
        else:
            return "call(" + str(self.target.expr) + ")"\
                + "(" + ",".join(str(r) for r in self.args) + ")"

    if self.matches.Branch:
        t = str(self.true)
        f = str(self.false)
        if "\n" in t or "\n" in f:
            return "if " + str(self.cond) + ":\n"\
                + indent(t).rstrip() + ("\nelse:\n" + indent(f).rstrip() if self.false != nullExpr else "")
        else:
            return "((%s) if %s else (%s))" % (t, str(self.cond), f)
    if self.matches.MakeStruct:
        return "struct(" + \
            ",".join("%s=%s" % (k, str(v)) for k, v in self.args) + ")"
    if self.matches.While:
        t = str(self.while_true)
        f = str(self.orelse)
        return "while " + str(self.cond) + ":\n"\
               + indent(t).rstrip() + "\nelse:\n" + indent(f).rstrip()
    if self.matches.Return:
        s = (str(self.arg) if self.arg is not None else "")
        if "\n" in s:
            return "return (" + s + ")"
        return "return " + s
    if self.matches.Let:
        if self.val.matches.Sequence and len(self.val.vals) > 1:
            return str(
                Expression.Sequence(
                    vals=(
                        self.val.vals[:-1] +
                        (Expression.Let(
                            var=self.var,
                            val=self.val.vals[-1],
                            within=self.within), )
                    )
                )
            )
        valstr = str(self.val)
        if "\n" in valstr:
            return self.var + " = (\n" + indent(valstr).rstrip() + ")\n"\
                + str(self.within)
        else:
            return self.var + " = " + str(self.val) + "\n" + str(self.within)
    if self.matches.Sequence:
        return "\n".join(str(x) for x in self.vals)
    if self.matches.Finally:
        return (
            "try:\n" + indent(str(self.expr)) + "\nfinally:\n"
            + indent("\n".join(str(x) for x in self.teardowns))
        )
    if self.matches.TryCatch:
        return (
            "try:\n" + indent(str(self.expr)) + "\n" +
            "catch %s:\n" % self.varname + indent(str(self.handler)).rstrip()
        )
    if self.matches.FunctionPointer:
        return "&func(name=%s,(%s)->%s%s%s)" % (
            self.target.name,
            ",".join([str(x) for x in (self.target.arg_types + tuple(["..."] if self.target.varargs else []))]),
            str(self.target.output_type),
            ",nothrow" if not self.target.can_throw else "",
            ",intrinsic" if self.target.intrinsic else ""
        )
        return "&func(%s)" % str(self.expr)
    if self.matches.Throw:
        return "throw (%s)" % str(self.expr)
    if self.matches.ActivatesTeardown:
        return "mark slot %s initialized" % self.name
    if self.matches.StackSlot:
        return "slot(name=%s,t=%s)" % (self.name, str(self.type))

    assert False


def expr_is_simple(expr):
    if expr.matches.StackSlot:
        return True
    if expr.matches.Constant:
        return True
    if expr.matches.Variable:
        return True
    if expr.matches.ElementPtr:
        return expr_is_simple(expr.left) and all(expr_is_simple(x) for x in expr.offsets)
    if expr.matches.Load:
        return expr_is_simple(expr.ptr)
    if expr.matches.Sequence and len(expr.vals) == 1:
        return expr_is_simple(expr.vals[0])
    return False


Expression = Alternative(
    "Expression",
    Constant={'val': Constant},
    Comment={'comment': str, 'expr': Expression},
    Load={'ptr': Expression},
    Store={'ptr': Expression, 'val': Expression},
    AtomicAdd={'ptr': Expression, 'val': Expression},
    Alloca={'type': Type},
    Cast={'left': Expression, 'to_type': Type},
    Binop={'op': BinaryOp, 'left': Expression, 'right': Expression},
    Unaryop={'op': UnaryOp, 'operand': Expression},
    Variable={'name': str},
    Attribute={'left': Expression, 'attr': str},
    StructElementByIndex={'left': Expression, 'index': int},
    ElementPtr={'left': Expression, 'offsets': TupleOf(Expression)},
    Call={'target': CallTarget, 'args': TupleOf(Expression)},
    FunctionPointer={'target': NamedCallTarget},
    MakeStruct={'args': TupleOf(Tuple(str, Expression))},
    Branch={
        'cond': Expression,
        'true': Expression,
        'false': Expression
    },
    Throw={'expr': Expression },  # throw a pointer.
    TryCatch={
        'expr': Expression,
        'varname': str,  # varname is bound to a int8*
        'handler': Expression
    },
    While={
        'cond': Expression,
        'while_true': Expression,
        'orelse': Expression
    },
    Return={'arg': OneOf(Expression, None)},
    Let={'var': str, 'val': Expression, 'within': Expression},
    # evaluate 'expr', and then call teardowns if we passed through a named 'ActivatesTeardown'
    # clause
    Finally={'expr': Expression, 'teardowns': TupleOf(Teardown)},
    Sequence={'vals': TupleOf(Expression)},
    ActivatesTeardown={'name': str},
    StackSlot={'name': str, 'type': Type},
    ElementPtrIntegers=lambda self, *offsets:
        Expression.ElementPtr(
            left=self,
            offsets=tuple(
                Expression.Constant(
                    val=Constant.Int(bits=32, signed=True, val=index)
                )
                for index in offsets
            )
    ),
    __rshift__=expr_concatenate,
    __str__=expr_str,
    structElt=lambda self, ix: Expression.StructElementByIndex(left=self, index=ix),
    logical_not=lambda self: Expression.Unaryop(op=UnaryOp.LogicalNot(), operand=self),
    bitwise_not=lambda self: Expression.Unaryop(op=UnaryOp.BitwiseNot(), operand=self),
    negate=lambda self: Expression.Unaryop(op=UnaryOp.Negate(), operand=self),
    sub=lambda self, other: Expression.Binop(op=BinaryOp.Sub(), left=self, right=ensureExpr(other)),
    add=lambda self, other: Expression.Binop(op=BinaryOp.Add(), left=self, right=ensureExpr(other)),
    mul=lambda self, other: Expression.Binop(op=BinaryOp.Mul(), left=self, right=ensureExpr(other)),
    div=lambda self, other: Expression.Binop(op=BinaryOp.Div(), left=self, right=ensureExpr(other)),
    eq=lambda self, other: Expression.Binop(op=BinaryOp.Eq(), left=self, right=ensureExpr(other)),
    neq=lambda self, other: Expression.Binop(op=BinaryOp.NotEq(), left=self, right=ensureExpr(other)),
    lt=lambda self, other: Expression.Binop(op=BinaryOp.Lt(), left=self, right=ensureExpr(other)),
    gt=lambda self, other: Expression.Binop(op=BinaryOp.Gt(), left=self, right=ensureExpr(other)),
    lte=lambda self, other: Expression.Binop(op=BinaryOp.LtE(), left=self, right=ensureExpr(other)),
    gte=lambda self, other: Expression.Binop(op=BinaryOp.GtE(), left=self, right=ensureExpr(other)),
    lshift=lambda self, other: Expression.Binop(op=BinaryOp.LShift(), left=self, right=ensureExpr(other)),
    rshift=lambda self, other: Expression.Binop(op=BinaryOp.RShift(), left=self, right=ensureExpr(other)),
    bitand=lambda self, other: Expression.Binop(op=BinaryOp.BitAnd(), left=self, right=ensureExpr(other)),
    bitor=lambda self, other: Expression.Binop(op=BinaryOp.BitOr(), left=self, right=ensureExpr(other)),
    load=lambda self: Expression.Load(ptr=self),
    store=lambda self, val: Expression.Store(ptr=self, val=ensureExpr(val)),
    atomic_add=lambda self, val: Expression.AtomicAdd(ptr=self, val=ensureExpr(val)),
    cast=lambda self, targetType: Expression.Cast(left=self, to_type=targetType),
    with_comment=lambda self, c: Expression.Comment(comment=c, expr=self),
    elemPtr=lambda self, *exprs: Expression.ElementPtr(left=self, offsets=[ensureExpr(e) for e in exprs]),
    is_simple=expr_is_simple
)


def ensureExpr(x):
    if isinstance(x, int):
        return const_int_expr(x)
    if isinstance(x, float):
        return const_float_expr(x)
    if isinstance(x, str):
        return const_utf8_cstr(x)
    if isinstance(x, Expression):
        return x
    return x.nonref_expr


nullExpr = Expression.Constant(val=Constant.Void())
emptyStructExpr = Expression.Constant(val=Constant.Struct(elements=[]))
trueExpr = Expression.Constant(val=Constant.Int(bits=1, val=1, signed=False))
falseExpr = Expression.Constant(val=Constant.Int(bits=1, val=0, signed=False))


def const_float_expr(f):
    return Expression.Constant(
        val=Constant.Float(bits=64, val=f)
    )


def const_int_expr(i):
    return Expression.Constant(
        val=Constant.Int(bits=64, val=i, signed=True)
    )


def const_int32_expr(i):
    return Expression.Constant(
        val=Constant.Int(bits=32, val=i, signed=True)
    )


def const_uint8_expr(i):
    return Expression.Constant(
        val=Constant.Int(bits=8, val=i, signed=False)
    )


def const_bool_expr(i):
    return Expression.Constant(
        val=Constant.Int(bits=1, val=i, signed=False)
    )


def const_utf8_cstr(i):
    return Expression.Constant(
        val=Constant.ByteArray(val=i.encode('utf-8'))
    )


def const_bytes_cstr(i):
    return Expression.Constant(
        val=Constant.ByteArray(val=i)
    )


FunctionBody = Alternative(
    "FunctionBody",
    Internal={'body': Expression},
    External={'name': str}
)

Function = NamedTuple(
    args=TupleOf(Tuple(str, Type)),
    body=FunctionBody,
    output_type=Type
)

Void = Type.Void()
VoidPtr = Void.pointer()
Bool = Type.Int(bits=1, signed=False)
UInt8 = Type.Int(bits=8, signed=False)
UInt8Ptr = UInt8.pointer()
Int8Ptr = Type.Pointer(value_type=Type.Int(bits=8, signed=True))
Float64 = Type.Float(bits=64)
Int64 = Type.Int(bits=64, signed=True)
Int32 = Type.Int(bits=32, signed=True)


def var(name):
    return Expression.Variable(name=name)
