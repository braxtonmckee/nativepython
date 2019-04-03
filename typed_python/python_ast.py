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

"""
Convert python AST objects into a more explicit set of
Algebraic types. These are easier to work with than the
Python ast directly.
"""

import ast
import typed_python.ast_util as ast_util
import weakref
from typed_python._types import Alternative, TupleOf, OneOf

# forward declarations.
Module = lambda: Module
Statement = lambda: Statement
Expr = lambda: Expr
Arg = lambda: Arg
NumericConstant = lambda: NumericConstant
ExprContext = lambda: ExprContext
Slice = lambda: Slice
BooleanOp = lambda: BooleanOp
BinaryOp = lambda: BinaryOp
UnaryOp = lambda: UnaryOp
ComparisonOp = lambda: ComparisonOp
Comprehension = lambda: Comprehension
ExceptionHandler = lambda: ExceptionHandler
Arguments = lambda: Arguments
Keyword = lambda: Keyword
Alias = lambda: Alias
WithItem = lambda: WithItem

Module = Alternative(
    "Module",
    Module={"body": TupleOf(Statement)},
    Expression={'body': Expr},
    Interactive={'body': TupleOf(Statement)},
    Suite={"body": TupleOf(Statement)}
)

CodeLoc = {
    'line_number': int,
    'col_offset': int,
    'filename': str
}

Statement = Alternative(
    "Statement",
    FunctionDef={
        "name": str,
        "args": Arguments,
        "body": TupleOf(Statement),
        "decorator_list": TupleOf(Expr),
        "returns": OneOf(Expr, None),
        **CodeLoc
    },

    ClassDef={
        "name": str,
        "bases": TupleOf(Expr),
        "keywords": TupleOf(Keyword),
        "body": TupleOf(Statement),
        "decorator_list": TupleOf(Expr),
        **CodeLoc
    },

    Return={
        "value": OneOf(Expr, None),
        **CodeLoc
    },
    Delete={
        "targets": TupleOf(Expr),
        **CodeLoc
    },
    Assign={
        "targets": TupleOf(Expr),
        "value": Expr,
        **CodeLoc
    },
    AugAssign={
        "target": Expr,
        "op": BinaryOp,
        "value": Expr,
        **CodeLoc
    },
    Print={
        "expr": OneOf(Expr, None),
        "values": TupleOf(Expr),
        "nl": int,
        **CodeLoc
    },
    For={
        "target": Expr,
        "iter": Expr,
        "body": TupleOf(Statement),
        "orelse": TupleOf(Statement),
        **CodeLoc
    },
    While={
        "test": Expr,
        "body": TupleOf(Statement),
        "orelse": TupleOf(Statement),
        **CodeLoc
    },
    If={
        "test": Expr,
        "body": TupleOf(Statement),
        "orelse": TupleOf(Statement),
        **CodeLoc
    },
    With={
        "items": TupleOf(WithItem),
        "body": TupleOf(Statement),
        **CodeLoc
    },
    Raise={
        "exc": OneOf(Expr, None),
        "cause": OneOf(Expr, None),
        **CodeLoc
    },
    Try={
        "body": TupleOf(Statement),
        "handlers": TupleOf(ExceptionHandler),
        "orelse": TupleOf(Statement),
        "finalbody": TupleOf(Statement),
        **CodeLoc
    },
    Assert={
        "test": Expr,
        "msg": OneOf(Expr, None),
        **CodeLoc
    },
    Import={
        "names": TupleOf(Alias),
        **CodeLoc
    },
    ImportFrom={
        "module": OneOf(str, TupleOf(str)),
        "names": OneOf(Alias, TupleOf(Alias)),
        "level": OneOf(int, None),
        **CodeLoc
    },
    Global={
        "names": TupleOf(str),
        **CodeLoc
    },
    Expr={
        "value": Expr,
        **CodeLoc
    },
    Pass={
        **CodeLoc
    },
    Break={
        **CodeLoc
    },
    Continue={
        **CodeLoc
    },
)


Expr = Alternative(
    "Expr",
    BoolOp={
        "op": BooleanOp,
        "values": TupleOf(Expr),
        **CodeLoc
    },
    BinOp={
        "left": Expr,
        "op": BinaryOp,
        "right": Expr,
        **CodeLoc
    },
    UnaryOp={
        "op": UnaryOp,
        "operand": Expr,
        **CodeLoc
    },
    Lambda={
        "args": Arguments,
        "body": Expr,
        **CodeLoc
    },
    IfExp={
        "test": Expr,
        "body": Expr,
        "orelse": Expr,
        **CodeLoc
    },
    Dict={
        "keys": TupleOf(Expr),
        "values": TupleOf(Expr),
        **CodeLoc
    },
    Set={
        "elts": TupleOf(Expr),
        **CodeLoc
    },
    ListComp={
        "elt": Expr,
        "generators": TupleOf(Comprehension),
        **CodeLoc
    },
    SetComp={
        "elt": Expr,
        "generators": TupleOf(Comprehension),
        **CodeLoc
    },
    DictComp={
        "key": Expr,
        "value": Expr,
        "generators": TupleOf(Comprehension),
        **CodeLoc
    },
    GeneratorExp={
        "elt": Expr,
        "generators": TupleOf(Comprehension),
        **CodeLoc
    },
    Yield={
        "value": OneOf(Expr, None),
        **CodeLoc
    },
    Compare={
        "left": Expr,
        "ops": TupleOf(ComparisonOp),
        "comparators": TupleOf(Expr),
        **CodeLoc
    },
    Call={
        "func": Expr,
        "args": TupleOf(Expr),
        "keywords": TupleOf(Keyword),
        **CodeLoc
    },
    Num={
        "n": NumericConstant,
        **CodeLoc
    },
    Str={
        "s": str,
        **CodeLoc
    },
    Attribute={
        "value": Expr,
        "attr": str,
        "ctx": ExprContext,
        **CodeLoc
    },
    Subscript={
        "value": Expr,
        "slice": Slice,
        "ctx": ExprContext,
        **CodeLoc
    },
    Name={
        "id": str,
        "ctx": ExprContext,
        **CodeLoc
    },
    List={
        "elts": TupleOf(Expr),
        "ctx": ExprContext,
        **CodeLoc
    },
    Tuple={
        "elts": TupleOf(Expr),
        "ctx": ExprContext,
        **CodeLoc
    },
    Starred={
        "value": Expr,
        "ctx": ExprContext,
        **CodeLoc
    },
)

NumericConstant = Alternative(
    "NumericConstant",
    Int={"value": int},
    Long={"value": str},
    Boolean={"value": bool},
    None_={},
    Float={"value": float},
    Unknown={}
)

ExprContext = Alternative(
    "ExprContext",
    Load={},
    Store={},
    Del={},
    AugLoad={},
    AugStore={},
    Param={}
)

Slice = Alternative(
    "Slice",
    Ellipsis={},
    Slice={
        "lower": OneOf(Expr, None),
        "upper": OneOf(Expr, None),
        "step": OneOf(Expr, None)
    },
    ExtSlice={"dims": TupleOf(Slice)},
    Index={"value": Expr}
)

BooleanOp = Alternative(
    "BooleanOp",
    And={},
    Or={}
)

BinaryOp = Alternative(
    "BinaryOp",
    Add={},
    Sub={},
    Mult={},
    Div={},
    Mod={},
    Pow={},
    LShift={},
    RShift={},
    BitOr={},
    BitXor={},
    BitAnd={},
    FloorDiv={}
)

UnaryOp = Alternative(
    "UnaryOp",
    Invert={},
    Not={},
    UAdd={},
    USub={}
)

ComparisonOp = Alternative(
    "ComparisonOp",
    Eq={},
    NotEq={},
    Lt={},
    LtE={},
    Gt={},
    GtE={},
    Is={},
    IsNot={},
    In={},
    NotIn={}
)

Comprehension = Alternative(
    "Comprehension",
    Item={
        "target": Expr,
        "iter": Expr,
        "ifs": TupleOf(Expr),
        "is_async": bool
    }
)

ExceptionHandler = Alternative(
    "ExceptionHandler",
    Item={
        "type": OneOf(Expr, None),
        "name": OneOf(str, None),
        "body": TupleOf(Statement),
        **CodeLoc
    }
)

Arguments = Alternative(
    "Arguments",
    Item={
        "args": TupleOf(Arg),
        "vararg": OneOf(Arg, None),
        "kwonlyargs": TupleOf(Arg),
        "kw_defaults": TupleOf(Expr),
        "kwarg": OneOf(Arg, None),
        "defaults": TupleOf(Expr)
    }
)

Arg = Alternative(
    "Arg",
    Item={
        'arg': str,
        'annotation': OneOf(Expr, None),
        **CodeLoc
    }
)

Keyword = Alternative(
    "Keyword",
    Item={
        "arg": OneOf(None, str),
        "value": Expr
    }
)

Alias = Alternative(
    "Alias",
    Item={
        "name": str,
        "asname": OneOf(str, None)
    }
)

WithItem = Alternative(
    "WithItem",
    Item={
        "context_expr": Expr,
        "optional_vars": OneOf(Expr, None),
    }
)

numericConverters = {
    int: lambda x: NumericConstant.Int(value=x),
    bool: lambda x: NumericConstant.Boolean(value=x),
    type(None): lambda x: NumericConstant.None_(),
    float: lambda x: NumericConstant.Float(value=x)
}


def createPythonAstConstant(n, **kwds):
    if type(n) not in numericConverters:
        return Expr.Num(
            n=NumericConstant.Unknown(),
            **kwds
        )
    return Expr.Num(
        n=numericConverters[type(n)](n),
        **kwds
    )


def createPythonAstString(s, **kwds):
    try:
        return Expr.Str(s=str(s), **kwds)
    except Exception:
        return Expr.Num(
            n=NumericConstant.Unknown(),
            **kwds
        )


def makeNameConstant(value, **kwds):
    return Expr.Num(n=numericConverters[type(value)](value), **kwds)


# map Python AST types to our syntax-tree types (defined above)
converters = {
    ast.Module: Module.Module,
    ast.Expression: Module.Expression,
    ast.Interactive: Module.Interactive,
    ast.Suite: Module.Suite,
    ast.FunctionDef: Statement.FunctionDef,
    ast.ClassDef: Statement.ClassDef,
    ast.Return: Statement.Return,
    ast.Delete: Statement.Delete,
    ast.Assign: Statement.Assign,
    ast.AugAssign: Statement.AugAssign,
    ast.For: Statement.For,
    ast.While: Statement.While,
    ast.If: Statement.If,
    ast.With: Statement.With,
    ast.Raise: Statement.Raise,
    ast.Try: Statement.Try,
    ast.Assert: Statement.Assert,
    ast.Import: Statement.Import,
    ast.ImportFrom: Statement.ImportFrom,
    ast.Global: Statement.Global,
    ast.Expr: Statement.Expr,
    ast.Pass: Statement.Pass,
    ast.Break: Statement.Break,
    ast.Continue: Statement.Continue,
    ast.BoolOp: Expr.BoolOp,
    ast.BinOp: Expr.BinOp,
    ast.UnaryOp: Expr.UnaryOp,
    ast.Lambda: Expr.Lambda,
    ast.IfExp: Expr.IfExp,
    ast.Dict: Expr.Dict,
    ast.Set: Expr.Set,
    ast.ListComp: Expr.ListComp,
    ast.SetComp: Expr.SetComp,
    ast.DictComp: Expr.DictComp,
    ast.GeneratorExp: Expr.GeneratorExp,
    ast.Yield: Expr.Yield,
    ast.Compare: Expr.Compare,
    ast.Call: Expr.Call,
    ast.Num: createPythonAstConstant,
    ast.Str: createPythonAstString,
    ast.Attribute: Expr.Attribute,
    ast.Subscript: Expr.Subscript,
    ast.Name: Expr.Name,
    ast.NameConstant: makeNameConstant,
    ast.List: Expr.List,
    ast.Tuple: Expr.Tuple,
    ast.Starred: Expr.Starred,
    ast.Load: ExprContext.Load,
    ast.Store: ExprContext.Store,
    ast.Del: ExprContext.Del,
    ast.AugLoad: ExprContext.AugLoad,
    ast.AugStore: ExprContext.AugStore,
    ast.Param: ExprContext.Param,
    ast.Ellipsis: Slice.Ellipsis,
    ast.Slice: Slice.Slice,
    ast.ExtSlice: Slice.ExtSlice,
    ast.Index: Slice.Index,
    ast.And: BooleanOp.And,
    ast.Or: BooleanOp.Or,
    ast.Add: BinaryOp.Add,
    ast.Sub: BinaryOp.Sub,
    ast.Mult: BinaryOp.Mult,
    ast.Div: BinaryOp.Div,
    ast.Mod: BinaryOp.Mod,
    ast.Pow: BinaryOp.Pow,
    ast.LShift: BinaryOp.LShift,
    ast.RShift: BinaryOp.RShift,
    ast.BitOr: BinaryOp.BitOr,
    ast.BitXor: BinaryOp.BitXor,
    ast.BitAnd: BinaryOp.BitAnd,
    ast.FloorDiv: BinaryOp.FloorDiv,
    ast.Invert: UnaryOp.Invert,
    ast.Not: UnaryOp.Not,
    ast.UAdd: UnaryOp.UAdd,
    ast.USub: UnaryOp.USub,
    ast.Eq: ComparisonOp.Eq,
    ast.NotEq: ComparisonOp.NotEq,
    ast.Lt: ComparisonOp.Lt,
    ast.LtE: ComparisonOp.LtE,
    ast.Gt: ComparisonOp.Gt,
    ast.GtE: ComparisonOp.GtE,
    ast.Is: ComparisonOp.Is,
    ast.IsNot: ComparisonOp.IsNot,
    ast.In: ComparisonOp.In,
    ast.NotIn: ComparisonOp.NotIn,
    ast.comprehension: Comprehension.Item,
    ast.excepthandler: lambda x: x,
    ast.ExceptHandler: ExceptionHandler.Item,
    ast.arguments: Arguments.Item,
    ast.arg: Arg.Item,
    ast.keyword: Keyword.Item,
    ast.alias: Alias.Item,
    ast.withitem: WithItem.Item,
}

# most converters map to an alternative type
reverseConverters = {
    t: v for v, t in converters.items()
    if hasattr(t, '__typed_python_category__') and t.__typed_python_category__ == "ConcreteAlternative"
}


def convertAlgebraicArgs(pyAst, *members):
    members = [x for x in members if x not in ['line_number', 'col_offset']]
    return {m: convertAlgebraicToPyAst(getattr(pyAst, m)) for m in members}


def convertAlgebraicToPyAst(pyAst):
    res = convertAlgebraicToPyAst_(pyAst)

    if hasattr(pyAst, "line_number"):
        res.lineno = pyAst.line_number
        res.col_offset = pyAst.col_offset

    return res


def convertAlgebraicToPyAst_(pyAst):
    if pyAst is None:
        return None

    if isinstance(pyAst, (str, int, float, bool)):
        return pyAst

    if hasattr(pyAst, "__typed_python_category__") and pyAst.__typed_python_category__ == "TupleOf":
        return [convertAlgebraicToPyAst(x) for x in pyAst]

    if type(pyAst) is Expr.Str:
        return ast.Str(s=pyAst.s)

    if type(pyAst) is Expr.Num:
        if pyAst.n.matches.Boolean:
            return ast.NameConstant(value=True if pyAst.n.value else False)
        if pyAst.n.matches.None_:
            return ast.NameConstant(value=None)
        return ast.Num(n=pyAst.n.value)

    if type(pyAst) in reverseConverters:
        return reverseConverters[type(pyAst)](**convertAlgebraicArgs(pyAst, *type(pyAst).ElementType.ElementNames))

    assert False, type(pyAst)


def convertPyAstToAlgebraic(tree, fname, keepLineInformation=True):
    if issubclass(type(tree), ast.AST):
        converter = converters[type(tree)]
        args = {}

        for f in tree._fields:
            args[f] = convertPyAstToAlgebraic(getattr(tree, f), fname, keepLineInformation)

        try:
            if keepLineInformation:
                args['line_number'] = tree.lineno
                args['col_offset'] = tree.col_offset
                args['filename'] = fname
            else:
                args['line_number'] = 0
                args['col_offset'] = 0
                args['filename'] = ''
        except AttributeError:
            pass

        try:
            return converter(**args)
        except Exception:
            del args['line_number']
            del args['col_offset']
            del args['filename']

            try:
                return converter(**args)
            except Exception:
                import traceback
                raise UserWarning(
                    "Failed to construct %s from %s with arguments\n%s\n\n%s" % (
                        converter,
                        type(tree),
                        "\n".join([
                            "\t%s:%s (from %s)" % (
                                k, repr(v)[:50], getattr(tree, k) if hasattr(tree, k) else None
                            )
                            for k, v in args.items()
                        ]),
                        traceback.format_exc()
                    )
                )

    if isinstance(tree, list):
        return [convertPyAstToAlgebraic(x, fname, keepLineInformation) for x in tree]

    return tree


# a nasty hack to allow us to find the Ast's of functions we have deserialized
# but for which we never had the source code.
_originalAstCache = weakref.WeakKeyDictionary()


def convertFunctionToAlgebraicPyAst(f, keepLineInformation=True):
    if f in _originalAstCache:
        return _originalAstCache[f]

    try:
        pyast = ast_util.pyAstFor(f)

        _, lineno = ast_util.getSourceLines(f)
        _, fname = ast_util.getSourceFilenameAndText(f)

        pyast = ast_util.functionDefOrLambdaAtLineNumber(pyast, lineno)
    except Exception:
        raise Exception("Failed to get source for function %s" % (f.__qualname__))

    try:
        return convertPyAstToAlgebraic(pyast, fname, keepLineInformation)
    except Exception as e:
        raise Exception("Failed to convert function at %s:%s:\n%s" % (fname, lineno, repr(e)))


def evaluateFunctionPyAst(pyAst):
    assert isinstance(pyAst, (Expr.Lambda, Statement.FunctionDef))

    filename = pyAst.filename

    if isinstance(pyAst, Statement):
        # strip out the decorator definitions. We just want the underlying function
        # object itself.
        pyAstModule = Statement.FunctionDef(
            name=pyAst.name,
            args=pyAst.args,
            body=pyAst.body,
            decorator_list=(),
            returns=pyAst.returns,
            line_number=pyAst.line_number,
            col_offset=pyAst.col_offset,
            filename=pyAst.filename,
        )
        pyAstModule = Module.Module(body=(pyAstModule,))
    elif isinstance(pyAst, Expr):
        pyAstModule = Module.Expression(body=pyAst)

    globals = {}

    if pyAstModule.matches.Expression:
        codeObject = compile(convertAlgebraicToPyAst(pyAstModule), filename, 'eval')

        res = eval(codeObject, globals)
    else:
        codeObject = compile(convertAlgebraicToPyAst(pyAstModule), filename, 'exec')
        exec(codeObject, globals)

        res = globals[pyAstModule.body[0].name]

    _originalAstCache[res] = pyAst

    return res
