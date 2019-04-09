from typed_python._types import NamedTuple, ListOf, TupleOf


def gen_named_tuple_type(name, **kwargs):
    items = kwargs.items()
    keys = kwargs.keys()
    ret = []
    ret.append(f'// "{name}"')
    for key, value in items:
        ret.append(f"//    {key}={value}")

    ret.append(f"class {name} {{")
    ret.append("public:")
    for key, value in items:
        ret.append(f"    typedef {value} {key}_type;")
    count = 1
    for key, value in items:
        offset = ""
        if count > 1:
            offset = " + " + " + ".join(["size" + str(i) for i in range(1, count)])
        ret.append(f"    {key}_type& {key}() const {{ return *({key}_type*)(data{offset}); }}")
        count += 1
    ret.append("private:")
    count = 1
    for key in keys:
        ret.append(f"    static const int size{count} = sizeof({key}_type);")
        count += 1
    total = count
    ret.append("    uint8_t data[{}];".format(" + ".join(["size" + str(i) for i in range(1, total)])))
    ret.append("public:")

    ret.append(f"    {name}& operator = (const {name}& other) {{")
    for key in keys:
        ret.append(f"        {key}() = other.{key}();")
    ret.append("        return *this;")
    ret.append("    }")
    ret.append("")

    ret.append(f"    {name}(const {name}& other) {{")
    for key in keys:
        ret.append(f"        new (&{key}()) {key}_type(other.{key}());")
    ret.append("    }")
    ret.append("")

    ret.append(f"    ~{name}() {{")
    for key in keys:
        ret.append(f"        {key}().~{key}_type();")
    ret.append("    }")
    ret.append("")

    ret.append(f"    {name}() {{")
    for key in keys:
        ret.append(f"        bool init{key} = false;")
    ret.append("        try {")
    for key in keys:
        ret.append(f"            new (&{key}()) {key}_type();")
        ret.append(f"            init{key} = true;")
    ret.append("        } catch(...) {")
    ret.append("            try {")
    for key in keys:
        ret.append(f"                if (init{key}) {key}().~{key}_type();")
    ret.append("            } catch(...) {")
    ret.append("            }")
    ret.append("            throw;")
    ret.append("        }")
    ret.append("    }")
    ret.append("};")
    ret.append("")

    ret.append("template <>")
    ret.append(f"class TypeDetails<{name}> {{")
    ret.append("public:")
    ret.append("    static Type* getType() {")
    ret.append("        static Type* t = NamedTuple::Make({")
    ret.append(",\n".join(
        [f"                TypeDetails<{name}::{key}_type>::getType()" for key in keys]))
    ret.append("            },{")
    ret.append(",\n".join(
        [f'                "{key}"' for key in keys]))
    ret.append("            });")
    ret.append("        if (t->bytecount() != bytecount) {")
    ret.append('            throw std::runtime_error("somehow we have the wrong bytecount!");')
    ret.append("        }")
    ret.append("        return t;")
    ret.append("    }")
    ret.append("    static const uint64_t bytecount = ")
    ret.append(" +\n". join([f"        sizeof({name}::{key}_type)" for key in keys]) + ";")
    ret.append("};")
    ret.append("")

    return [e + "\n" for e in ret]

# py type -> c++ direct type
# Int64 -> int64_t
# Bool -> bool
# ListOf(Int64) -> ListOf<int64_t>
# TupleOf(Bool) -> TupleOf<bool>
# for generated types with arbitrary names, just use the name
# Arb=NamedTuple(X=Int64,Y=Bool) -> Arb
# Either assume Arb is defined in a previous stage, or keep track of it
def cpp_type(py_type):
    simple_cats = {
        "Int64":"int64_t",
        "UInt64":"uint64_t",
        "Int32":"uint32_t",
        "UInt32":"uint32_t",
        "Int16":"uint16_t",
        "UInt16":"uint16_t",
        "Int8":"uint8_t",
        "UInt8":"uint8_t",
        "Bool":"bool",
        "Float64":"double",
        "Float32":"float",
        "String":"String"
    }
    cat = py_type.__typed_python_category__
    if cat in simple_cats.keys():
        return simple_cats[cat]
    # if cat == 'NamedTuple': # not supported
    #     return "NamedTuple({})".format(
    #         ", ".join(["{}={}".format(n,cpp_type(t))
    #             for n, t in zip(py_type.ElementNames, py_type.ElementTypes)])
    #         )
    if cat == 'NoneType':
        return "None"
    if cat == 'ListOf' or cat == 'TupleOf':
        return "{}<{}>".format(cat, cpp_type(py_type.ElementType))
    if cat == 'OneOf':
        return "OneOf<{}>".format(
             ", ".join([cpp_type(t) for t in py_type.Types])
             )
    return "undefined type"

def typed_python_codegen(**kwargs):
    ret = []
    for k, v in kwargs.items():
        if v.__typed_python_category__ == 'NamedTuple':
            ret += gen_named_tuple_type(k, **{n: cpp_type(t) for n, t in zip(v.ElementNames, v.ElementTypes)})
    return ret


def generate_some_types(destination):
    with open(destination, "w") as f:
        f.writelines(gen_named_tuple_type("NamedTupleBoolAndInt", X="int64_t", Y="float"))
        f.writelines(gen_named_tuple_type("NamedTupleBoolIntStr", X="int64_t", Y="float", Z="String"))
        f.writelines(gen_named_tuple_type("NamedTupleListAndTupleOfStr", items="ListOf<String>", elements="TupleOf<String>"))
        f.writelines(gen_named_tuple_type("NamedTupleTwoParts", first="NamedTupleBoolAndInt", second="NamedTupleListAndTupleOfStr"))
        typed_python_codegen(
            Blah=NamedTuple(X=int, Y=float)
        )
