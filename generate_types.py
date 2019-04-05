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


def generate_some_types(destination):
    with open(destination, "w") as f:
        f.writelines(gen_named_tuple_type("NamedTupleBoolAndInt", X="int64_t", Y="float"))
        f.writelines(gen_named_tuple_type("NamedTupleBoolIntStr", X="int64_t", Y="float", Z="String"))
        f.writelines(gen_named_tuple_type("NamedTupleListAndTupleOfStr", items="ListOf<String>", elements="TupleOf<String>"))
        f.writelines(gen_named_tuple_type("NamedTupleTwoParts", first="NamedTupleBoolAndInt", second="NamedTupleListAndTupleOfStr"))
