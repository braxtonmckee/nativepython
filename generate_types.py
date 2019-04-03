def gen_named_tuple_type(name, **kwargs):
    ret = []
    ret.append(f"class {name} {{")
    ret.append("private:")
    count = 1
    for key, value in kwargs.items():
        ret.append(f"\tstatic const int size{count} = sizeof({value});")
        count += 1
    total = count
    ret.append("\tuint8_t data[{}];".format(" + ".join(["size" + str(i) for i in range(1, total)])))
    ret.append("public:")
    count = 1
    for key, value in kwargs.items():
        ret.append("\t{}& {}() {{ return *({}*)(data{}{}); }}".format(
            value,
            key,
            value,
            (" + " if count > 1 else ""),
            (" + ".join(["size" + str(i) for i in range(1, count)]) if count > 1 else "")
        )
        )
        count += 1
    ret.append("};")
    ret.append("")
    return [e + "\n" for e in ret]


def generate_some_types(destination):
    with open(destination, "w") as f:
        f.writelines(gen_named_tuple_type("NamedTupleBoolAndInt", X="int64_t", Y="float"))
        f.writelines(gen_named_tuple_type("NamedTupleBoolIntStr", X="int64_t", Y="float", Z="String"))
        f.writelines(gen_named_tuple_type("NamedTupleListAndTupleOfStr", items="ListOf<String>", elements="TupleOf<String>"))
