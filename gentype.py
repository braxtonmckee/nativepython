def gen_named_tuple_type(name, **kwargs):
    print("class {} {{".format(name))
    print("private:")
    count = 1
    for key, value in kwargs.items():
        # print("\tstatic const int size{} = TypeDetails<{}>::bytecount;".format(count, value))
        print("\tstatic const int size{} = sizeof({});".format(count, value))
        count += 1
    total = count
    print("\tuint8_t data[{}];".format(" + ".join(["size" + str(i) for i in range(1, total)])))
    print("public:")
    count = 1
    for key, value in kwargs.items():
        print("\t{}& {}() {{ return *({}*)(data{}{}); }}".format(
            value,
            key,
            value,
            (" + " if count > 1 else ""),
            (" + ".join(["size" + str(i) for i in range(1, count)]) if count > 1 else "")
        )
        )
        count += 1
    print("};")
    print()


gen_named_tuple_type("NamedTupleBoolAndInt", X="int64_t", Y="float")
gen_named_tuple_type("NamedTupleBoolIntStr", X="int64_t", Y="float", Z="str")
gen_named_tuple_type("NamedTupleListAndTupleOfStr", items="ListOf<str>", elements="TupleOf<str>")
