#include "PythonSerializationContext.hpp"
#include "AllTypes.hpp"
#include "PyInstance.hpp"

// virtual
void PythonSerializationContext::serializePythonObject(PyObject* o, SerializationBuffer& b) const {
    PyEnsureGilAcquired acquireTheGil;

    Type* t = PyInstance::extractTypeFrom(o->ob_type);

    if (t) {
        b.write_uint8(T_NATIVE);
        //we have a natural serialization mechanism already
        serializeNativeType(t, b);

        PyEnsureGilReleased releaseTheGil;
        t->serialize(((PyInstance*)o)->dataPtr(), b);

    } else {
        if (o == Py_None) {
            b.write_uint8(T_NONE);
        } else
        if (o == Py_True) {
            b.write_uint8(T_TRUE);
        } else
        // The checks for 'True' and 'False' must happen before the test for PyLong
        // because bool is a subtype of int in Python
        if (o == Py_False) {
            b.write_uint8(T_FALSE);
        } else
        if (PyLong_CheckExact(o)) {
            b.write_uint8(T_LONG);
            b.write_int64(PyLong_AsLong(o));
        } else
        if (PyFloat_CheckExact(o)) {
            b.write_uint8(T_FLOAT);
            b.write_double(PyFloat_AsDouble(o));
        } else
        if (PyComplex_CheckExact(o)) {
            throw std::runtime_error(std::string("`complex` objects cannot be serialized yet"));
        } else
        if (PyBytes_CheckExact(o)) {
            b.write_uint8(T_BYTES);
            b.write_uint32(PyBytes_GET_SIZE(o));
            b.write_bytes((uint8_t*)PyBytes_AsString(o), PyBytes_GET_SIZE(o));
        } else
        if (PyUnicode_CheckExact(o)) {
            b.write_uint8(T_UNICODE);
            Py_ssize_t sz;
            const char* c = PyUnicode_AsUTF8AndSize(o, &sz);
            b.write_uint32(sz);
            b.write_bytes((uint8_t*)c, sz);
        } else
        if (PyList_CheckExact(o)) {
            b.write_uint8(T_LIST);
            serializePyList(o, b);
        } else
        if (PyTuple_CheckExact(o)) {
            b.write_uint8(T_TUPLE);
            serializePyTuple(o, b);
        } else
        if (PySet_CheckExact(o)) {
            b.write_uint8(T_SET);
            serializePySet(o, b);
        } else
        if (PyFrozenSet_CheckExact(o)) {
            b.write_uint8(T_FROZENSET);
            serializePyFrozenSet(o, b);
        } else
        if (PyDict_CheckExact(o)) {
            b.write_uint8(T_DICT);
            serializePyDict(o, b);
        } else
        if (PyType_Check(o)) {
            Type* nativeType = PyInstance::extractTypeFrom((PyTypeObject*)o);

            if (nativeType) {
                b.write_uint8(T_NATIVETYPE);
                serializeNativeType(nativeType, b);
                return;
            } else {
                b.write_uint8(T_OBJECT);
                serializePythonObjectNamedOrAsObj(o, b);
            }
        } else {
            b.write_uint8(T_OBJECT);
            serializePythonObjectNamedOrAsObj(o, b);
        }
    }
}

void PythonSerializationContext::serializePyDict(PyObject* o, SerializationBuffer& b) const {
    PyEnsureGilAcquired acquireTheGil;

    static Type* anyPyObjType = PythonObjectOfType::AnyPyObject();

    uint32_t id;
    bool isNew;
    std::tie(id, isNew) = b.cachePointer(o, anyPyObjType);
    b.write_uint32(id);

    if (isNew) {
        b.write_uint32(PyDict_Size(o));

        PyObject *key, *value;
        Py_ssize_t pos = 0;

        int i = 0;

        while (PyDict_Next(o, &pos, &key, &value)) {
            serializePythonObject(key, b);
            serializePythonObject(value, b);
        }
    }
}

PyObject* PythonSerializationContext::deserializePydict(DeserializationBuffer& b) const {
    PyEnsureGilAcquired acquireTheGil;

    uint32_t id = b.read_uint32();

    PyObject* res = (PyObject*)b.lookupCachedPointer(id);
    if (res) {
        return incref(res);
    }

    res = PyDict_New();

    static Type* anyPyObjType = PythonObjectOfType::AnyPyObject();

    b.addCachedPointer(id, incref(res), anyPyObjType);

    size_t sz = b.read_uint32();

    try {
        for (long k = 0; k < sz; k++) {
            PyObject* key = deserializePythonObject(b);
            PyObject* value = deserializePythonObject(b);

            if (PyDict_SetItem(res, key, value) != 0) {
                throw PythonExceptionSet();
            }

            decref(key);
            decref(value);
        }
    } catch(...) {
        decref(res);
        throw;
    }

    return res;
}

PyObject* PythonSerializationContext::deserializePyFrozenSet(DeserializationBuffer &b) const {
    PyEnsureGilAcquired acquireTheGil;

    uint32_t id = b.read_uint32();

    PyObject* res = (PyObject*)b.lookupCachedPointer(id);
    if (res) {
        return incref(res);
    }

    size_t sz = b.read_uint32();

    res = PyFrozenSet_New(NULL);
    if (!res) {
        PyErr_PrintEx(1);
        throw std::runtime_error(
            std::string("Failed to allocate memory for frozen set deserialization"));
    }

    static Type* anyPyObjType = PythonObjectOfType::AnyPyObject();

    b.addCachedPointer(id, incref(res), anyPyObjType);

    try {
        for (long k = 0; k < sz; k++) {
            PyObject* item = deserializePythonObject(b);
            if (!item) {
                throw std::runtime_error(std::string("object in frozenset couldn't be deserialized"));
            }
            // In the process of deserializing a member, we may have increfed the frozenset
            // currently being deserialized. In that case PySet_Add will fail, so we temporarily
            // decref it.
            auto refcount = Py_REFCNT(res);
            for (int i = 1; i < refcount; i++) decref(res);

            int success = PySet_Add(res, item);

            for (int i = 1; i < refcount; i++) incref(res);

            decref(item);
            if (success < 0) {
                PyErr_PrintEx(1);
                throw std::runtime_error(std::string("Call to PySet_Add failed"));
            }
        }
    } catch(...) {
        PySet_Clear(res);
        decref(res);
        throw;
    }

    return res;
}

void PythonSerializationContext::serializePythonObjectNamedOrAsObj(PyObject* o, SerializationBuffer& b) const {
    PyEnsureGilAcquired acquireTheGil;

    static Type* anyPyObjType = PythonObjectOfType::AnyPyObject();

    uint32_t id;
    bool isNew;
    std::tie(id, isNew) = b.cachePointer(o, anyPyObjType);
    b.write_uint32(id);

    if (!isNew) {
        return;
    }

    //see if the object has a name
    PyObjectStealer typeName(PyObject_CallMethod(mContextObj, "nameForObject", "O", o));
    if (!typeName) {
        throw PythonExceptionSet();
    }
    if (typeName != Py_None) {
        if (!PyUnicode_Check(typeName)) {
            throw std::runtime_error(std::string("nameForObject returned a non-string"));
        }

        b.write_uint8(T_OBJECT_NAMED);
        b.write_string(std::string(PyUnicode_AsUTF8(typeName)));
        return;
    }

    //give the plugin a chance to convert the instance to something else
    PyObjectStealer representation(PyObject_CallMethod(mContextObj, "representationFor", "O", o));
    if (!representation) {
        throw PythonExceptionSet();
    }

    if (representation == Py_None) {
        //check whether this is a type derived from a serializable native type, which we don't support
        if (PyLong_Check(o)) {
            throwDerivedClassError("long");
        }
        if (PyFloat_Check(o)) {
            throwDerivedClassError("float");
        }
        if (PyBytes_Check(o)) {
            throwDerivedClassError("bytes");
        }
        if (PyComplex_Check(o)) {
            throwDerivedClassError("complex");
        }
        if (PyUnicode_Check(o)) {
            throwDerivedClassError("str");
        }
        if (PyList_Check(o)) {
            throwDerivedClassError("list");
        }
        if (PyTuple_Check(o)) {
            throwDerivedClassError("tuple");
        }
        if (PySet_Check(o)) {
            throwDerivedClassError("set");
        }
        if (PyFrozenSet_Check(o)) {
            throwDerivedClassError("frozenset");
        }
        if (PyDict_Check(o)) {
            throwDerivedClassError("dict");
        }
        if (PyModule_Check(o)) {
            throw std::runtime_error(
                std::string("Cannot serialize module '") + PyModule_GetName(o) + ("' because it's not explicitly named. "
                    "Please ensure that it's imported as a module variable somewhere in your codebase.")
                );
        }

        //we did nothing interesting
        b.write_uint8(T_OBJECT_TYPEANDDICT);
        serializePythonObject((PyObject*)o->ob_type, b);

        PyObjectStealer d(PyObject_GenericGetDict(o, nullptr));
        if (!d) {
            throw std::runtime_error(std::string("Object of type ") + o->ob_type->tp_name + " had no dict.");
        }
        serializePyDict(d, b);
    } else {
        b.write_uint8(T_OBJECT_REPRESENTATION);
        serializePyRepresentation(representation, b);
    }
}

void PythonSerializationContext::serializePyRepresentation(PyObject* representation, SerializationBuffer& b) const {
    PyEnsureGilAcquired acquireTheGil;

    if (!PyTuple_Check(representation) || PyTuple_Size(representation) != 3) {
        decref(representation);
        throw std::runtime_error("representationFor should return None or a tuple with 3 things");
    }
    if (!PyTuple_Check(PyTuple_GetItem(representation, 1))) {
        decref(representation);
        throw std::runtime_error("representationFor second arguments should be a tuple");
    }
    serializePythonObject(PyTuple_GetItem(representation, 0), b);
    serializePythonObject(PyTuple_GetItem(representation, 1), b);
    serializePythonObject(PyTuple_GetItem(representation, 2), b);
}

PyObject* PythonSerializationContext::deserializePythonObjectNamedOrAsObj(DeserializationBuffer& b) const {
    PyEnsureGilAcquired acquireTheGil;

    static Type* anyPyObjType = PythonObjectOfType::AnyPyObject();

    uint32_t id = b.read_uint32();
    PyObject* p = (PyObject*)b.lookupCachedPointer(id);
    if (p) {
        return incref(p);
    }

    uint8_t code = b.read_uint8();

    if (code == T_OBJECT_TYPEANDDICT) {
        //no representation
        PyObject* t = deserializePythonObject(b);
        if (!PyType_Check(t)) {
            std::string tname = t->ob_type->tp_name;
            decref(t);
            throw std::runtime_error("Expected a type object. Got " + tname + " instead");
        }

        PyObject* result = ((PyTypeObject*)t)->tp_new(((PyTypeObject*)t), PyTuple_Pack(0), NULL);

        if (!result) {
            throw std::runtime_error("tp_new for " + std::string(((PyTypeObject*)t)->tp_name) + " threw an exception");
        }

        b.addCachedPointer(id, incref(result), anyPyObjType);

        PyObjectStealer d(deserializePydict(b));
        PyObject_GenericSetDict(result, d, nullptr);
        return result;
    } else
    if (code == T_OBJECT_REPRESENTATION) {
        return deserializePyRepresentation(b, id);
    } else
    if (code == T_OBJECT_NAMED) {
        std::string name = b.readString();

        PyObject* res = PyObject_CallMethod(mContextObj, "objectFromName", "s", name.c_str());

        if (!res) {
            throw PythonExceptionSet();
        }

        b.addCachedPointer(id, incref(res), anyPyObjType);

        return res;
    } else {
        throw std::runtime_error("corrupt data: invalid code after T_OBJECT");
    }
}

PyObject* PythonSerializationContext::deserializePyRepresentation(DeserializationBuffer& b, int32_t id) const {
    PyEnsureGilAcquired acquireTheGil;

    PyObject* t = deserializePythonObject(b);
    PyObject* tArgs = deserializePythonObject(b);

    if (!PyTuple_Check(tArgs)) {
        throw std::runtime_error("corrupt data: second reconstruction argument is not a tuple.");
    }

    PyObject* instance = PyObject_Call(t, tArgs, NULL);
    decref(t);
    decref(tArgs);

    if (!instance) {
        throw PythonExceptionSet();
    }

    if (id >= 0) {
        static Type* anyPyObjType = PythonObjectOfType::AnyPyObject();

        b.addCachedPointer(id, incref(instance), anyPyObjType);
    }

    PyObject* rep = deserializePythonObject(b);
    PyObject* res = PyObject_CallMethod(mContextObj, "setInstanceStateFromRepresentation", "OO", instance, rep);
    decref(rep);

    if (!res) {
        throw PythonExceptionSet();
    }
    if (res != Py_True) {
        decref(res);
        throw std::runtime_error("setInstanceStateFromRepresentation didn't return True.");
    }
    decref(res);

    return instance;
}

void PythonSerializationContext::serializeNativeType(Type* nativeType, SerializationBuffer& b, bool allowCaching) const {
    PyEnsureGilAcquired acquireTheGil;

    PyObjectStealer nameForObject(PyObject_CallMethod(mContextObj, "nameForObject", "O", PyInstance::typeObj(nativeType)));

    if (!nameForObject) {
        throw PythonExceptionSet();
    }

    if (nameForObject != Py_None) {
        if (!PyUnicode_Check(nameForObject)) {
            decref(nameForObject);
            throw std::runtime_error("nameForObject returned something other than None or a string.");
        }

        b.write_uint8(T_OBJECT_NAMED);
        b.write_string(std::string(PyUnicode_AsUTF8(nameForObject)));
        return;
    }

    PyObjectStealer representation(
        PyObject_CallMethod(mContextObj, "representationFor", "O", PyInstance::typeObj(nativeType))
        );

    if (!representation) {
        throw PythonExceptionSet();
    }

    if (representation != Py_None) {
        b.write_uint8(T_OBJECT_REPRESENTATION);

        serializePyRepresentation(representation, b);

        return;
    }

    b.write_uint8(T_NATIVETYPE_BY_CATEGORY);

    if (allowCaching) {
        std::pair<uint32_t, bool> idAndIsNew = b.cachePointerToType(nativeType);
        b.write_uint32(idAndIsNew.first);

        if (!idAndIsNew.second) {
            return;
        }
    }

    b.write_uint8(nativeType->getTypeCategory());

    if (nativeType->getTypeCategory() == Type::TypeCategory::catInt64 ||
            nativeType->getTypeCategory() == Type::TypeCategory::catFloat64 ||
            nativeType->getTypeCategory() == Type::TypeCategory::catNone ||
            nativeType->getTypeCategory() == Type::TypeCategory::catBytes ||
            nativeType->getTypeCategory() == Type::TypeCategory::catString ||
            nativeType->getTypeCategory() == Type::TypeCategory::catBool
            ) {
        return;
    }

    if (nativeType->getTypeCategory() == Type::TypeCategory::catConcreteAlternative) {
        serializeNativeType(nativeType->getBaseType(), b);
        b.write_uint32(((ConcreteAlternative*)nativeType)->which());
        return;
    }

    if (nativeType->getTypeCategory() == Type::TypeCategory::catConstDict) {
        serializeNativeType(((ConstDict*)nativeType)->keyType(), b);
        serializeNativeType(((ConstDict*)nativeType)->valueType(), b);
        return;
    }

    if (nativeType->getTypeCategory() == Type::TypeCategory::catDict) {
        serializeNativeType(((Dict*)nativeType)->keyType(), b);
        serializeNativeType(((Dict*)nativeType)->valueType(), b);
        return;
    }

    if (nativeType->getTypeCategory() == Type::TypeCategory::catTupleOf) {
        serializeNativeType(((TupleOfType*)nativeType)->getEltType(), b);
        return;
    }

    if (nativeType->getTypeCategory() == Type::TypeCategory::catListOf) {
        serializeNativeType(((ListOfType*)nativeType)->getEltType(), b);
        return;
    }

    if (nativeType->getTypeCategory() == Type::TypeCategory::catTuple) {
        b.write_uint32(((CompositeType*)nativeType)->getTypes().size());
        for (auto t: ((CompositeType*)nativeType)->getTypes()) {
            serializeNativeType(t, b);
        }
        return;
    }

    if (nativeType->getTypeCategory() == Type::TypeCategory::catNamedTuple) {
        b.write_uint32(((CompositeType*)nativeType)->getTypes().size());
        for (long k = 0; k < ((CompositeType*)nativeType)->getTypes().size(); k++) {
            b.write_string(((CompositeType*)nativeType)->getNames()[k]);
            serializeNativeType(((CompositeType*)nativeType)->getTypes()[k], b, true);
        }
        return;
    }

    if (nativeType->getTypeCategory() == Type::TypeCategory::catOneOf) {
        b.write_uint32(((OneOf*)nativeType)->getTypes().size());
        for (auto t: ((OneOf*)nativeType)->getTypes()) {
            serializeNativeType(t, b);
        }
        return;
    }

    if (nativeType->getTypeCategory() == Type::TypeCategory::catPythonObjectOfType) {
        serializePythonObject((PyObject*)((PythonObjectOfType*)nativeType)->pyType(), b);
        return;
    }

    if (nativeType->getTypeCategory() == Type::TypeCategory::catValue) {
        Instance i = ((Value*)nativeType)->value();
        serializeNativeType(i.type(), b);

        PyEnsureGilReleased releaseTheGil;
        i.type()->serialize(i.data(), b);

        return;
    }

    throw std::runtime_error("Can't serialize native type " + nativeType->name() + " if its unnamed.");
}

Type* PythonSerializationContext::deserializeNativeType(DeserializationBuffer& b, bool allowCaching) const {
    PyEnsureGilAcquired acquireTheGil;

    uint8_t style = b.read_uint8();

    if (style == T_OBJECT_NAMED) {
        //it's a named type
        std::string name = b.readString();
        PyObject* res = PyObject_CallMethod(mContextObj, "objectFromName", "s", name.c_str());

        if (!res) {
            throw PythonExceptionSet();
        }

        if (!PyType_Check(res)) {
            std::string msg = "objectFromName returned a non-type for name " + name + ". it has type " + res->ob_type->tp_name;
            decref(res);
            throw std::runtime_error(msg);
        }

        Type* resultType = PyInstance::extractTypeFrom((PyTypeObject*)res);
        decref(res);

        if (!resultType) {
            throw std::runtime_error("we expected objectFromName to return a native type in this context, but it didn't.");
        }
        return resultType;
    }

    if (style == T_OBJECT_REPRESENTATION) {
        //the -1 code indicates we don't want to memoize this. That should have happened at an outer layer.
        PyObject* res = deserializePyRepresentation(b, -1);

        Type* resultType = PyInstance::extractTypeFrom((PyTypeObject*)res);

        decref(res);

        if (!resultType) {
            throw std::runtime_error("we expected objectFromName to return a native type in this context, but it didn't.");
        }

        return resultType;
    }

    if (style != T_NATIVETYPE_BY_CATEGORY) {
        throw std::runtime_error("corrupt data: expected a valid code for native type deserialization");
    }

    if (!allowCaching) {
        return deserializeNativeTypeUncached(b);
    }

    int32_t typePtrId = b.read_uint32();
    Type* cachedTypePtr = (Type*)b.lookupCachedPointer(typePtrId);

    if (cachedTypePtr) {
        return cachedTypePtr;
    }

    //otherwise, create a forward type object and stick it in the cache
    Forward* fwdType = new Forward(nullptr, "<circular type reference during serialization>");

    b.addCachedPointer(typePtrId, fwdType);

    Type* resultType = deserializeNativeTypeUncached(b);

    fwdType->resolveDuringSerialization(resultType);

    b.updateCachedPointer(typePtrId, resultType);

    return resultType;
}

Type* PythonSerializationContext::deserializeNativeTypeUncached(DeserializationBuffer& b) const {
    PyEnsureGilAcquired acquireTheGil;

    uint8_t category = b.read_uint8();
    if (category == Type::TypeCategory::catInt64) {
        return ::Int64::Make();
    }
    if (category == Type::TypeCategory::catFloat64) {
        return ::Float64::Make();
    }
    if (category == Type::TypeCategory::catNone) {
        return ::None::Make();
    }
    if (category == Type::TypeCategory::catBytes) {
        return ::Bytes::Make();
    }
    if (category == Type::TypeCategory::catString) {
        return ::StringType::Make();
    }
    if (category == Type::TypeCategory::catBool) {
        return ::Bool::Make();
    }

    if (category == Type::TypeCategory::catConcreteAlternative) {
        Type* base = deserializeNativeType(b);
        if (base->getTypeCategory() != Type::TypeCategory::catAlternative) {
            throw std::runtime_error("corrupt data: expected an Alternative type here");
        }
        uint32_t which = b.read_uint32();

        Alternative* a = (Alternative*)base;
        if (which >= a->subtypes().size()) {
            throw std::runtime_error("corrupt data: invalid alternative specified");
        }

        return ::ConcreteAlternative::Make(a,which);
    }


    if (category == Type::TypeCategory::catDict) {
        Type* keyType = deserializeNativeType(b);
        Type* valueType = deserializeNativeType(b);

        return ::Dict::Make(keyType, valueType);
    }

    if (category == Type::TypeCategory::catConstDict) {
        Type* keyType = deserializeNativeType(b);
        Type* valueType = deserializeNativeType(b);

        return ::ConstDict::Make(keyType, valueType);
    }

    if (category == Type::TypeCategory::catTupleOf) {
        return ::TupleOfType::Make(
            deserializeNativeType(b)
            );
    }

    if (category == Type::TypeCategory::catListOf) {
        return ::ListOfType::Make(
            deserializeNativeType(b)
            );
    }

    if (category == Type::TypeCategory::catTuple) {
        std::vector<Type*> types;
        size_t count = b.read_uint32();
        for (long k = 0; k < count; k++) {
            types.push_back(deserializeNativeType(b));
        }
        return ::Tuple::Make(types);
    }

    if (category == Type::TypeCategory::catNamedTuple) {
        std::vector<Type*> types;
        std::vector<std::string> names;
        size_t count = b.read_uint32();
        for (long k = 0; k < count; k++) {
            names.push_back(b.readString());
            types.push_back(deserializeNativeType(b, true));
        }
        return ::NamedTuple::Make(types, names);
    }

    if (category == Type::TypeCategory::catOneOf) {
        std::vector<Type*> types;
        size_t count = b.read_uint32();
        for (long k = 0; k < count; k++) {
            types.push_back(deserializeNativeType(b));
        }
        return ::OneOf::Make(types);
    }

    if (category == Type::TypeCategory::catPythonObjectOfType) {
        PyObject* typeObj = deserializePythonObject(b);
        return ::PythonObjectOfType::Make((PyTypeObject*)typeObj);
    }

    if (category == Type::TypeCategory::catValue) {
        Type* t = deserializeNativeType(b);

        Instance i = Instance::createAndInitialize(t, [&](instance_ptr p) {
            PyEnsureGilReleased releaseTheGil;

            t->deserialize(p, b);
        });

        return ::Value::Make(i);
    }

    throw std::runtime_error("corrupt data. invalid native type code.");
}

// virtual
PyObject* PythonSerializationContext::deserializePythonObject(DeserializationBuffer& b) const {
    PyEnsureGilAcquired acquireTheGil;

    uint8_t code = b.read_uint8();
    if (code == T_LIST) {
        return deserializePyList(b);
    } else
    if (code == T_TUPLE) {
        return deserializePyTuple(b);
    } else
    if (code == T_SET) {
        return deserializePySet(b);
    } else
    if (code == T_FROZENSET) {
        return deserializePyFrozenSet(b);
    } else
    if (code == T_DICT) {
        return deserializePydict(b);
    } else
    if (code == T_NATIVE) {
        Type* t = deserializeNativeType(b);

        PyInstance::guaranteeForwardsResolvedOrThrow(t);

        return PyInstance::initialize(t, [&](instance_ptr selfData) {
            PyEnsureGilReleased releaseTheGil;

            t->deserialize(selfData, b);
        });
    } else
    if (code == T_NATIVETYPE) {
        return incref((PyObject*)PyInstance::typeObj(deserializeNativeType(b)));
    } else
    if (code == T_OBJECT) {
        return deserializePythonObjectNamedOrAsObj(b);
    } else
    if (code == T_NONE) {
        return incref(Py_None);
    } else
    if (code == T_TRUE) {
        return incref(Py_True);
    } else
    if (code == T_FALSE) {
        return incref(Py_False);
    } else
    if (code == T_UNICODE) {
        PyObject* result;
        uint32_t sz = b.read_uint32();
        b.read_bytes_fun(sz, [&](uint8_t* ptr) {
            result = PyUnicode_DecodeUTF8((const char*)ptr, sz, nullptr);
            if (!result) {
                throw PythonExceptionSet();
            }
        });
        return result;
    } else
    if (code == T_BYTES) {
        PyObject* result;
        uint32_t sz = b.read_uint32();
        b.read_bytes_fun(sz, [&](uint8_t* ptr) {
            result = PyBytes_FromStringAndSize((const char*)ptr, sz);
        });
        return result;
    } else
    if (code == T_FLOAT) {
        return PyFloat_FromDouble(b.read_double());
    } else
    if (code == T_LONG) {
        return PyLong_FromLong(b.read_int64());
    }

    throw std::runtime_error("corrupt data: invalid deserialization code");
}

template<class Size_Fn, class GetItem_Fn>
inline void PythonSerializationContext::serializeIndexable(PyObject* o, SerializationBuffer& b, Size_Fn size_fn, GetItem_Fn get_item_fn) const {
    PyEnsureGilAcquired acquireTheGil;

    static Type* anyPyObjType = PythonObjectOfType::AnyPyObject();

    uint32_t id;
    bool isNew;
    std::tie(id, isNew) = b.cachePointer(o, anyPyObjType);
    b.write_uint32(id);

    if (isNew) {
        size_t sz = size_fn(o);

        b.write_uint32(sz);

        for (long k = 0; k < sz; k++) {
            serializePythonObject(get_item_fn(o, k), b);
        }
    }
}

template<class Factory_Fn, class SetItem_Fn>
inline PyObject* PythonSerializationContext::deserializeIndexable(DeserializationBuffer& b, Factory_Fn factory_fn, SetItem_Fn set_item_fn) const {
    PyEnsureGilAcquired acquireTheGil;

    static Type* anyPyObjType = PythonObjectOfType::AnyPyObject();

    uint32_t id = b.read_uint32();

    PyObject* res = (PyObject*)b.lookupCachedPointer(id);
    if (res) {
        return incref(res);
    }

    size_t sz = b.read_uint32();

    res = factory_fn(sz);
    b.addCachedPointer(id, incref(res), anyPyObjType);

    try {
        for (long k = 0; k < sz; k++) {
            set_item_fn(res, k, deserializePythonObject(b));
        }
    } catch(...) {
        decref(res);
        throw;
    }

    return res;
}

template<class Size_Fn>
inline void PythonSerializationContext::serializeIterable(PyObject* o, SerializationBuffer& b, Size_Fn size_fn) const {
    PyEnsureGilAcquired acquireTheGil;

    static Type* anyPyObjType = PythonObjectOfType::AnyPyObject();

    uint32_t id;
    bool isNew;
    std::tie(id, isNew) = b.cachePointer(o, anyPyObjType);
    b.write_uint32(id);

    if (isNew) {
        size_t sz = size_fn(o);

        b.write_uint32(sz);
        PyObject* iter = PyObject_GetIter(o);
        if (!iter) {
            return;
        }
        PyObject* item = PyIter_Next(iter);
        while (item) {
            serializePythonObject(item, b);
            decref(item);
            item = PyIter_Next(iter);
        }
        decref(iter);
    }
}

template<class Factory_Fn, class AddItem_Fn, class Clear_Fn>
inline PyObject* PythonSerializationContext::deserializeIterable(DeserializationBuffer &b, Factory_Fn factory_fn, AddItem_Fn add_item_fn, Clear_Fn clear_fn) const {
    PyEnsureGilAcquired acquireTheGil;

    static Type* anyPyObjType = PythonObjectOfType::AnyPyObject();

    uint32_t id = b.read_uint32();

    PyObject* res = (PyObject*)b.lookupCachedPointer(id);
    if (res) {
        return incref(res);
    }

    size_t sz = b.read_uint32();

    res = factory_fn(NULL);
    if (!res) {
        PyErr_PrintEx(1);
        throw std::runtime_error(std::string(
            "Failed to allocate storage into which to deserialize iterable"));
    }

    b.addCachedPointer(id, incref(res), anyPyObjType);

    try {
        for (long k = 0; k < sz; k++) {
            PyObject* item = deserializePythonObject(b);
            int success = add_item_fn(res, item);
            decref(item);
            if (success < 0) {
                PyErr_PrintEx(1);
                throw std::runtime_error(std::string("Call to add_item_fn failed"));
            }
        }
    } catch(...) {
        clear_fn(res);
        decref(res);
        throw;
    }

    return res;
}

void PythonSerializationContext::serializePyList(PyObject* o, SerializationBuffer& b) const {
    serializeIndexable(o, b, PyList_Size, PyList_Get_Item_No_Checks);
}

PyObject* PythonSerializationContext::deserializePyList(DeserializationBuffer& b) const {
    return deserializeIndexable(b, PyList_New, PyList_Set_Item_No_Checks);
}

void PythonSerializationContext::serializePyTuple(PyObject* o, SerializationBuffer& b) const {
    serializeIndexable(o, b, PyTuple_Size, PyTuple_Get_Item_No_Checks);
}

PyObject* PythonSerializationContext::deserializePyTuple(DeserializationBuffer& b) const {
    return deserializeIndexable(b, PyTuple_New, PyTuple_Set_Item_No_Checks);
}

void PythonSerializationContext::serializePySet(PyObject* o, SerializationBuffer& b) const {
    serializeIterable(o, b, PySet_Size);
}

PyObject* PythonSerializationContext::deserializePySet(DeserializationBuffer &b) const {
    return deserializeIterable(b, PySet_New, PySet_Add, PySet_Clear);
}

void PythonSerializationContext::serializePyFrozenSet(PyObject* o, SerializationBuffer& b) const {
    serializeIterable(o, b, PySet_Size);
}

std::shared_ptr<ByteBuffer> PythonSerializationContext::compressOrDecompress(uint8_t* begin, uint8_t* end, bool compress) const {
    assertHoldingTheGil();

    if (!mContextObj) {
        return std::shared_ptr<ByteBuffer>(new RangeByteBuffer(begin,end));
    }

    PyObjectStealer pyBytes(
        PyBytes_FromStringAndSize((const char*)begin, end-begin)
        );

    PyObjectStealer outBytes(
        PyObject_CallMethod(
            mContextObj,
            compress ? "compress" : "decompress",
            "O",
            (PyObject*)pyBytes //without this cast, the actual "Stealer" object gets passed
                               //because this is a C varargs function and it doesn't know
                               //that the intended type is PyObject*.
            )
        );

    if (!outBytes) {
        throw PythonExceptionSet();
    }

    if (!PyBytes_Check(outBytes)) {
        PyErr_Format(PyExc_TypeError,
            compress ?
                    "'compress' method didn't return bytes object."
                :   "'decompress' method didn't return bytes object."
            );
        throw PythonExceptionSet();
    }

    return std::shared_ptr<ByteBuffer>(new PyBytesByteBuffer(outBytes));
}

std::shared_ptr<ByteBuffer> PythonSerializationContext::compress(uint8_t* begin, uint8_t* end) const {
    return compressOrDecompress(begin, end, true);
}

std::shared_ptr<ByteBuffer> PythonSerializationContext::decompress(uint8_t* begin, uint8_t* end) const {
    return compressOrDecompress(begin, end, false);
}

