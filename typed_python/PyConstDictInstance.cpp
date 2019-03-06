#include "PyConstDictInstance.hpp"

ConstDict* PyConstDictInstance::type() {
    return (ConstDict*)extractTypeFrom(((PyObject*)this)->ob_type);
}

// static
PyObject* PyConstDictInstance::constDictItems(PyObject *o) {
    Type* self_type = extractTypeFrom(o->ob_type);

    PyInstance* w = (PyInstance*)o;

    if (self_type && self_type->getTypeCategory() == Type::TypeCategory::catConstDict) {
        PyInstance* self = (PyInstance*)o->ob_type->tp_alloc(o->ob_type, 0);

        self->mIteratorOffset = 0;
        self->mIteratorFlag = 2;
        self->mIsMatcher = false;

        self->initialize([&](instance_ptr data) {
            self_type->copy_constructor(data, w->dataPtr());
        });


        return (PyObject*)self;
    }

    PyErr_SetString(PyExc_TypeError, ("Cannot iterate an instance of " + self_type->name()).c_str());
    return NULL;
}

// static
PyObject* PyConstDictInstance::constDictKeys(PyObject *o) {
    Type* self_type = extractTypeFrom(o->ob_type);
    PyInstance* w = (PyInstance*)o;

    if (self_type && self_type->getTypeCategory() == Type::TypeCategory::catConstDict) {
        PyInstance* self = (PyInstance*)o->ob_type->tp_alloc(o->ob_type, 0);

        self->mIteratorOffset = 0;
        self->mIteratorFlag = 0;
        self->mIsMatcher = false;

        self->initialize([&](instance_ptr data) {
            self_type->copy_constructor(data, w->dataPtr());
        });

        return (PyObject*)self;
    }

    PyErr_SetString(PyExc_TypeError, ("Cannot iterate an instance of " + self_type->name()).c_str());
    return NULL;
}

// static
PyObject* PyConstDictInstance::constDictValues(PyObject *o) {
    Type* self_type = extractTypeFrom(o->ob_type);
    PyInstance* w = (PyInstance*)o;

    if (self_type && self_type->getTypeCategory() == Type::TypeCategory::catConstDict) {
        PyInstance* self = (PyInstance*)o->ob_type->tp_alloc(o->ob_type, 0);

        self->mIteratorOffset = 0;
        self->mIteratorFlag = 1;
        self->mIsMatcher = false;

        self->initialize([&](instance_ptr data) {
            self_type->copy_constructor(data, w->dataPtr());
        });


        return (PyObject*)self;
    }

    PyErr_SetString(PyExc_TypeError, ("Cannot iterate an instance of " + self_type->name()).c_str());
    return NULL;
}

// static
PyObject* PyConstDictInstance::constDictGet(PyObject* o, PyObject* args) {
    PyConstDictInstance* self_w = (PyConstDictInstance*)o;

    if (PyTuple_Size(args) < 1 || PyTuple_Size(args) > 2) {
        PyErr_SetString(PyExc_TypeError, "ConstDict.get takes one or two arguments");
        return NULL;
    }

    PyObjectHolder item(PyTuple_GetItem(args,0));
    PyObjectHolder ifNotFound(PyTuple_Size(args) == 2 ? PyTuple_GetItem(args,1) : Py_None);

    Type* self_type = extractTypeFrom(o->ob_type);
    Type* item_type = extractTypeFrom(item->ob_type);

    if (self_type->getTypeCategory() == Type::TypeCategory::catConstDict) {
        if (item_type == self_w->type()->keyType()) {
            PyInstance* item_w = (PyInstance*)(PyObject*)item;

            instance_ptr i = self_w->type()->lookupValueByKey(self_w->dataPtr(), item_w->dataPtr());

            if (!i) {
                return incref(ifNotFound);
            }

            return extractPythonObject(i, self_w->type()->valueType());
        } else {
            try {
                Instance key(self_w->type()->keyType(), [&](instance_ptr data) {
                    copyConstructFromPythonInstance(self_w->type()->keyType(), data, item);
                });

                instance_ptr i = self_w->type()->lookupValueByKey(self_w->dataPtr(), key.data());

                if (!i) {
                    return incref(ifNotFound);
                }

                return extractPythonObject(i, self_w->type()->valueType());
            } catch(PythonExceptionSet& e) {
                return NULL;
            } catch(std::exception& e) {
                PyErr_SetString(PyExc_TypeError, e.what());
                return NULL;
            }
        }

        PyErr_SetString(PyExc_TypeError, "Invalid ConstDict lookup type");
        return NULL;
    }

    PyErr_SetString(PyExc_TypeError, "Wrong type!");
    return NULL;
}

PyObject* PyConstDictInstance::tp_iter_concrete() {
    PyInstance* output = (PyInstance*)PyInstance::initialize(type(), [&](instance_ptr data) {
        type()->copy_constructor(data, dataPtr());
    });

    output->mIteratorOffset = 0;
    output->mIteratorFlag = mIteratorFlag;
    output->mIsMatcher = false;

    return (PyObject*)output;
}

PyObject* PyConstDictInstance::tp_iternext_concrete() {
    if (mIteratorOffset >= type()->size(dataPtr())) {
        return NULL;
    }

    mIteratorOffset++;

    if (mIteratorFlag == 2) {
        auto t1 = extractPythonObject(
                type()->kvPairPtrKey(dataPtr(), mIteratorOffset-1),
                type()->keyType()
                );
        auto t2 = extractPythonObject(
                type()->kvPairPtrValue(dataPtr(), mIteratorOffset-1),
                type()->valueType()
                );

        auto res = PyTuple_Pack(2, t1, t2);

        decref(t1);
        decref(t2);

        return res;
    } else if (mIteratorFlag == 1) {
        return extractPythonObject(
            type()->kvPairPtrValue(dataPtr(), mIteratorOffset-1),
            type()->valueType()
            );
    } else {
        return extractPythonObject(
            type()->kvPairPtrKey(dataPtr(), mIteratorOffset-1),
            type()->keyType()
            );
    }
}

Py_ssize_t PyConstDictInstance::mp_and_sq_length_concrete() {
    return type()->size(dataPtr());
}

int PyConstDictInstance::sq_contains_concrete(PyObject* item) {
    Type* item_type = extractTypeFrom(item->ob_type);

    if (item_type == type()->keyType()) {
        PyInstance* item_w = (PyInstance*)item;

        instance_ptr i = type()->lookupValueByKey(dataPtr(), item_w->dataPtr());

        if (!i) {
            return 0;
        }

        return 1;
    } else {
        Instance key(type()->keyType(), [&](instance_ptr data) {
            copyConstructFromPythonInstance(type()->keyType(), data, item);
        });

        instance_ptr i = type()->lookupValueByKey(dataPtr(), key.data());

        return i ? 1 : 0;
    }
}

PyObject* PyConstDictInstance::pyOperatorConcrete(PyObject* rhs, const char* op, const char* opErr) {
    if (strcmp(op, "__add__") == 0) {
        Type* rhs_type = extractTypeFrom(rhs->ob_type);

        if (type() == rhs_type) {
            PyInstance* w_rhs = (PyInstance*)rhs;

            return PyInstance::initialize(type(), [&](instance_ptr data) {
                type()->addDicts(dataPtr(), w_rhs->dataPtr(), data);
            });
        } else {
            Instance other(type(), [&](instance_ptr data) {
                copyConstructFromPythonInstance(type(), data, rhs);
            });

            return PyInstance::initialize(type(), [&](instance_ptr data) {
                type()->addDicts(dataPtr(), other.data(), data);
            });
        }
    }
    if (strcmp(op, "__sub__") == 0) {
        Type* tupleOfKeysType = type()->tupleOfKeysType();

        //convert rhs to a relevant dict type.
        Instance keys(tupleOfKeysType, [&](instance_ptr data) {
            copyConstructFromPythonInstance(tupleOfKeysType, data, rhs);
        });

        return PyInstance::initialize(type(), [&](instance_ptr data) {
            type()->subtractTupleOfKeysFromDict(dataPtr(), keys.data(), data);
        });
    }

    return ((PyInstance*)this)->pyOperatorConcrete(rhs, op, opErr);
}

PyObject* PyConstDictInstance::mp_subscript_concrete(PyObject* item) {
    Type* item_type = extractTypeFrom(item->ob_type);

    if (item_type == type()->keyType()) {
        PyInstance* item_w = (PyInstance*)item;

        instance_ptr i = type()->lookupValueByKey(dataPtr(), item_w->dataPtr());

        if (!i) {
            PyErr_SetObject(PyExc_KeyError, item);
            return NULL;
        }

        return extractPythonObject(i, type()->valueType());
    } else {
        Instance key(type()->keyType(), [&](instance_ptr data) {
            copyConstructFromPythonInstance(type()->keyType(), data, item);
        });

        instance_ptr i = type()->lookupValueByKey(dataPtr(), key.data());

        if (!i) {
            PyErr_SetObject(PyExc_KeyError, item);
            return NULL;
        }

        return extractPythonObject(i, type()->valueType());
    }

    PyErr_SetObject(PyExc_KeyError, item);
    return NULL;
}

PyMethodDef* PyConstDictInstance::typeMethodsConcrete() {
    return new PyMethodDef [5] {
        {"get", (PyCFunction)PyConstDictInstance::constDictGet, METH_VARARGS, NULL},
        {"items", (PyCFunction)PyConstDictInstance::constDictItems, METH_NOARGS, NULL},
        {"keys", (PyCFunction)PyConstDictInstance::constDictKeys, METH_NOARGS, NULL},
        {"values", (PyCFunction)PyConstDictInstance::constDictValues, METH_NOARGS, NULL},
        {NULL, NULL}
    };
}

void PyConstDictInstance::mirrorTypeInformationIntoPyTypeConcrete(ConstDict* constDictT, PyTypeObject* pyType) {
    //expose 'ElementType' as a member of the type object
    PyDict_SetItemString(pyType->tp_dict, "KeyType",
            typePtrToPyTypeRepresentation(constDictT->keyType())
            );
    PyDict_SetItemString(pyType->tp_dict, "ValueType",
            typePtrToPyTypeRepresentation(constDictT->valueType())
            );
}
