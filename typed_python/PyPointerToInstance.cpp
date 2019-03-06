#include "PyPointerToInstance.hpp"

PointerTo* PyPointerToInstance::type() {
    return (PointerTo*)extractTypeFrom(((PyObject*)this)->ob_type);
}

//static
PyObject* PyPointerToInstance::pointerInitialize(PyObject* o, PyObject* args) {
    PyInstance* self_w = (PyInstance*)o;
    PointerTo* pointerT = (PointerTo*)extractTypeFrom(o->ob_type);

    if (PyTuple_Size(args) != 0 && PyTuple_Size(args) != 1) {
        PyErr_SetString(PyExc_TypeError, "PointerTo.initialize takes zero or one argument");
        return NULL;
    }

    instance_ptr target = (instance_ptr)*(void**)self_w->dataPtr();

    if (PyTuple_Size(args) == 0) {
        if (!pointerT->getEltType()->is_default_constructible()) {
            PyErr_Format(PyExc_TypeError, "%s is not default initializable", pointerT->getEltType()->name().c_str());
            return NULL;
        }

        pointerT->getEltType()->constructor(target);
        return incref(Py_None);
    } else {
        try {
            PyObjectHolder arg0(PyTuple_GetItem(args,0));

            copyConstructFromPythonInstance(pointerT->getEltType(), target, arg0);
            return incref(Py_None);
        } catch(std::exception& e) {
            PyErr_SetString(PyExc_TypeError, e.what());
            return NULL;
        } catch(PythonExceptionSet& e) {
            return NULL;
        }
    }
}

//static
PyObject* PyPointerToInstance::pointerSet(PyObject* o, PyObject* args) {
    PyInstance* self_w = (PyInstance*)o;
    PointerTo* pointerT = (PointerTo*)extractTypeFrom(o->ob_type);

    if (PyTuple_Size(args) != 1) {
        PyErr_SetString(PyExc_TypeError, "PointerTo.set takes one argument");
        return NULL;
    }

    instance_ptr target = (instance_ptr)*(void**)self_w->dataPtr();

    instance_ptr tempObj = (instance_ptr)malloc(pointerT->getEltType()->bytecount());

    try {
        PyObjectHolder arg0(PyTuple_GetItem(args,0));

        copyConstructFromPythonInstance(pointerT->getEltType(), tempObj, arg0);
    } catch(std::exception& e) {
        free(tempObj);
        PyErr_SetString(PyExc_TypeError, e.what());
        return NULL;
    } catch(PythonExceptionSet& e) {
        return NULL;
    }

    pointerT->getEltType()->assign(target, tempObj);
    pointerT->getEltType()->destroy(tempObj);
    free(tempObj);

    return incref(Py_None);
}

PyObject* PyPointerToInstance::sq_item_concrete(Py_ssize_t ix) {
    instance_ptr output;

    type()->offsetBy((instance_ptr)&output, dataPtr(), ix);

    return extractPythonObject(output, type()->getEltType());
}

//static
PyObject* PyPointerToInstance::pointerGet(PyObject* o, PyObject* args) {
    PyInstance* self_w = (PyInstance*)o;
    PointerTo* pointerT = (PointerTo*)extractTypeFrom(o->ob_type);

    if (PyTuple_Size(args) != 0) {
        PyErr_SetString(PyExc_TypeError, "PointerTo.get takes zero arguments");
        return NULL;
    }

    instance_ptr target = (instance_ptr)*(void**)self_w->dataPtr();

    return extractPythonObject(target, pointerT->getEltType());
}

//static
PyObject* PyPointerToInstance::pointerCast(PyObject* o, PyObject* args) {
    PyInstance* self_w = (PyInstance*)o;
    PointerTo* pointerT = (PointerTo*)extractTypeFrom(o->ob_type);

    if (PyTuple_Size(args) != 1) {
        PyErr_SetString(PyExc_TypeError, "PointerTo.cast takes one argument");
        return NULL;
    }

    PyObjectHolder arg0(PyTuple_GetItem(args, 0));

    Type* targetType = PyPointerToInstance::unwrapTypeArgToTypePtr(arg0);

    if (!targetType) {
        PyErr_SetString(PyExc_TypeError, "PointerTo.cast requires a type argument");
        return NULL;
    }

    Type* newType = PointerTo::Make(targetType);

    return extractPythonObject(self_w->dataPtr(), newType);
}

PyObject* PyPointerToInstance::pyOperatorConcrete(PyObject* rhs, const char* op, const char* opErr) {
    if (strcmp(op, "__add__") == 0) {
        int64_t ix = PyLong_AsLong(rhs);
        void* output;

        type()->offsetBy((instance_ptr)&output, dataPtr(), ix);

        return extractPythonObject((instance_ptr)&output, type());
    }

    if (strcmp(op, "__sub__") == 0) {
        PointerTo* otherPointer = (PointerTo*)extractTypeFrom(rhs->ob_type);

        if (otherPointer != type()) {
            //call 'super'
            return ((PyInstance*)this)->pyOperatorConcrete(rhs, op, opErr);
        }

        PyInstance* other_w = (PyPointerToInstance*)rhs;

        uint8_t* ptr = *(uint8_t**)dataPtr();
        uint8_t* other_ptr = *(uint8_t**)other_w->dataPtr();

        return PyLong_FromLong((ptr-other_ptr) / type()->getEltType()->bytecount());
    }

    return ((PyInstance*)this)->pyOperatorConcrete(rhs, op, opErr);
}

PyMethodDef* PyPointerToInstance::typeMethodsConcrete() {
    return new PyMethodDef [5] {
        {"initialize", (PyCFunction)PyPointerToInstance::pointerInitialize, METH_VARARGS, NULL},
        {"set", (PyCFunction)PyPointerToInstance::pointerSet, METH_VARARGS, NULL},
        {"get", (PyCFunction)PyPointerToInstance::pointerGet, METH_VARARGS, NULL},
        {"cast", (PyCFunction)PyPointerToInstance::pointerCast, METH_VARARGS, NULL},
        {NULL, NULL}
    };
}

void PyPointerToInstance::mirrorTypeInformationIntoPyTypeConcrete(PointerTo* pointerT, PyTypeObject* pyType) {
    //expose 'ElementType' as a member of the type object
    PyDict_SetItemString(
            pyType->tp_dict,
            "ElementType",
            typePtrToPyTypeRepresentation(pointerT->getEltType())
            );
}


