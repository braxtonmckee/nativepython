#include <Python.h>
#include <map>
#include <memory>
#include <vector>
#include <string>
#include <iostream>

#include "PyVersionedObjectsOfType.hpp"
#include "VersionedObjectsOfType.hpp"
#include "../typed_python/Type.hpp"
#include "../typed_python/PyInstance.hpp"

class PyVersionedObjectsOfType {
public:
    PyObject_HEAD;
    VersionedObjectsOfType* objectsOfType;

    static void dealloc(PyVersionedObjectsOfType *self)
    {
        delete self->objectsOfType;
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject *add(PyVersionedObjectsOfType *self, PyObject* args, PyObject* kwargs)
    {
        static const char *kwlist[] = {"objectId", "versionId", "instance", NULL};
        PyObject *instance = NULL;
        int64_t objectId;
        int64_t versionId;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "llO", (char**)kwlist, &objectId, &versionId, &instance)) {
            return nullptr;
        }

        try {
            Instance key(self->objectsOfType->getType(), [&](instance_ptr data) {
                PyInstance::copyConstructFromPythonInstance(self->objectsOfType->getType(), data, instance);
            });

            return incref(
                self->objectsOfType->add(objectId, versionId, key.data()) ?
                    Py_True : Py_False
                );
        } catch(PythonExceptionSet& e) {
            return NULL;
        } catch(std::exception& e) {
            PyErr_SetString(PyExc_TypeError, e.what());
            return NULL;
        }
    }

    static PyObject *markDeleted(PyVersionedObjectsOfType *self, PyObject* args, PyObject* kwargs)
    {
        static const char *kwlist[] = {"objectId", "versionId", NULL};
        int64_t objectId;
        int64_t versionId;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ll", (char**)kwlist, &objectId, &versionId)) {
            return nullptr;
        }

        try {
            return incref(
                self->objectsOfType->markDeleted(objectId, versionId) ?
                    Py_True : Py_False
                );
        } catch(PythonExceptionSet& e) {
            return NULL;
        } catch(std::exception& e) {
            PyErr_SetString(PyExc_TypeError, e.what());
            return NULL;
        }
    }

    static PyObject *best(PyVersionedObjectsOfType *self, PyObject* args, PyObject* kwargs)
    {
        static const char *kwlist[] = {"objectId", "versionId", NULL};
        int64_t objectId;
        int64_t versionId;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ll", (char**)kwlist, &objectId, &versionId)) {
            return nullptr;
        }

        auto instancePtrAndVersionPair = self->objectsOfType->best(objectId, versionId);

        PyObjectStealer version(PyLong_FromLong(instancePtrAndVersionPair.second));

        if (!instancePtrAndVersionPair.first) {
            return PyTuple_Pack(3, Py_False, Py_None, (PyObject*)version);
        }

        PyObjectStealer ref(PyInstance::extractPythonObject(instancePtrAndVersionPair.first, self->objectsOfType->getType()));

        return PyTuple_Pack(3, Py_True, (PyObject*)ref, (PyObject*)version);
    }

    static PyObject *new_(PyTypeObject *type, PyObject *args, PyObject *kwds)
    {
        PyVersionedObjectsOfType *self;
        self = (PyVersionedObjectsOfType *) type->tp_alloc(type, 0);

        if (self != NULL) {
            self->objectsOfType = nullptr;
        }
        return (PyObject *) self;
    }

    static int init(PyVersionedObjectsOfType *self, PyObject *args, PyObject *kwds)
    {
        static const char *kwlist[] = {"type", NULL};
        PyObject *type = NULL;

        if (!PyArg_ParseTupleAndKeywords(args, kwds, "O", (char**)kwlist,&type)) {
            return -1;
        }

        if (!type) {
            PyErr_SetString(PyExc_TypeError, "Expected a Type object");
            return -1;
        }

        Type* typeArg = PyInstance::unwrapTypeArgToTypePtr(type);

        if (!typeArg) {
            PyErr_SetString(PyExc_TypeError, "Expected a Type object");
            return -1;
        }

        self->objectsOfType = new VersionedObjectsOfType(typeArg);

        return 0;
    }

    static PyObject* moveGuaranteedLowestIdForward(PyVersionedObjectsOfType* self, PyObject* args, PyObject* kwargs) {
        static const char* kwlist[] = { "transaction_id", NULL };
        int64_t transaction;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "l", (char**)kwlist, &transaction)) {
            return nullptr;
        }

        return translateExceptionToPyObject([&]() {
            self->objectsOfType->moveGuaranteedLowestIdForward(transaction);

            return incref(
                self->objectsOfType->empty() ?
                    Py_True : Py_False
                );
        });
    }
};

PyMethodDef PyVersionedObjectsOfType_methods[] = {
    {"add", (PyCFunction) PyVersionedObjectsOfType::add, METH_VARARGS | METH_KEYWORDS},
    {"markDeleted", (PyCFunction) PyVersionedObjectsOfType::markDeleted, METH_VARARGS | METH_KEYWORDS},
    {"best", (PyCFunction) PyVersionedObjectsOfType::best, METH_VARARGS | METH_KEYWORDS},
    {"moveGuaranteedLowestIdForward", (PyCFunction) PyVersionedObjectsOfType::moveGuaranteedLowestIdForward, METH_VARARGS | METH_KEYWORDS},
    {NULL}  /* Sentinel */
};


PyTypeObject PyType_VersionedObjectsOfType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "VersionedObjectsOfType",
    .tp_basicsize = sizeof(PyVersionedObjectsOfType),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor) PyVersionedObjectsOfType::dealloc,
    .tp_print = 0,
    .tp_getattr = 0,
    .tp_setattr = 0,
    .tp_as_async = 0,
    .tp_repr = 0,
    .tp_as_number = 0,
    .tp_as_sequence = 0,
    .tp_as_mapping = 0,
    .tp_hash = 0,
    .tp_call = 0,
    .tp_str = 0,
    .tp_getattro = 0,
    .tp_setattro = 0,
    .tp_as_buffer = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = 0,
    .tp_traverse = 0,
    .tp_clear = 0,
    .tp_richcompare = 0,
    .tp_weaklistoffset = 0,
    .tp_iter = 0,
    .tp_iternext = 0,
    .tp_methods = PyVersionedObjectsOfType_methods,
    .tp_members = 0,
    .tp_getset = 0,
    .tp_base = 0,
    .tp_dict = 0,
    .tp_descr_get = 0,
    .tp_descr_set = 0,
    .tp_dictoffset = 0,
    .tp_init = (initproc) PyVersionedObjectsOfType::init,
    .tp_alloc = 0,
    .tp_new = PyVersionedObjectsOfType::new_,
    .tp_free = 0,
    .tp_is_gc = 0,
    .tp_bases = 0,
    .tp_mro = 0,
    .tp_cache = 0,
    .tp_subclasses = 0,
    .tp_weaklist = 0,
    .tp_del = 0,
    .tp_version_tag = 0,
    .tp_finalize = 0,
};

