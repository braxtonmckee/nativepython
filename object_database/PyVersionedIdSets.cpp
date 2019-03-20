#include <Python.h>
#include <map>
#include <memory>
#include <vector>
#include <string>
#include <iostream>

#include "PyVersionedIdSets.hpp"
#include "PyVersionedIdSet.hpp"
#include "VersionedIdSets.hpp"
#include "../typed_python/Type.hpp"
#include "../typed_python/PyInstance.hpp"

void PyVersionedIdSets::dealloc(PyVersionedIdSets *self)
{
    if (self->idSets) {
        delete self->idSets;
    }

    Py_TYPE(self)->tp_free((PyObject*)self);
}

PyObject* PyVersionedIdSets::get(PyVersionedIdSets *self, PyObject* args, PyObject* kwargs)
{
    static const char *kwlist[] = {"fieldid", "index", NULL};
    int64_t fieldid;
    PyObject* hashValue;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lS", (char**)kwlist, &fieldid, &hashValue)) {
        return nullptr;
    }

    char* buffer;
    Py_ssize_t length;

    if (PyBytes_AsStringAndSize(hashValue, &buffer, &length) == -1) {
        return nullptr;
    }

    std::shared_ptr<VersionedIdSet> setPtr = self->idSets->idSetPtrFor(std::make_pair(fieldid, std::string(buffer, buffer+length)));

    PyVersionedIdSet* result = (PyVersionedIdSet*)PyType_VersionedIdSet.tp_alloc(&PyType_VersionedIdSet, 0);

    result->idSet = setPtr;

    return (PyObject*)result;
}

PyObject* PyVersionedIdSets::new_(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    PyVersionedIdSets *self;
    self = (PyVersionedIdSets*)type->tp_alloc(type, 0);

    if (self != NULL) {
        self->idSets = nullptr;
    }
    return (PyObject*)self;
}

int PyVersionedIdSets::init(PyVersionedIdSets *self, PyObject *args, PyObject *kwds)
{
    self->idSets = new VersionedIdSets();

    return 0;
}

PyMethodDef PyVersionedIdSets_methods[] = {
    {"get", (PyCFunction) PyVersionedIdSets::get, METH_VARARGS | METH_KEYWORDS},
    {NULL}  /* Sentinel */
};


PyTypeObject PyType_VersionedIdSets = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "VersionedIdSets",
    .tp_basicsize = sizeof(PyVersionedIdSets),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor) PyVersionedIdSets::dealloc,
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
    .tp_methods = PyVersionedIdSets_methods,
    .tp_members = 0,
    .tp_getset = 0,
    .tp_base = 0,
    .tp_dict = 0,
    .tp_descr_get = 0,
    .tp_descr_set = 0,
    .tp_dictoffset = 0,
    .tp_init = (initproc) PyVersionedIdSets::init,
    .tp_alloc = 0,
    .tp_new = PyVersionedIdSets::new_,
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

