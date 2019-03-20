#pragma once

#include <Python.h>
#include "VersionedIdSet.hpp"
#include <memory>

extern PyTypeObject PyType_VersionedIdSet;

class PyVersionedIdSet {
public:
    PyObject_HEAD;
    std::shared_ptr<VersionedIdSet> idSet;

    static void dealloc(PyVersionedIdSet *self);

    static PyObject *new_(PyTypeObject *type, PyObject *args, PyObject *kwds);

    static PyObject* isActive(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs);

    static PyObject* lookupOne(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs);

    static PyObject* lookupFirst(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs);

    static PyObject* lookupNext(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs);

    static PyObject* add(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs);

    static PyObject* addTransaction(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs);

    static PyObject* remove(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs);

    static PyObject* transactionCount(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs);

    static PyObject* totalEntryCount(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs);

    static PyObject* wantsGuaranteedLowestIdMoveForward(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs);

    static PyObject* moveGuaranteedLowestIdForward(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs);

    static int init(PyVersionedIdSet *self, PyObject *args, PyObject *kwds);
};

