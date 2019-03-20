#pragma once

#include <Python.h>
#include "VersionedIdSets.hpp"

extern PyTypeObject PyType_VersionedIdSets;

class PyVersionedIdSets {
public:
    PyObject_HEAD;
    VersionedIdSets* idSets;

    static void dealloc(PyVersionedIdSets *self);

    static PyObject *get(PyVersionedIdSets *self, PyObject* args, PyObject* kwargs);

    static PyObject *new_(PyTypeObject *type, PyObject *args, PyObject *kwds);

    static int init(PyVersionedIdSets *self, PyObject *args, PyObject *kwds);
};

