#include <Python.h>
#include <map>
#include <memory>
#include <vector>
#include <string>
#include <iostream>
#include "../typed_python/util.hpp"

#include "PyVersionedIdSet.hpp"
#include "VersionedIdSet.hpp"

void PyVersionedIdSet::dealloc(PyVersionedIdSet* self)
{
    self->idSet.~shared_ptr();

    Py_TYPE(self)->tp_free((PyObject*)self);
}

PyObject* PyVersionedIdSet::new_(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    PyVersionedIdSet *self;
    self = (PyVersionedIdSet*)type->tp_alloc(type, 0);

    if (self != NULL) {
        new (&self->idSet) std::shared_ptr<VersionedIdSet>();
    }
    return (PyObject*)self;
}

int PyVersionedIdSet::init(PyVersionedIdSet *self, PyObject *args, PyObject *kwds)
{
    self->idSet.reset(new VersionedIdSet());

    return 0;
}

PyObject* PyVersionedIdSet::isActive(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = { "transaction_id", "object_id", NULL };
    int64_t transaction;
    int64_t object;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ll", (char**)kwlist, &transaction, &object)) {
        return nullptr;
    }

    return incref(self->idSet->isActive(transaction, object) ? Py_True : Py_False);
}

PyObject* PyVersionedIdSet::moveGuaranteedLowestIdForward(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = { "transaction_id", NULL };
    int64_t transaction;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "l", (char**)kwlist, &transaction)) {
        return nullptr;
    }

    return translateExceptionToPyObject([&]() {
        self->idSet->moveGuaranteedLowestIdForward(transaction);

        return incref(
            self->idSet->empty() ?
                Py_True : Py_False
            );
    });
}

PyObject* PyVersionedIdSet::lookupOne(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = { "transaction_id", NULL };
    int64_t transaction;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "l", (char**)kwlist, &transaction)) {
        return nullptr;
    }

    return translateExceptionToPyObject([&]() {
        return PyLong_FromLong(self->idSet->lookupOne(transaction));
    });
}

PyObject* PyVersionedIdSet::lookupFirst(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = { "transaction_id", NULL };
    int64_t transaction;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "l", (char**)kwlist, &transaction)) {
        return nullptr;
    }

    return translateExceptionToPyObject([&]() {
        return PyLong_FromLong(self->idSet->lookupFirst(transaction));
    });
}

PyObject* PyVersionedIdSet::lookupNext(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = { "transaction_id", "object_id", NULL };
    int64_t transaction;
    int64_t object;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ll", (char**)kwlist, &transaction, &object)) {
        return nullptr;
    }

    return translateExceptionToPyObject([&]() {
        return PyLong_FromLong(self->idSet->lookupNext(transaction, object));
    });
}

PyObject* PyVersionedIdSet::add(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = { "transaction_id", "object_id", NULL };
    int64_t transaction;
    int64_t object;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ll", (char**)kwlist, &transaction, &object)) {
        return nullptr;
    }

    return translateExceptionToPyObject([&]() {
        self->idSet->add(transaction, object);

        return incref(Py_None);
    });
}

PyObject* PyVersionedIdSet::remove(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = { "transaction_id", "object_id", NULL };
    int64_t transaction;
    int64_t object;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ll", (char**)kwlist, &transaction, &object)) {
        return nullptr;
    }

    return translateExceptionToPyObject([&]() {
        self->idSet->remove(transaction, object);

        return incref(Py_None);
    });
}

PyObject* PyVersionedIdSet::transactionCount(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = { NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "", (char**)kwlist)) {
        return nullptr;
    }

    return translateExceptionToPyObject([&]() {
        return PyLong_FromLong(self->idSet->transactionCount());
    });
}

PyObject* PyVersionedIdSet::totalEntryCount(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = { NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "", (char**)kwlist)) {
        return nullptr;
    }

    return translateExceptionToPyObject([&]() {
        return PyLong_FromLong(self->idSet->totalEntryCount());
    });
}

PyObject* PyVersionedIdSet::addTransaction(PyVersionedIdSet* self, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = { "transaction_id", "added", "removed", NULL };

    int64_t tid;
    PyObject* added;
    PyObject* removed;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lOO", (char**)kwlist, &tid, &added, &removed)) {
        return nullptr;
    }

    return translateExceptionToPyObject([&]() {
        Type* addedType = PyInstance::extractTypeFrom((PyTypeObject*)added->ob_type);
        Type* removedType = PyInstance::extractTypeFrom((PyTypeObject*)removed->ob_type);

        static TupleOf* tupleOfObjectId = TupleOf::Make(Int64::Make());

        auto& idSet = *self->idSet;

        if (addedType == tupleOfObjectId && removedType == tupleOfObjectId) {
            int64_t addedCount = tupleOfObjectId->count(((PyInstance*)added)->dataPtr());
            int64_t removedCount = tupleOfObjectId->count(((PyInstance*)removed)->dataPtr());
            int64_t* addedPtr = (int64_t*)tupleOfObjectId->eltPtr(((PyInstance*)added)->dataPtr(), 0);
            int64_t* removedPtr = (int64_t*)tupleOfObjectId->eltPtr(((PyInstance*)removed)->dataPtr(), 0);

            for (long k = 0; k < addedCount; k++) {
                idSet.add(tid, addedPtr[k]);
            }

            for (long k = 0; k < removedCount; k++) {
                idSet.remove(tid, removedPtr[k]);
            }
        } else {
            iterate(added, [&](PyObject* o) {
                if (!PyLong_CheckExact(o)) {
                    throw std::runtime_error("Please pass integers for object ids.");
                }
                idSet.add(tid, PyLong_AsLong(o));
            });

            iterate(removed, [&](PyObject* o) {
                if (!PyLong_CheckExact(o)) {
                    throw std::runtime_error("Please pass integers for object ids.");
                }
                idSet.remove(tid, PyLong_AsLong(o));
            });
        }

        return incref(Py_None);
    });
}

PyMethodDef PyVersionedIdSet_methods[] = {
    {"isActive", (PyCFunction) PyVersionedIdSet::isActive, METH_VARARGS | METH_KEYWORDS},
    {"lookupOne", (PyCFunction) PyVersionedIdSet::lookupOne, METH_VARARGS | METH_KEYWORDS},
    {"moveGuaranteedLowestIdForward", (PyCFunction) PyVersionedIdSet::moveGuaranteedLowestIdForward, METH_VARARGS | METH_KEYWORDS},
    {"addTransaction", (PyCFunction) PyVersionedIdSet::addTransaction, METH_VARARGS | METH_KEYWORDS},
    {"lookupFirst", (PyCFunction) PyVersionedIdSet::lookupFirst, METH_VARARGS | METH_KEYWORDS},
    {"lookupNext", (PyCFunction) PyVersionedIdSet::lookupNext, METH_VARARGS | METH_KEYWORDS},
    {"add", (PyCFunction) PyVersionedIdSet::add, METH_VARARGS | METH_KEYWORDS},
    {"remove", (PyCFunction) PyVersionedIdSet::remove, METH_VARARGS | METH_KEYWORDS},
    {"transactionCount", (PyCFunction) PyVersionedIdSet::transactionCount, METH_VARARGS | METH_KEYWORDS},
    {"totalEntryCount", (PyCFunction) PyVersionedIdSet::totalEntryCount, METH_VARARGS | METH_KEYWORDS},

    {NULL}  /* Sentinel */
};


PyTypeObject PyType_VersionedIdSet = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "VersionedIdSet",
    .tp_basicsize = sizeof(PyVersionedIdSet),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor) PyVersionedIdSet::dealloc,
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
    .tp_methods = PyVersionedIdSet_methods,
    .tp_members = 0,
    .tp_getset = 0,
    .tp_base = 0,
    .tp_dict = 0,
    .tp_descr_get = 0,
    .tp_descr_set = 0,
    .tp_dictoffset = 0,
    .tp_init = (initproc) PyVersionedIdSet::init,
    .tp_alloc = 0,
    .tp_new = PyVersionedIdSet::new_,
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

