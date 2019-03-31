#include <Python.h>
#include <map>
#include <memory>
#include <vector>
#include <string>
#include <iostream>

#include "PyVersionedObjects.hpp"
#include "VersionedObjects.hpp"
#include "../typed_python/Type.hpp"
#include "../typed_python/PyInstance.hpp"

class PyVersionedObjects {
public:
    PyObject_HEAD;
    VersionedObjects* objects;

    static void dealloc(PyVersionedObjects *self)
    {
        delete self->objects;
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject *addObjectVersion(PyVersionedObjects *self, PyObject* args, PyObject* kwargs)
    {
        static const char *kwlist[] = {"fieldId", "objectId", "versionId", "instance", NULL};
        PyObject *instance = NULL;
        int64_t fieldId;
        int64_t objectId;
        int64_t versionId;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lllO", (char**)kwlist, &fieldId, &objectId, &versionId, &instance)) {
            return nullptr;
        }

        Type* targetType = self->objects->valueTypeForField(fieldId);
        if (!targetType) {
            PyErr_Format(PyExc_TypeError, "No valid type found for field id %d", fieldId);
            return NULL;
        }

        try {
            Instance key(targetType, [&](instance_ptr data) {
                PyInstance::copyConstructFromPythonInstance(targetType, data, instance);
            });

            return incref(
                self->objects->addObjectVersion(fieldId, objectId, versionId, key.data()) ?
                    Py_True : Py_False
                );
        } catch(PythonExceptionSet& e) {
            return NULL;
        } catch(std::exception& e) {
            PyErr_SetString(PyExc_TypeError, e.what());
            return NULL;
        }
    }

    static PyObject *markObjectVersionDeleted(PyVersionedObjects *self, PyObject* args, PyObject* kwargs)
    {
        static const char *kwlist[] = {"fieldId", "objectId", "versionId", NULL};
        int64_t fieldId;
        int64_t objectId;
        int64_t versionId;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lll", (char**)kwlist, &fieldId, &objectId, &versionId)) {
            return nullptr;
        }

        try {
            return incref(
                self->objects->markObjectVersionDeleted(fieldId, objectId, versionId) ?
                    Py_True : Py_False
                );
        } catch(PythonExceptionSet& e) {
            return NULL;
        } catch(std::exception& e) {
            PyErr_SetString(PyExc_TypeError, e.what());
            return NULL;
        }
    }

    static PyObject *bestObjectVersion(PyVersionedObjects *self, PyObject* args, PyObject* kwargs)
    {
        static const char *kwlist[] = {"fieldId", "objectId", "versionId", NULL};
        int64_t fieldId;
        int64_t objectId;
        int64_t versionId;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lll", (char**)kwlist, &fieldId, &objectId, &versionId)) {
            return nullptr;
        }

        Type* targetType = self->objects->valueTypeForField(fieldId);

        if (!targetType) {
            PyErr_Format(PyExc_TypeError, "No valid type found for field id %d", fieldId);
            return NULL;
        }

        auto instancePtrAndVersionPair = self->objects->bestObjectVersion(fieldId, objectId, versionId);

        PyObjectStealer version(PyLong_FromLong(instancePtrAndVersionPair.second));

        if (!instancePtrAndVersionPair.first) {
            return PyTuple_Pack(3, Py_False, Py_None, (PyObject*)version);
        }

        PyObjectStealer ref(PyInstance::extractPythonObject(instancePtrAndVersionPair.first, targetType));

        return PyTuple_Pack(3, Py_True, (PyObject*)ref, (PyObject*)version);
    }

    static PyObject *new_(PyTypeObject *type, PyObject *args, PyObject *kwargs)
    {
        PyVersionedObjects *self;
        self = (PyVersionedObjects *) type->tp_alloc(type, 0);

        if (self != NULL) {
            self->objects = nullptr;
        }
        return (PyObject *) self;
    }

    static int init(PyVersionedObjects *self, PyObject *args, PyObject *kwargs)
    {
        static const char *kwlist[] = {NULL};
        PyObject *type = NULL;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "", (char**)kwlist)) {
            return -1;
        }

        self->objects = new VersionedObjects();

        return 0;
    }

    static PyObject* moveGuaranteedLowestIdForward(PyVersionedObjects* self, PyObject* args, PyObject* kwargs) {
        static const char* kwlist[] = { "transaction_id", NULL };
        int64_t transaction;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "l", (char**)kwlist, &transaction)) {
            return nullptr;
        }

        return translateExceptionToPyObject([&]() {
            self->objects->moveGuaranteedLowestIdForward(transaction);

            return incref(Py_None);
        });
    }

    static PyObject* define(PyVersionedObjects* self, PyObject* args, PyObject* kwargs) {
        static const char *kwlist[] = {"fieldId", "type", NULL};
        int64_t fieldId;
        PyObject *type = NULL;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lO", (char**)kwlist, &fieldId, &type)) {
            return NULL;
        }

        if (!type) {
            PyErr_SetString(PyExc_TypeError, "Expected a Type object");
            return NULL;
        }

        Type* typeArg = PyInstance::unwrapTypeArgToTypePtr(type);

        if (!typeArg) {
            PyErr_SetString(PyExc_TypeError, "Expected a Type object");
            return NULL;
        }

        return translateExceptionToPyObject([&]() {
            self->objects->defineValueType(fieldId, typeArg);

            return incref(Py_None);
        });
    }

    static index_value pyObjectToIndexValue(PyObject* o) {
        static Type* indexTupleType = Tuple::Make(
            std::vector<Type*>(
                {Int64::Make(), Int64::Make(), Int64::Make(), Int64::Make(), Int64::Make()}
            )
        );

        Instance indexValue(indexTupleType, [&](instance_ptr data) {
            PyInstance::copyConstructFromPythonInstance(indexTupleType, data, o);
        });

        return *(index_value*)indexValue.data();
    }

    static PyObject* indexLookupOne(PyVersionedObjects* self, PyObject* args, PyObject* kwargs) {
        static const char* kwlist[] = { "field_id", "index_value", "transaction_id", NULL };
        int64_t fieldId;
        PyObject* indexValue;
        int64_t transactionId;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lOl", (char**)kwlist, &fieldId, &indexValue, &transactionId)) {
            return nullptr;
        }

        return translateExceptionToPyObject([&]() {
            return PyLong_FromLong(
                self->objects->indexLookupOne(fieldId, pyObjectToIndexValue(indexValue), transactionId)
                );
        });
    }

    static PyObject* indexLookupFirst(PyVersionedObjects* self, PyObject* args, PyObject* kwargs) {
        static const char* kwlist[] = { "field_id", "index_value", "transaction_id", NULL };
        int64_t fieldId;
        PyObject* indexValue;
        int64_t transactionId;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lOl", (char**)kwlist, &fieldId, &indexValue, &transactionId)) {
            return nullptr;
        }

        return translateExceptionToPyObject([&]() {
            return PyLong_FromLong(
                self->objects->indexLookupFirst(fieldId, pyObjectToIndexValue(indexValue), transactionId)
                );
        });
    }

    static PyObject* indexLookupNext(PyVersionedObjects* self, PyObject* args, PyObject* kwargs) {
        static const char* kwlist[] = { "field_id", "index_value", "transaction_id", "object_id", NULL };
        int64_t fieldId;
        PyObject* indexValue;
        int64_t transactionId;
        int64_t oid;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lOll", (char**)kwlist, &fieldId, &indexValue, &transactionId, &oid)) {
            return nullptr;
        }

        return translateExceptionToPyObject([&]() {
            return PyLong_FromLong(
                self->objects->indexLookupNext(fieldId, pyObjectToIndexValue(indexValue), transactionId, oid)
                );
        });
    }

    /****
    unpack an object_id or an array of object_id, using a fastpath in the case of a TupleOf(int64_t)

    calls 'f' with each item.
    ****/
    template<class f>
    static void iterateObjectIds(PyObject* objectIds, f func) {
        if (PyLong_Check(objectIds)) {
            func(PyLong_AsLong(objectIds));
            return;
        }
        Type* t = PyInstance::extractTypeFrom((PyTypeObject*)objectIds->ob_type);

        static TupleOf* tupleOfObjectId = TupleOf::Make(Int64::Make());

        if (t == tupleOfObjectId) {
            int64_t count = tupleOfObjectId->count(((PyInstance*)objectIds)->dataPtr());
            int64_t* ptr = (int64_t*)tupleOfObjectId->eltPtr(((PyInstance*)objectIds)->dataPtr(), 0);

            for (long k = 0; k < count; k++) {
                func(ptr[k]);
            }
            return;
        }

        iterate(objectIds, [&](PyObject* o) {
            if (!PyLong_CheckExact(o)) {
                throw std::runtime_error("Please pass integers for object ids.");
            }
            func(PyLong_AsLong(o));
        });
    }

    static PyObject* indexAdd(PyVersionedObjects* self, PyObject* args, PyObject* kwargs) {
        static const char* kwlist[] = { "field_id", "index_value", "transaction_id", "object_id", NULL };
        int64_t fieldId;
        PyObject* indexValue;
        int64_t transactionId;
        PyObject* intOrTupleOfInt;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lOlO", (char**)kwlist, &fieldId, &indexValue, &transactionId, &intOrTupleOfInt)) {
            return nullptr;
        }

        return translateExceptionToPyObject([&]() {
            index_value index = pyObjectToIndexValue(indexValue);

            iterateObjectIds(intOrTupleOfInt, [&](object_id oid) {
                self->objects->indexAdd(fieldId, index, transactionId, oid);
            });
            return incref(Py_None);
        });
    }

    static PyObject* indexRemove(PyVersionedObjects* self, PyObject* args, PyObject* kwargs) {
        static const char* kwlist[] = { "field_id", "index_value", "transaction_id", "object_id", NULL };
        int64_t fieldId;
        PyObject* indexValue;
        int64_t transactionId;
        PyObject* intOrTupleOfInt;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lOlO", (char**)kwlist, &fieldId, &indexValue, &transactionId, &intOrTupleOfInt)) {
            return nullptr;
        }

        return translateExceptionToPyObject([&]() {
            index_value index = pyObjectToIndexValue(indexValue);

            iterateObjectIds(intOrTupleOfInt, [&](object_id oid) {
                self->objects->indexRemove(fieldId, index, transactionId, oid);
            });
            return incref(Py_None);
        });
    }
};

PyMethodDef PyVersionedObjects_methods[] = {
    {"addObjectVersion", (PyCFunction) PyVersionedObjects::addObjectVersion, METH_VARARGS | METH_KEYWORDS},
    {"define", (PyCFunction) PyVersionedObjects::define, METH_VARARGS | METH_KEYWORDS},
    {"markObjectVersionDeleted", (PyCFunction) PyVersionedObjects::markObjectVersionDeleted, METH_VARARGS | METH_KEYWORDS},
    {"bestObjectVersion", (PyCFunction) PyVersionedObjects::bestObjectVersion, METH_VARARGS | METH_KEYWORDS},
    {"moveGuaranteedLowestIdForward", (PyCFunction) PyVersionedObjects::moveGuaranteedLowestIdForward, METH_VARARGS | METH_KEYWORDS},
    {"indexLookupOne", (PyCFunction) PyVersionedObjects::indexLookupOne, METH_VARARGS | METH_KEYWORDS},
    {"indexLookupFirst", (PyCFunction) PyVersionedObjects::indexLookupFirst, METH_VARARGS | METH_KEYWORDS},
    {"indexLookupNext", (PyCFunction) PyVersionedObjects::indexLookupNext, METH_VARARGS | METH_KEYWORDS},
    {"indexAdd", (PyCFunction) PyVersionedObjects::indexAdd, METH_VARARGS | METH_KEYWORDS},
    {"indexRemove", (PyCFunction) PyVersionedObjects::indexRemove, METH_VARARGS | METH_KEYWORDS},
    {NULL}  /* Sentinel */
};


PyTypeObject PyType_VersionedObjects = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "VersionedObjects",
    .tp_basicsize = sizeof(PyVersionedObjects),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor) PyVersionedObjects::dealloc,
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
    .tp_methods = PyVersionedObjects_methods,
    .tp_members = 0,
    .tp_getset = 0,
    .tp_base = 0,
    .tp_dict = 0,
    .tp_descr_get = 0,
    .tp_descr_set = 0,
    .tp_dictoffset = 0,
    .tp_init = (initproc) PyVersionedObjects::init,
    .tp_alloc = 0,
    .tp_new = PyVersionedObjects::new_,
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

