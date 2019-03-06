#include <Python.h>
#include <numpy/arrayobject.h>
#include <map>
#include <memory>
#include <vector>
#include <string>
#include <iostream>

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
        int objectId;
        int versionId;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iiO", (char**)kwlist, &objectId, &versionId, &instance)) {
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

    static PyObject *best(PyVersionedObjectsOfType *self, PyObject* args, PyObject* kwargs)
    {
        static const char *kwlist[] = {"objectId", "versionId", NULL};
        int objectId;
        int versionId;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ii", (char**)kwlist, &objectId, &versionId)) {
            return nullptr;
        }

        auto instancePtrAndVersionPair = self->objectsOfType->best(objectId, versionId);

        if (!instancePtrAndVersionPair.first) {
            return incref(Py_None);
        }

        PyObjectStealer ref(PyInstance::extractPythonObject(instancePtrAndVersionPair.first, self->objectsOfType->getType()));
        PyObjectStealer version(PyLong_FromLong(instancePtrAndVersionPair.second));

        return PyTuple_Pack(2, (PyObject*)ref, (PyObject*)version);
    }

    static PyObject *remove(PyVersionedObjectsOfType *self, PyObject* args, PyObject* kwargs)
    {
        static const char *kwlist[] = {"objectId", "versionId", NULL};
        int objectId;
        int versionId;

        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ii", (char**)kwlist, &objectId, &versionId)) {
            return nullptr;
        }

        return incref(self->objectsOfType->remove(objectId, versionId) ? Py_True:Py_False);
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
};

static PyMethodDef PyVersionedObjectsOfType_methods[] = {
    {"add", (PyCFunction) PyVersionedObjectsOfType::add, METH_VARARGS | METH_KEYWORDS},
    {"best", (PyCFunction) PyVersionedObjectsOfType::best, METH_VARARGS | METH_KEYWORDS},
    {"remove", (PyCFunction) PyVersionedObjectsOfType::remove, METH_VARARGS | METH_KEYWORDS},
    //{"remove", (PyCFunction) PyVersionedObjectsOfType::remove, METH_NOARGS},

    {NULL}  /* Sentinel */
};


static PyTypeObject PyType_VersionedObjectsOfType = {
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

static PyMethodDef module_methods[] = {
    {NULL, NULL}
};

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_types",
    .m_doc = NULL,
    .m_size = 0,
    .m_methods = module_methods,
    .m_slots = NULL,
    .m_traverse = NULL,
    .m_clear = NULL,
    .m_free = NULL
};

PyMODINIT_FUNC
PyInit__types(void)
{
    //initialize numpy. This is only OK because all the .cpp files get
    //glommed together in a single file. If we were to change that behavior,
    //then additional steps must be taken as per the API documentation.
    import_array();

    if (PyType_Ready(&PyType_VersionedObjectsOfType) < 0)
        return NULL;

    PyObject *module = PyModule_Create(&moduledef);

    if (module == NULL)
        return NULL;

    Py_INCREF(&PyType_VersionedObjectsOfType);

    PyModule_AddObject(module, "VersionedObjectsOfType", (PyObject *)&PyType_VersionedObjectsOfType);

    return module;
}
