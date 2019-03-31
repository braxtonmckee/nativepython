#include <Python.h>
#include <numpy/arrayobject.h>
#include "../typed_python/AllTypes.hpp"
#include "../typed_python/PyInstance.hpp"
#include "PyVersionedObjectsOfType.hpp"
#include "PyVersionedObjects.hpp"
#include "PyVersionedIdSet.hpp"

PyObject *bytesToIndexValue(PyObject *none, PyObject* args, PyObject* kwargs)
{
    static const char *kwlist[] = {"value", NULL};
    PyObject* bytes;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", (char**)kwlist, &bytes)) {
        return nullptr;
    }

    static Type* indexTupleType = Tuple::Make(
        std::vector<Type*>(
            {Int64::Make(), Int64::Make(), Int64::Make(), Int64::Make(), Int64::Make()}
        )
    );

    if (!PyBytes_Check(bytes)) {
        PyErr_Format(PyExc_TypeError, "Expected bytes, got %S", bytes);
        return NULL;
    }

    char* buffer;
    Py_ssize_t length;

    if (PyBytes_AsStringAndSize(bytes, &buffer, &length) == -1) {
        return nullptr;
    }

    if (length > 20) {
        PyErr_Format(PyExc_TypeError, "Expected bytes of size less than or equal to 20");
        return NULL;
    }

    char actualBuffer[20];
    memset(actualBuffer, 0, 20);
    memcpy(actualBuffer, buffer, length);

    return PyInstance::extractPythonObject((instance_ptr)actualBuffer, indexTupleType);
}

static PyMethodDef module_methods[] = {
    {"bytesToIndexValue", (PyCFunction)bytesToIndexValue, METH_VARARGS | METH_KEYWORDS, NULL},
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

    if (PyType_Ready(&PyType_VersionedIdSet) < 0)
        return NULL;

    if (PyType_Ready(&PyType_VersionedObjects) < 0)
        return NULL;

    PyObject *module = PyModule_Create(&moduledef);

    if (module == NULL)
        return NULL;

    Py_INCREF(&PyType_VersionedObjectsOfType);

    PyModule_AddObject(module, "VersionedObjectsOfType", (PyObject *)&PyType_VersionedObjectsOfType);
    PyModule_AddObject(module, "VersionedIdSet", (PyObject *)&PyType_VersionedIdSet);
    PyModule_AddObject(module, "VersionedObjects", (PyObject *)&PyType_VersionedObjects);

    return module;
}
