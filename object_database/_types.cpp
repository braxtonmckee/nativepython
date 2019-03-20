#include <Python.h>
#include <numpy/arrayobject.h>
#include "PyVersionedObjectsOfType.hpp"
#include "PyVersionedIdSet.hpp"
#include "PyVersionedIdSets.hpp"

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

    if (PyType_Ready(&PyType_VersionedIdSet) < 0)
        return NULL;

    if (PyType_Ready(&PyType_VersionedIdSets) < 0)
        return NULL;

    PyObject *module = PyModule_Create(&moduledef);

    if (module == NULL)
        return NULL;

    Py_INCREF(&PyType_VersionedObjectsOfType);

    PyModule_AddObject(module, "VersionedObjectsOfType", (PyObject *)&PyType_VersionedObjectsOfType);
    PyModule_AddObject(module, "VersionedIdSets", (PyObject *)&PyType_VersionedIdSets);
    PyModule_AddObject(module, "VersionedIdSet", (PyObject *)&PyType_VersionedIdSet);

    return module;
}
