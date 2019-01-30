#include "util.hpp"
#include "Instance.hpp"
#include "PyInstance.hpp"

bool unpackTupleToTypes(PyObject* tuple, std::vector<Type*>& out) {
    if (!PyTuple_Check(tuple)) {
        PyErr_SetString(PyExc_TypeError, "Argument to type tuple was not a tuple");
        return false;
    }
    for (int i = 0; i < PyTuple_Size(tuple); ++i) {
        PyObject* entry = PyTuple_GetItem(tuple, i);
        Type* targetType = NULL;

        targetType = PyInstance::tryUnwrapPyInstanceToType(entry);
        if (!targetType) {
            PyErr_Format(PyExc_TypeError, "Expected a type in position %d of type tuple. Got %S instead.", i, entry);
            return false;
        }

        out.push_back(targetType);
    }

    return true;
}

bool unpackTupleToStringAndTypes(PyObject* tuple, std::vector<std::pair<std::string, Type*> >& out) {
    std::set<std::string> memberNames;

    for (int i = 0; i < PyTuple_Size(tuple); ++i) {
        PyObject* entry = PyTuple_GetItem(tuple, i);
        Type* targetType = NULL;

        if (!PyTuple_Check(entry) || PyTuple_Size(entry) != 2
                || !PyUnicode_Check(PyTuple_GetItem(entry, 0))
                || !(targetType =
                    PyInstance::tryUnwrapPyInstanceToType(
                        PyTuple_GetItem(entry, 1)
                        ))
                )
        {
            PyErr_SetString(PyExc_TypeError, "Badly formed class type argument.");
            return false;
        }

        std::string memberName = PyUnicode_AsUTF8(PyTuple_GetItem(entry, 0));

        if (memberNames.find(memberName) != memberNames.end()) {
            PyErr_Format(PyExc_TypeError, "Cannot redefine Class member %s", memberName.c_str());
            return false;
        }

        memberNames.insert(memberName);

        out.push_back(
            std::make_pair(memberName, targetType)
            );
    }

    return true;
}

bool unpackTupleToStringTypesAndValues(PyObject* tuple, std::vector<std::tuple<std::string, Type*, Instance> >& out) {
    std::set<std::string> memberNames;

    for (int i = 0; i < PyTuple_Size(tuple); ++i) {
        PyObject* entry = PyTuple_GetItem(tuple, i);
        Type* targetType = NULL;

        if (!PyTuple_Check(entry) || PyTuple_Size(entry) != 3
                || !PyUnicode_Check(PyTuple_GetItem(entry, 0))
                || !(targetType =
                    PyInstance::tryUnwrapPyInstanceToType(
                        PyTuple_GetItem(entry, 1)
                        ))
                )
        {
            PyErr_SetString(PyExc_TypeError, "Badly formed class type argument.");
            return false;
        }

        std::string memberName = PyUnicode_AsUTF8(PyTuple_GetItem(entry, 0));

        if (memberNames.find(memberName) != memberNames.end()) {
            PyErr_Format(PyExc_TypeError, "Cannot redefine Class member %s", memberName.c_str());
            return false;
        }

        memberNames.insert(memberName);

        Instance inst = PyInstance::unwrapPyObjectToInstance(PyTuple_GetItem(entry, 2));

        if (PyErr_Occurred()) {
            return false;
        }

        out.push_back(
            std::make_tuple(
                memberName,
                targetType,
                inst
            )
        );
    }

    return true;
}

bool unpackTupleToStringAndObjects(PyObject* tuple, std::vector<std::pair<std::string, PyObject*> >& out) {
    std::set<std::string> memberNames;

    for (int i = 0; i < PyTuple_Size(tuple); ++i) {
        PyObject* entry = PyTuple_GetItem(tuple, i);
        Type* targetType = NULL;

        if (!PyTuple_Check(entry) || PyTuple_Size(entry) != 2
                || !PyUnicode_Check(PyTuple_GetItem(entry, 0))
                )
        {
            PyErr_SetString(PyExc_TypeError, "Badly formed class type argument.");
            return false;
        }

        std::string memberName = PyUnicode_AsUTF8(PyTuple_GetItem(entry,0));

        if (memberNames.find(memberName) != memberNames.end()) {
            PyErr_Format(PyExc_TypeError, "Cannot redefine Class member %s", memberName.c_str());
            return false;
        }

        memberNames.insert(memberName);

        PyObject* item = PyTuple_GetItem(entry, 1);
        Py_INCREF(item);

        out.push_back(
            std::make_pair(memberName, item)
            );
    }

    return true;
}