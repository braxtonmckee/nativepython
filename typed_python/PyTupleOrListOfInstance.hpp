#pragma once

#include "PyInstance.hpp"

class PyTupleOrListOfInstance : public PyInstance {
public:
    typedef TupleOrListOf modeled_type;

    TupleOrListOf* type();

    PyObject* sq_item_concrete(Py_ssize_t ix);

    Py_ssize_t mp_and_sq_length_concrete();

    PyObject* mp_subscript_concrete(PyObject* item);

    static void copyConstructFromPythonInstanceConcrete(TupleOrListOf* tupT, instance_ptr tgt, PyObject* pyRepresentation, bool isExplicit);

    PyObject* pyOperatorConcrete(PyObject* rhs, const char* op, const char* opErr);

    PyObject* pyOperatorAdd(PyObject* rhs, const char* op, const char* opErr, bool reversed);

    PyObject* pyOperatorConcreteReverse(PyObject* lhs, const char* op, const char* opErrRep);

    static PyObject* toArray(PyObject* o, PyObject* args);

    static bool pyValCouldBeOfTypeConcrete(modeled_type* type, PyObject* pyRepresentation);

    static void mirrorTypeInformationIntoPyTypeConcrete(TupleOrListOf* inType, PyTypeObject* pyType);
};

class PyListOfInstance : public PyTupleOrListOfInstance {
public:
    typedef ListOf modeled_type;

    ListOf* type();

    static PyObject* listAppend(PyObject* o, PyObject* args);

    static PyObject* listResize(PyObject* o, PyObject* args);

    static PyObject* listReserve(PyObject* o, PyObject* args);

    static PyObject* listClear(PyObject* o, PyObject* args);

    static PyObject* listReserved(PyObject* o, PyObject* args);

    static PyObject* listPop(PyObject* o, PyObject* args);

    static PyObject* listSetSizeUnsafe(PyObject* o, PyObject* args);

    static PyObject* listPointerUnsafe(PyObject* o, PyObject* args);

    int mp_ass_subscript_concrete(PyObject* item, PyObject* value);

    static PyMethodDef* typeMethodsConcrete();

    static void constructFromPythonArgumentsConcrete(ListOf* t, uint8_t* data, PyObject* args, PyObject* kwargs);

    static bool compare_to_python_concrete(ListOf* listT, instance_ptr self, PyObject* other, bool exact, int pyComparisonOp) {
        auto convert = [&](char cmpValue) { return cmpResultToBoolForPyOrdering(pyComparisonOp, cmpValue); };

        if (!PyList_Check(other)) {
            return convert(-1);
        }

        int lenO = PyList_Size(other);
        int lenS = listT->count(self);
        for (long k = 0; k < lenO && k < lenS; k++) {
            PyObjectHolder listItem(PyList_GetItem(other,k));

            if (!compare_to_python(listT->getEltType(), listT->eltPtr(self, k), listItem, exact, Py_EQ)) {
                if (compare_to_python(listT->getEltType(), listT->eltPtr(self, k), listItem, exact, Py_LT)) {
                    return convert(-1);
                }
                return convert(1);
            }
        }

        if (lenS < lenO) { return convert(-1); }
        if (lenS > lenO) { return convert(1); }
        return convert(0);
    }
};

class PyTupleOfInstance : public PyTupleOrListOfInstance {
public:
    typedef TupleOf modeled_type;

    TupleOf* type();

    static PyMethodDef* typeMethodsConcrete();

    static bool compare_to_python_concrete(TupleOf* tupT, instance_ptr self, PyObject* other, bool exact, int pyComparisonOp) {
        auto convert = [&](char cmpValue) { return cmpResultToBoolForPyOrdering(pyComparisonOp, cmpValue); };

        if (!PyTuple_Check(other)) {
            return convert(-1);
        }

        int lenO = PyTuple_Size(other);
        int lenS = tupT->count(self);

        for (long k = 0; k < lenO && k < lenS; k++) {
            PyObjectHolder arg(PyTuple_GetItem(other,k));

            if (!compare_to_python(tupT->getEltType(), tupT->eltPtr(self, k), arg, exact, Py_EQ)) {
                if (compare_to_python(tupT->getEltType(), tupT->eltPtr(self, k), arg, exact, Py_LT)) {
                    return convert(-1);
                }
                return convert(1);
            }
        }

        if (lenS < lenO) { return convert(-1); }
        if (lenS > lenO) { return convert(1); }
        return convert(0);
    }

};
