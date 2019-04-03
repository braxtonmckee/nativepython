#pragma once

#include "PyInstance.hpp"

class PyStringInstance : public PyInstance {
public:
    typedef StringType modeled_type;

    static void copyConstructFromPythonInstanceConcrete(StringType* eltType, instance_ptr tgt, PyObject* pyRepresentation, bool isExplicit) {
        if (PyUnicode_Check(pyRepresentation)) {
            auto kind = PyUnicode_KIND(pyRepresentation);
            assert(
                kind == PyUnicode_1BYTE_KIND ||
                kind == PyUnicode_2BYTE_KIND ||
                kind == PyUnicode_4BYTE_KIND
                );
            StringType().constructor(
                tgt,
                kind == PyUnicode_1BYTE_KIND ? 1 :
                kind == PyUnicode_2BYTE_KIND ? 2 :
                                                4,
                PyUnicode_GET_LENGTH(pyRepresentation),
                kind == PyUnicode_1BYTE_KIND ? (const char*)PyUnicode_1BYTE_DATA(pyRepresentation) :
                kind == PyUnicode_2BYTE_KIND ? (const char*)PyUnicode_2BYTE_DATA(pyRepresentation) :
                                               (const char*)PyUnicode_4BYTE_DATA(pyRepresentation)
                );
            return;
        }
        throw std::logic_error("Can't initialize a StringType from an instance of " +
            std::string(pyRepresentation->ob_type->tp_name));
    }

    static bool pyValCouldBeOfTypeConcrete(modeled_type* type, PyObject* pyRepresentation) {
        return PyUnicode_Check(pyRepresentation);
    }

    static PyObject* extractPythonObjectConcrete(StringType* t, instance_ptr data) {
        int bytes_per_codepoint = StringType().bytes_per_codepoint(data);

        return PyUnicode_FromKindAndData(
            bytes_per_codepoint == 1 ? PyUnicode_1BYTE_KIND :
            bytes_per_codepoint == 2 ? PyUnicode_2BYTE_KIND :
                                       PyUnicode_4BYTE_KIND,
            StringType().eltPtr(data, 0),
            StringType().count(data)
            );
    }

    static bool compare_to_python_concrete(StringType* t, instance_ptr self, PyObject* other, bool exact, int pyComparisonOp) {
        if (!PyUnicode_Check(other)) {
            return cmpResultToBoolForPyOrdering(pyComparisonOp, -1);
        }

        auto kind = PyUnicode_KIND(other);

        int bytesPer = kind == PyUnicode_1BYTE_KIND ? 1 :
            kind == PyUnicode_2BYTE_KIND ? 2 : 4;

        if (bytesPer != t->bytes_per_codepoint(self)) {
            return cmpResultToBoolForPyOrdering(pyComparisonOp, -1);
        }

        if (PyUnicode_GET_LENGTH(other) < t->count(self)) {
            return cmpResultToBoolForPyOrdering(pyComparisonOp, -1);
        }

        if (PyUnicode_GET_LENGTH(other) > t->count(self)) {
            return cmpResultToBoolForPyOrdering(pyComparisonOp, 1);
        }

        return cmpResultToBoolForPyOrdering(
            pyComparisonOp,
            memcmp(
                kind == PyUnicode_1BYTE_KIND ? (const char*)PyUnicode_1BYTE_DATA(other) :
                kind == PyUnicode_2BYTE_KIND ? (const char*)PyUnicode_2BYTE_DATA(other) :
                                               (const char*)PyUnicode_4BYTE_DATA(other),
                ((StringType*)t)->eltPtr(self, 0),
                PyUnicode_GET_LENGTH(other) * bytesPer
                )
            );
    }

};

