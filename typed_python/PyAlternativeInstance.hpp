#pragma once

#include "PyInstance.hpp"

class PyAlternativeInstance : public PyInstance {
public:
    typedef Alternative modeled_type;

    Alternative* type();

    PyObject* pyTernaryOperatorConcrete(PyObject* rhs, PyObject* ternary, const char* op, const char* opErr);

    PyObject* pyOperatorConcrete(PyObject* rhs, const char* op, const char* opErr);

    PyObject* pyUnaryOperatorConcrete(const char* op, const char* opErr);

    static bool pyValCouldBeOfTypeConcrete(modeled_type* type, PyObject* pyRepresentation) {
        return true;
    }
};

class PyConcreteAlternativeInstance : public PyInstance {
public:
    typedef ConcreteAlternative modeled_type;

    ConcreteAlternative* type();

    PyObject* pyTernaryOperatorConcrete(PyObject* rhs, PyObject* ternary, const char* op, const char* opErr);

    PyObject* pyOperatorConcrete(PyObject* rhs, const char* op, const char* opErr);

    PyObject* pyUnaryOperatorConcrete(const char* op, const char* opErr);

    static bool pyValCouldBeOfTypeConcrete(modeled_type* type, PyObject* pyRepresentation) {
        return true;
    }

    static void constructFromPythonArgumentsConcrete(ConcreteAlternative* t, uint8_t* data, PyObject* args, PyObject* kwargs);

};