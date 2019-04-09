#pragma once

#include "PyInstance.hpp"

class PyOneOfInstance : public PyInstance {
public:
    typedef OneOfType modeled_type;

    static void copyConstructFromPythonInstanceConcrete(OneOfType* oneOf, instance_ptr tgt, PyObject* pyRepresentation, bool isExplicit) {
        for (long k = 0; k < oneOf->getTypes().size(); k++) {
            Type* subtype = oneOf->getTypes()[k];

            if (pyValCouldBeOfType(subtype, pyRepresentation)) {
                try {
                    copyConstructFromPythonInstance(subtype, tgt+1, pyRepresentation);
                    *(uint8_t*)tgt = k;
                    return;
                } catch(PythonExceptionSet& e) {
                    PyErr_Clear();
                } catch(...) {
                }
            }
        }

        throw std::logic_error("Can't initialize a " + oneOf->name() + " from an instance of " +
            std::string(pyRepresentation->ob_type->tp_name));
    }

    static bool pyValCouldBeOfTypeConcrete(modeled_type* type, PyObject* pyRepresentation) {
        return true;
    }

    static PyObject* extractPythonObjectConcrete(modeled_type* oneofT, instance_ptr data) {
        std::pair<Type*, instance_ptr> child = oneofT->unwrap(data);
        return extractPythonObject(child.second, child.first);
    }

    static bool compare_to_python_concrete(OneOfType* t, instance_ptr self, PyObject* other, bool exact, int pyComparisonOp) {
        std::pair<Type*, instance_ptr> child = t->unwrap(self);
        return compare_to_python(child.first, child.second, other, exact, pyComparisonOp);
    }

    static void mirrorTypeInformationIntoPyTypeConcrete(OneOfType* oneOfT, PyTypeObject* pyType) {
        PyObjectStealer types(PyTuple_New(oneOfT->getTypes().size()));

        for (long k = 0; k < oneOfT->getTypes().size(); k++) {
            PyTuple_SetItem(types, k, incref(typePtrToPyTypeRepresentation(oneOfT->getTypes()[k])));
        }

        //expose 'ElementType' as a member of the type object
        PyDict_SetItemString(pyType->tp_dict, "Types", types);
    }
};
