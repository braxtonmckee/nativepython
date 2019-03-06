#include "PyAlternativeInstance.hpp"
#include "PyFunctionInstance.hpp"

Alternative* PyAlternativeInstance::type() {
    return (Alternative*)extractTypeFrom(((PyObject*)this)->ob_type);
}

ConcreteAlternative* PyConcreteAlternativeInstance::type() {
    return (ConcreteAlternative*)extractTypeFrom(((PyObject*)this)->ob_type);
}

PyObject* PyAlternativeInstance::pyTernaryOperatorConcrete(PyObject* rhs, PyObject* thirdArg, const char* op, const char* opErr) {
    auto it = type()->getMethods().find(op);

    if (it != type()->getMethods().end()) {
        Function* f = it->second;

        PyObjectStealer argTuple(
            PyTuple_Pack(3, (PyObject*)this, (PyObject*)rhs, (PyObject*)thirdArg)
            );

        for (const auto& overload: f->getOverloads()) {
            std::pair<bool, PyObject*> res = PyFunctionInstance::tryToCallOverload(overload, nullptr, argTuple, nullptr);
            if (res.first) {
                return res.second;
            }
        }
    }

    return ((PyInstance*)this)->pyTernaryOperatorConcrete(rhs, thirdArg, op, opErr);
}
PyObject* PyConcreteAlternativeInstance::pyTernaryOperatorConcrete(PyObject* rhs, PyObject* thirdArg, const char* op, const char* opErr) {
    auto it = type()->getAlternative()->getMethods().find(op);

    if (it != type()->getAlternative()->getMethods().end()) {
        Function* f = it->second;

        PyObjectStealer argTuple(
            PyTuple_Pack(3, (PyObject*)this, (PyObject*)rhs, (PyObject*)thirdArg)
            );

        for (const auto& overload: f->getOverloads()) {
            std::pair<bool, PyObject*> res = PyFunctionInstance::tryToCallOverload(overload, nullptr, argTuple, nullptr);
            if (res.first) {
                return res.second;
            }
        }
    }

    return ((PyInstance*)this)->pyTernaryOperatorConcrete(rhs, thirdArg, op, opErr);
}

PyObject* PyAlternativeInstance::pyOperatorConcrete(PyObject* rhs, const char* op, const char* opErr) {
    auto it = type()->getMethods().find(op);

    if (it != type()->getMethods().end()) {
        Function* f = it->second;

        PyObjectStealer argTuple(
            PyTuple_Pack(2, (PyObject*)this, (PyObject*)rhs)
            );

        for (const auto& overload: f->getOverloads()) {
            std::pair<bool, PyObject*> res = PyFunctionInstance::tryToCallOverload(overload, nullptr, argTuple, nullptr);
            if (res.first) {
                return res.second;
            }
        }
    }

    return ((PyInstance*)this)->pyOperatorConcrete(rhs, op, opErr);
}
PyObject* PyConcreteAlternativeInstance::pyOperatorConcrete(PyObject* rhs, const char* op, const char* opErr) {
    auto it = type()->getAlternative()->getMethods().find(op);

    if (it != type()->getAlternative()->getMethods().end()) {
        Function* f = it->second;

        PyObjectStealer argTuple(
            PyTuple_Pack(2, (PyObject*)this, (PyObject*)rhs)
            );

        for (const auto& overload: f->getOverloads()) {
            std::pair<bool, PyObject*> res = PyFunctionInstance::tryToCallOverload(overload, nullptr, argTuple, nullptr);
            if (res.first) {
                return res.second;
            }
        }
    }

    return ((PyInstance*)this)->pyOperatorConcrete(rhs, op, opErr);
}

PyObject* PyAlternativeInstance::pyUnaryOperatorConcrete(const char* op, const char* opErr) {
    auto it = type()->getMethods().find(op);

    if (it != type()->getMethods().end()) {
        Function* f = it->second;

        PyObjectStealer argTuple(
            PyTuple_Pack(2, (PyObject*)this)
            );

        for (const auto& overload: f->getOverloads()) {
            std::pair<bool, PyObject*> res = PyFunctionInstance::tryToCallOverload(overload, nullptr, argTuple, nullptr);
            if (res.first) {
                return res.second;
            }
        }
    }

    return ((PyInstance*)this)->pyUnaryOperatorConcrete(op, opErr);
}
PyObject* PyConcreteAlternativeInstance::pyUnaryOperatorConcrete(const char* op, const char* opErr) {
    auto it = type()->getAlternative()->getMethods().find(op);

    if (it != type()->getAlternative()->getMethods().end()) {
        Function* f = it->second;

        PyObjectStealer argTuple(
            PyTuple_Pack(2, (PyObject*)this)
            );

        for (const auto& overload: f->getOverloads()) {
            std::pair<bool, PyObject*> res = PyFunctionInstance::tryToCallOverload(overload, nullptr, argTuple, nullptr);
            if (res.first) {
                return res.second;
            }
        }
    }

    return ((PyInstance*)this)->pyUnaryOperatorConcrete(op, opErr);
}

void PyConcreteAlternativeInstance::constructFromPythonArgumentsConcrete(ConcreteAlternative* alt, uint8_t* data, PyObject* args, PyObject* kwargs) {
    alt->constructor(data, [&](instance_ptr p) {
        if ((kwargs == nullptr || PyDict_Size(kwargs) == 0) && PyTuple_Size(args) == 1) {
            //construct an alternative from a single argument.
            //if it's a binary compatible subtype of the alternative we're constructing, then
            //invoke the copy constructor.
            PyObjectHolder arg(PyTuple_GetItem(args, 0));
            Type* argType = extractTypeFrom(arg->ob_type);

            if (argType && alt == argType) {
                //it's already the right kind of instance, so we can copy-through the underlying element
                alt->elementType()->copy_constructor(p, alt->eltPtr(((PyInstance*)(PyObject*)arg)->dataPtr()));
                return;
            }

            //otherwise, if we have exactly one subelement, attempt to construct from that
            if (alt->elementType()->getTypeCategory() != Type::TypeCategory::catNamedTuple) {
                throw std::runtime_error("ConcreteAlternatives are supposed to only contain NamedTuples");
            }

            NamedTuple* alternativeEltType = (NamedTuple*)alt->elementType();

            if (alternativeEltType->getTypes().size() != 1) {
                throw std::logic_error("Can't initialize " + alt->name() + " with positional arguments because it doesn't have only one field.");
            }

            PyInstance::copyConstructFromPythonInstance(alternativeEltType->getTypes()[0], p, arg);
        } else if (PyTuple_Size(args) == 0) {
            //construct an alternative from Kwargs
            constructFromPythonArguments(p, alt->elementType(), args, kwargs);
        } else {
            throw std::logic_error("Can only initialize " + alt->name() + " from python with kwargs or a single in-place argument");
        }
    });
}

void PyAlternativeInstance::copyConstructFromPythonInstanceConcrete(Alternative* altType, instance_ptr tgt, PyObject* pyRepresentation, bool isExplicit) {
    Type* argType = extractTypeFrom(pyRepresentation->ob_type);

    if (argType && argType->getTypeCategory() == Type::TypeCategory::catConcreteAlternative &&
            ((ConcreteAlternative*)argType)->getAlternative() == altType) {
        altType->copy_constructor(tgt, ((PyInstance*)pyRepresentation)->dataPtr());
        return;
    }

    PyInstance::copyConstructFromPythonInstanceConcrete(altType, tgt, pyRepresentation, isExplicit);
}


PyObject* PyAlternativeInstance::tp_getattr_concrete(PyObject* pyAttrName, const char* attrName) {
    if (mIsMatcher) {
        if (type()->subtypes()[type()->which(dataPtr())].first == attrName) {
            return incref(Py_True);
        }
        return incref(Py_False);
    }

    if (strcmp(attrName,"matches") == 0) {
        PyInstance* self = duplicate();

        self->mIteratorOffset = -1;
        self->mIsMatcher = true;

        return (PyObject*)self;
    }

    //see if its a method
    Alternative* toCheck = type();

    auto it = toCheck->getMethods().find(attrName);
    if (it != toCheck->getMethods().end()) {
        return PyMethod_New((PyObject*)it->second->getOverloads()[0].getFunctionObj(), (PyObject*)this);
    }

    //see if its a member of our held type
    NamedTuple* heldT = (NamedTuple*)type()->subtypes()[type()->which(dataPtr())].second;
    instance_ptr heldData = type()->eltPtr(dataPtr());

    int ix = heldT->indexOfName(attrName);
    if (ix >= 0) {
        return extractPythonObject(
            heldT->eltPtr(heldData, ix),
            heldT->getTypes()[ix]
            );
    }

    return PyInstance::tp_getattr_concrete(pyAttrName, attrName);
}

PyObject* PyConcreteAlternativeInstance::tp_getattr_concrete(PyObject* pyAttrName, const char* attrName) {
    if (mIsMatcher) {
        if (type()->getAlternative()->subtypes()[type()->getAlternative()->which(dataPtr())].first == attrName) {
            return incref(Py_True);
        }
        return incref(Py_False);
    }

    if (strcmp(attrName,"matches") == 0) {
        PyInstance* self = duplicate();

        self->mIteratorOffset = -1;
        self->mIsMatcher = true;

        return (PyObject*)self;
    }

    //see if its a method
    Alternative* toCheck = type()->getAlternative();

    auto it = toCheck->getMethods().find(attrName);
    if (it != toCheck->getMethods().end()) {
        return PyMethod_New((PyObject*)it->second->getOverloads()[0].getFunctionObj(), (PyObject*)this);
    }

    //see if its a member of our held type
    NamedTuple* heldT = (NamedTuple*)type()->getAlternative()->subtypes()[type()->which()].second;
    instance_ptr heldData = type()->getAlternative()->eltPtr(dataPtr());

    int ix = heldT->indexOfName(attrName);
    if (ix >= 0) {
        return extractPythonObject(
            heldT->eltPtr(heldData, ix),
            heldT->getTypes()[ix]
            );
    }

    return PyInstance::tp_getattr_concrete(pyAttrName, attrName);
}

void PyAlternativeInstance::mirrorTypeInformationIntoPyTypeConcrete(Alternative* alt, PyTypeObject* pyType) {
    PyObjectStealer alternatives(PyTuple_New(alt->subtypes().size()));

    for (long k = 0; k < alt->subtypes().size(); k++) {
        ConcreteAlternative* concrete = ConcreteAlternative::Make(alt, k);

        PyDict_SetItemString(
            pyType->tp_dict,
            alt->subtypes()[k].first.c_str(),
            (PyObject*)typeObjInternal(concrete)
            );

        PyTuple_SetItem(alternatives, k, incref((PyObject*)typeObjInternal(concrete)));
    }

    PyDict_SetItemString(
        pyType->tp_dict,
        "__typed_python_alternatives__",
        alternatives
        );
}

void PyConcreteAlternativeInstance::mirrorTypeInformationIntoPyTypeConcrete(ConcreteAlternative* alt, PyTypeObject* pyType) {
    PyDict_SetItemString(
        pyType->tp_dict,
        "Alternative",
        (PyObject*)typeObjInternal(alt->getAlternative())
        );

    PyDict_SetItemString(
        pyType->tp_dict,
        "Index",
        PyLong_FromLong(alt->which())
        );

    PyDict_SetItemString(
        pyType->tp_dict,
        "Name",
        PyUnicode_FromString(alt->getAlternative()->subtypes()[alt->which()].first.c_str())
        );

    PyDict_SetItemString(
        pyType->tp_dict,
        "ElementType",
        (PyObject*)typeObjInternal(alt->elementType())
        );

}


int PyAlternativeInstance::tp_setattr_concrete(PyObject* attrName, PyObject* attrVal) {
    PyErr_Format(
        PyExc_AttributeError,
        "Cannot set attributes on instance of type '%s' because it is immutable",
        type()->name().c_str()
    );
    return -1;
}


int PyConcreteAlternativeInstance::tp_setattr_concrete(PyObject* attrName, PyObject* attrVal) {
    PyErr_Format(
        PyExc_AttributeError,
        "Cannot set attributes on instance of type '%s' because it is immutable",
        type()->name().c_str()
    );
    return -1;
}


PyObject* PyConcreteAlternativeInstance::tp_call_concrete(PyObject* args, PyObject* kwargs) {
    auto it = type()->getAlternative()->getMethods().find("__call__");

    if (it == type()->getAlternative()->getMethods().end()) {
        PyErr_Format(
            PyExc_TypeError,
            "'%s' object is not callable because '__call__' was not defined",
            type()->name().c_str()
            );
        throw PythonExceptionSet();
    } else {
        Function* f = it->second;

        for (const auto& overload: f->getOverloads()) {
            std::pair<bool, PyObject*> res = PyFunctionInstance::tryToCallOverload(
                overload, (PyObject*)this, args, kwargs
                );

            if (res.first) {
                return res.second;
            }
        }
    }
}

