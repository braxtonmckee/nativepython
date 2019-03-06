#include "PyFunctionInstance.hpp"

Function* PyFunctionInstance::type() {
    return (Function*)extractTypeFrom(((PyObject*)this)->ob_type);
}

// static
std::pair<bool, PyObject*> PyFunctionInstance::tryToCallOverload(const Function::Overload& f, PyObject* self, PyObject* args, PyObject* kwargs) {
    PyObjectStealer targetArgTuple(PyTuple_New(PyTuple_Size(args)+(self?1:0)));
    Function::Matcher matcher(f);

    int write_slot = 0;

    if (self) {
        PyTuple_SetItem(targetArgTuple, write_slot++, incref(self));
        matcher.requiredTypeForArg(nullptr);
    }

    for (long k = 0; k < PyTuple_Size(args); k++) {
        PyObjectHolder elt(PyTuple_GetItem(args, k));

        //what type would we need for this unnamed arg?
        Type* targetType = matcher.requiredTypeForArg(nullptr);

        if (!matcher.stillMatches()) {
            return std::make_pair(false, nullptr);
        }

        if (!targetType) {
            incref(elt);
            PyTuple_SetItem(targetArgTuple, write_slot++, elt);
        }
        else {
            try {
                PyObject* targetObj =
                    PyInstance::initializePythonRepresentation(targetType, [&](instance_ptr data) {
                        copyConstructFromPythonInstance(targetType, data, elt);
                    });

                PyTuple_SetItem(targetArgTuple, write_slot++, targetObj);
            } catch(...) {
                //not a valid conversion, but keep going
                return std::make_pair(false, nullptr);
            }
        }
    }

    PyObjectHolder newKwargs;

    if (kwargs) {
        newKwargs.steal(PyDict_New());

        PyObject *key, *value;
        Py_ssize_t pos = 0;

        while (PyDict_Next(kwargs, &pos, &key, &value)) {
            if (!PyUnicode_Check(key)) {
                PyErr_SetString(PyExc_TypeError, "Keywords arguments must be strings.");
                return std::make_pair(false, nullptr);
            }

            //what type would we need for this unnamed arg?
            Type* targetType = matcher.requiredTypeForArg(PyUnicode_AsUTF8(key));

            if (!matcher.stillMatches()) {
                return std::make_pair(false, nullptr);
            }

            if (!targetType) {
                PyDict_SetItem(newKwargs, key, value);
            }
            else {
                try {
                    PyObjectStealer convertedValue(
                        PyInstance::initializePythonRepresentation(targetType, [&](instance_ptr data) {
                            copyConstructFromPythonInstance(targetType, data, value);
                        })
                    );

                    PyDict_SetItem(newKwargs, key, convertedValue);
                } catch(...) {
                    //not a valid conversion
                    return std::make_pair(false, nullptr);
                }
            }
        }
    }

    if (!matcher.definitelyMatches()) {
        return std::make_pair(false, nullptr);
    }

    PyObjectHolder result;

    bool hadNativeDispatch = false;

    if (!native_dispatch_disabled) {
        auto tried_and_result = dispatchFunctionCallToNative(f, targetArgTuple, newKwargs);
        hadNativeDispatch = tried_and_result.first;
        result.steal(tried_and_result.second);
    }

    if (!hadNativeDispatch) {
        result.steal(PyObject_Call((PyObject*)f.getFunctionObj(), targetArgTuple, newKwargs));
    }

    //exceptions pass through directly
    if (!result) {
        return std::make_pair(true, result);
    }

    //force ourselves to convert to the native type
    if (f.getReturnType()) {
        try {
            PyObject* newRes = PyInstance::initializePythonRepresentation(f.getReturnType(), [&](instance_ptr data) {
                    copyConstructFromPythonInstance(f.getReturnType(), data, result);
                });
            return std::make_pair(true, newRes);
        } catch (std::exception& e) {
            PyErr_SetString(PyExc_TypeError, e.what());
            return std::make_pair(true, (PyObject*)nullptr);
        }
    }

    return std::make_pair(true, incref(result));
}

std::pair<bool, PyObject*> PyFunctionInstance::tryToCall(const Function* f, PyObject* arg0, PyObject* arg1, PyObject* arg2) {
    PyObjectStealer argTuple(
        (arg0 && arg1 && arg2) ?
            PyTuple_Pack(3, arg0, arg1, arg2) :
        (arg0 && arg1) ?
            PyTuple_Pack(2, arg0, arg1) :
        arg0 ?
            PyTuple_Pack(1, arg0) :
            PyTuple_Pack(0)
        );

    for (const auto& overload: f->getOverloads()) {
        std::pair<bool, PyObject*> res = PyFunctionInstance::tryToCallOverload(overload, nullptr, argTuple, nullptr);
        if (res.first) {
            return res;
        }
    }

    return std::pair<bool, PyObject*>(false, NULL);
}

// static
std::pair<bool, PyObject*> PyFunctionInstance::dispatchFunctionCallToNative(const Function::Overload& overload, PyObject* argTuple, PyObject *kwargs) {
    for (const auto& spec: overload.getCompiledSpecializations()) {
        auto res = dispatchFunctionCallToCompiledSpecialization(overload, spec, argTuple, kwargs);
        if (res.first) {
            return res;
        }
    }

    return std::pair<bool, PyObject*>(false, (PyObject*)nullptr);
}

std::pair<bool, PyObject*> PyFunctionInstance::dispatchFunctionCallToCompiledSpecialization(
                                                        const Function::Overload& overload,
                                                        const Function::CompiledSpecialization& specialization,
                                                        PyObject* argTuple,
                                                        PyObject *kwargs
                                                        ) {
    Type* returnType = specialization.getReturnType();

    if (!returnType) {
        throw std::runtime_error("Malformed function specialization: missing a return type.");
    }

    if (PyTuple_Size(argTuple) != overload.getArgs().size() || overload.getArgs().size() != specialization.getArgTypes().size()) {
        return std::pair<bool, PyObject*>(false, (PyObject*)nullptr);
    }

    if (kwargs && PyDict_Size(kwargs)) {
        return std::pair<bool, PyObject*>(false, (PyObject*)nullptr);
    }

    std::vector<Instance> instances;

    for (long k = 0; k < overload.getArgs().size(); k++) {
        auto arg = overload.getArgs()[k];
        Type* argType = specialization.getArgTypes()[k];

        if (arg.getIsKwarg() || arg.getIsStarArg()) {
            return std::pair<bool, PyObject*>(false, (PyObject*)nullptr);
        }

        try {
            PyObjectHolder arg(PyTuple_GetItem(argTuple, k));

            instances.push_back(
                Instance::createAndInitialize(argType, [&](instance_ptr p) {
                    copyConstructFromPythonInstance(argType, p, arg);
                })
                );
            }
        catch(...) {
            //failed to convert
            return std::pair<bool, PyObject*>(false, (PyObject*)nullptr);
        }

    }

    try {
        Instance result = Instance::createAndInitialize(returnType, [&](instance_ptr returnData) {
            std::vector<instance_ptr> args;
            for (auto& i: instances) {
                args.push_back(i.data());
            }

            PyEnsureGilReleased releaseTheGIL;

            specialization.getFuncPtr()(returnData, &args[0]);
        });

        return std::pair<bool, PyObject*>(true, (PyObject*)extractPythonObject(result.data(), result.type()));
    } catch(...) {
        const char* e = nativepython_runtime_get_stashed_exception();
        if (!e) {
            e = "Generated code threw an unknown exception.";
        }

        PyErr_Format(PyExc_TypeError, e);
        return std::pair<bool, PyObject*>(true, (PyObject*)nullptr);
    }
}


// static
PyObject* PyFunctionInstance::createOverloadPyRepresentation(Function* f) {
    static PyObject* internalsModule = PyImport_ImportModule("typed_python.internals");

    if (!internalsModule) {
        throw std::runtime_error("Internal error: couldn't find typed_python.internals");
    }

    static PyObject* funcOverload = PyObject_GetAttrString(internalsModule, "FunctionOverload");

    if (!funcOverload) {
        throw std::runtime_error("Internal error: couldn't find typed_python.internals.FunctionOverload");
    }

    PyObjectStealer overloadTuple(PyTuple_New(f->getOverloads().size()));

    for (long k = 0; k < f->getOverloads().size(); k++) {
        auto& overload = f->getOverloads()[k];

        PyObjectStealer pyIndex(PyLong_FromLong(k));

        PyObjectStealer pyOverloadInst(
            PyObject_CallFunctionObjArgs(
                funcOverload,
                typePtrToPyTypeRepresentation(f),
                (PyObject*)pyIndex,
                (PyObject*)overload.getFunctionObj(),
                overload.getReturnType() ? (PyObject*)typePtrToPyTypeRepresentation(overload.getReturnType()) : Py_None,
                NULL
                )
            );

        if (pyOverloadInst) {
            for (auto arg: f->getOverloads()[k].getArgs()) {
                PyObjectStealer res(
                    PyObject_CallMethod(
                        (PyObject*)pyOverloadInst,
                        "addArg",
                        "sOOOO",
                        arg.getName().c_str(),
                        arg.getDefaultValue() ? PyTuple_Pack(1, arg.getDefaultValue()) : Py_None,
                        arg.getTypeFilter() ? (PyObject*)typePtrToPyTypeRepresentation(arg.getTypeFilter()) : Py_None,
                        arg.getIsStarArg() ? Py_True : Py_False,
                        arg.getIsKwarg() ? Py_True : Py_False
                        )
                    );

                if (!res) {
                    PyErr_PrintEx(0);
                }
            }

            PyTuple_SetItem(overloadTuple, k, incref(pyOverloadInst));
        } else {
            PyErr_PrintEx(0);
            PyTuple_SetItem(overloadTuple, k, incref(Py_None));
        }
    }

    return incref(overloadTuple);
}

PyObject* PyFunctionInstance::tp_call_concrete(PyObject* args, PyObject* kwargs) {
    for (const auto& overload: type()->getOverloads()) {
        std::pair<bool, PyObject*> res = PyFunctionInstance::tryToCallOverload(overload, nullptr, args, kwargs);
        if (res.first) {
            return res.second;
        }
    }

    std::string argTupleTypeDesc = argTupleTypeDescription(args, kwargs);

    PyErr_Format(
        PyExc_TypeError, "'%s' cannot find a valid overload with arguments of type %s",
        type()->name().c_str(),
        argTupleTypeDesc.c_str()
        );

    return NULL;
}

std::string PyFunctionInstance::argTupleTypeDescription(PyObject* args, PyObject* kwargs) {
    std::ostringstream outTypes;
    outTypes << "(";
    bool first = true;
    for (long k = 0; k < PyTuple_Size(args); k++) {
        if (!first) {
            outTypes << ",";
        } else {
            first = false;
        }
        outTypes << PyTuple_GetItem(args,k)->ob_type->tp_name;
    }
    if (kwargs) {
        PyObject *key, *value;
        Py_ssize_t pos = 0;

        while (kwargs && PyDict_Next(kwargs, &pos, &key, &value)) {
            if (!first) {
                outTypes << ",";
            } else {
                first = false;
            }
            outTypes << PyUnicode_AsUTF8(key) << "=" << value->ob_type->tp_name;
        }
    }

    outTypes << ")";

    return outTypes.str();
}

void PyFunctionInstance::mirrorTypeInformationIntoPyTypeConcrete(Function* inType, PyTypeObject* pyType) {
    //expose a list of overloads
    PyObjectStealer overloads(createOverloadPyRepresentation(inType));

    PyDict_SetItemString(
            pyType->tp_dict,
            "overloads",
            overloads
            );
}
