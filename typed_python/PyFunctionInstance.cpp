#include "PyFunctionInstance.hpp"

Function* PyFunctionInstance::type() {
    return (Function*)extractTypeFrom(((PyObject*)this)->ob_type);
}


// static
std::pair<bool, PyObject*> PyFunctionInstance::tryToCallOverload(const Function::Overload& f, PyObject* self, PyObject* args, PyObject* kwargs) {
    PyObject* targetArgTuple = PyTuple_New(PyTuple_Size(args)+(self?1:0));
    Function::Matcher matcher(f);

    int write_slot = 0;

    if (self) {
        Py_INCREF(self);
        PyTuple_SetItem(targetArgTuple, write_slot++, self);
        matcher.requiredTypeForArg(nullptr);
    }

    for (long k = 0; k < PyTuple_Size(args); k++) {
        PyObject* elt = PyTuple_GetItem(args, k);

        //what type would we need for this unnamed arg?
        Type* targetType = matcher.requiredTypeForArg(nullptr);

        if (!matcher.stillMatches()) {
            Py_DECREF(targetArgTuple);
            return std::make_pair(false, nullptr);
        }

        if (!targetType) {
            Py_INCREF(elt);
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
                Py_DECREF(targetArgTuple);
                return std::make_pair(false, nullptr);
            }
        }
    }

    PyObject* newKwargs = nullptr;

    if (kwargs) {
        newKwargs = PyDict_New();

        PyObject *key, *value;
        Py_ssize_t pos = 0;

        while (PyDict_Next(kwargs, &pos, &key, &value)) {
            if (!PyUnicode_Check(key)) {
                Py_DECREF(targetArgTuple);
                Py_DECREF(newKwargs);
                PyErr_SetString(PyExc_TypeError, "Keywords arguments must be strings.");
                return std::make_pair(false, nullptr);
            }

            //what type would we need for this unnamed arg?
            Type* targetType = matcher.requiredTypeForArg(PyUnicode_AsUTF8(key));

            if (!matcher.stillMatches()) {
                Py_DECREF(targetArgTuple);
                Py_DECREF(newKwargs);
                return std::make_pair(false, nullptr);
            }

            if (!targetType) {
                PyDict_SetItem(newKwargs, key, value);
            }
            else {
                try {
                    PyObject* convertedValue = PyInstance::initializePythonRepresentation(targetType, [&](instance_ptr data) {
                        copyConstructFromPythonInstance(targetType, data, value);
                    });

                    PyDict_SetItem(newKwargs, key, convertedValue);
                    Py_DECREF(convertedValue);
                } catch(...) {
                    //not a valid conversion
                    Py_DECREF(targetArgTuple);
                    Py_DECREF(newKwargs);
                    return std::make_pair(false, nullptr);
                }
            }
        }
    }

    if (!matcher.definitelyMatches()) {
        Py_DECREF(targetArgTuple);
        return std::make_pair(false, nullptr);
    }

    PyObject* result;

    bool hadNativeDispatch = false;

    if (!native_dispatch_disabled) {
        auto tried_and_result = dispatchFunctionCallToNative(f, targetArgTuple, newKwargs);
        hadNativeDispatch = tried_and_result.first;
        result = tried_and_result.second;
    }

    if (!hadNativeDispatch) {
        result = PyObject_Call((PyObject*)f.getFunctionObj(), targetArgTuple, newKwargs);
    }

    Py_DECREF(targetArgTuple);
    if (newKwargs) {
        Py_DECREF(newKwargs);
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
            Py_DECREF(result);
            return std::make_pair(true, newRes);
        } catch (std::exception& e) {
            Py_DECREF(result);
            PyErr_SetString(PyExc_TypeError, e.what());
            return std::make_pair(true, (PyObject*)nullptr);
        }
    }

    return std::make_pair(true, result);
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
            instances.push_back(
                Instance::createAndInitialize(argType, [&](instance_ptr p) {
                    copyConstructFromPythonInstance(argType, p, PyTuple_GetItem(argTuple, k));
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

    PyObject* overloadTuple = PyTuple_New(f->getOverloads().size());

    for (long k = 0; k < f->getOverloads().size(); k++) {
        auto& overload = f->getOverloads()[k];

        PyObject* pyIndex = PyLong_FromLong(k);

        PyObject* pyOverloadInst = PyObject_CallFunctionObjArgs(
            funcOverload,
            typePtrToPyTypeRepresentation(f),
            pyIndex,
            (PyObject*)overload.getFunctionObj(),
            overload.getReturnType() ? (PyObject*)typePtrToPyTypeRepresentation(overload.getReturnType()) : Py_None,
            NULL
            );

        Py_DECREF(pyIndex);

        if (pyOverloadInst) {
            for (auto arg: f->getOverloads()[k].getArgs()) {
                PyObject* res = PyObject_CallMethod(pyOverloadInst, "addArg", "sOOOO",
                    arg.getName().c_str(),
                    arg.getDefaultValue() ? PyTuple_Pack(1, arg.getDefaultValue()) : Py_None,
                    arg.getTypeFilter() ? (PyObject*)typePtrToPyTypeRepresentation(arg.getTypeFilter()) : Py_None,
                    arg.getIsStarArg() ? Py_True : Py_False,
                    arg.getIsKwarg() ? Py_True : Py_False
                    );

                if (!res) {
                    PyErr_PrintEx(0);
                } else {
                    Py_DECREF(res);
                }
            }

            PyTuple_SetItem(overloadTuple, k, pyOverloadInst);
        } else {
            PyErr_PrintEx(0);
            Py_INCREF(Py_None);
            PyTuple_SetItem(overloadTuple, k, Py_None);
        }
    }

    return overloadTuple;
}
