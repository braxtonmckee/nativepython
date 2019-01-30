#pragma once

#include "Type.hpp"

//wraps an actual python instance. Note that we assume we're holding the GIL whenever
//we interact with actual python objects. Compiled code needs to treat these objects
//with extreme care...
class PythonObjectOfType : public Type {
public:
    PythonObjectOfType(PyTypeObject* typePtr) :
            Type(TypeCategory::catPythonObjectOfType)
    {
        mPyTypePtr = typePtr;
        m_name = typePtr->tp_name;

        forwardTypesMayHaveChanged();
    }

    bool isBinaryCompatibleWithConcrete(Type* other) {
        return other == this;
    }

    template<class visitor_type>
    void _visitContainedTypes(const visitor_type& visitor) {
    }

    template<class visitor_type>
    void _visitReferencedTypes(const visitor_type& visitor) {
    }

    void _forwardTypesMayHaveChanged() {
        m_size = sizeof(PyObject*);

        int isinst = PyObject_IsInstance(Py_None, (PyObject*)mPyTypePtr);
        if (isinst == -1) {
            isinst = 0;
            PyErr_Clear();
        }

        m_is_default_constructible = isinst != 0;
    }

    int32_t hash32(instance_ptr left) {
        PyObject* p = *(PyObject**)left;

        return PyObject_Hash(p);
    }

    template<class buf_t>
    void serialize(instance_ptr self, buf_t& buffer) {
        PyObject* p = *(PyObject**)self;
        buffer.getContext().serializePythonObject(p, buffer);
    }

    template<class buf_t>
    void deserialize(instance_ptr self, buf_t& buffer) {
         *(PyObject**)self = buffer.getContext().deserializePythonObject(buffer);
    }

    void repr(instance_ptr self, ReprAccumulator& stream);

    char cmp(instance_ptr left, instance_ptr right);

    void constructor(instance_ptr self) {
        *(PyObject**)self = Py_None;
        Py_INCREF(Py_None);
    }

    void destroy(instance_ptr self) {
        Py_DECREF(*(PyObject**)self);
    }

    void copy_constructor(instance_ptr self, instance_ptr other) {
        Py_INCREF(*(PyObject**)other);
        *(PyObject**)self = *(PyObject**)other;
    }

    void assign(instance_ptr self, instance_ptr other) {
        Py_INCREF(*(PyObject**)other);
        Py_DECREF(*(PyObject**)self);
        *(PyObject**)self = *(PyObject**)other;
    }

    static PythonObjectOfType* Make(PyTypeObject* pyType);

    PyTypeObject* pyType() const {
        return mPyTypePtr;
    }

private:
    PyTypeObject* mPyTypePtr;
};
