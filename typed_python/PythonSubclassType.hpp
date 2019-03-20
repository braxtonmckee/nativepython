#pragma once

#include "Type.hpp"

class PythonSubclass : public Type {
public:
    PythonSubclass(Type* base, PyTypeObject* typePtr) :
            Type(TypeCategory::catPythonSubclass)
    {
        m_base = base;
        mTypeRep = typePtr;
        m_name = typePtr->tp_name;
        m_is_simple = false;

        forwardTypesMayHaveChanged();
    }

    bool isBinaryCompatibleWithConcrete(Type* other);

    template<class visitor_type>
    void _visitContainedTypes(const visitor_type& visitor) {
        visitor(m_base);
    }

    template<class visitor_type>
    void _visitReferencedTypes(const visitor_type& visitor) {
        visitor(m_base);
    }

    void _forwardTypesMayHaveChanged() {
        m_size = m_base->bytecount();
        m_is_default_constructible = m_base->is_default_constructible();
    }

    int32_t hash32(instance_ptr left) {
        return m_base->hash32(left);
    }

    template<class buf_t>
    void serialize(instance_ptr self, buf_t& buffer) {
        m_base->serialize(self,buffer);
    }

    template<class buf_t>
    void deserialize(instance_ptr self, buf_t& buffer) {
        m_base->deserialize(self,buffer);
    }

    void repr(instance_ptr self, ReprAccumulator& stream) {
        m_base->repr(self,stream);
    }

    bool cmp(instance_ptr left, instance_ptr right, int pyComparisonOp) {
        return m_base->cmp(left,right,pyComparisonOp);
    }

    void constructor(instance_ptr self) {
        m_base->constructor(self);
    }

    void destroy(instance_ptr self) {
        m_base->destroy(self);
    }

    void copy_constructor(instance_ptr self, instance_ptr other) {
        m_base->copy_constructor(self, other);
    }

    void assign(instance_ptr self, instance_ptr other) {
        m_base->assign(self, other);
    }

    static PythonSubclass* Make(Type* base, PyTypeObject* pyType);

    Type* baseType() const {
        return m_base;
    }

    PyTypeObject* pyType() const {
        return mTypeRep;
    }
};
