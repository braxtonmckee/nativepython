#pragma once

#include "ReprAccumulator.hpp"
#include "AlternativeType.hpp"

class ConcreteAlternative : public Type {
public:
    typedef Alternative::layout layout;

    ConcreteAlternative(Alternative* m_alternative, int64_t which) :
            Type(TypeCategory::catConcreteAlternative),
            m_alternative(m_alternative),
            m_which(which)
    {
        m_is_simple = false;
        forwardTypesMayHaveChanged();
    }

    bool isBinaryCompatibleWithConcrete(Type* other);

    template<class visitor_type>
    void _visitContainedTypes(const visitor_type& visitor) {
        Type* t = m_alternative;
        visitor(t);
        assert(t == m_alternative);
    }

    template<class visitor_type>
    void _visitReferencedTypes(const visitor_type& visitor) {
        Type* t = m_alternative;
        visitor(t);
        assert(t == m_alternative);
    }

    void _forwardTypesMayHaveChanged();

    int32_t hash32(instance_ptr left) {
        return m_alternative->hash32(left);
    }

    void repr(instance_ptr self, ReprAccumulator& stream) {
        m_alternative->repr(self,stream);
    }

    template<class buf_t>
    void deserialize(instance_ptr self, buf_t& buffer) {
        m_alternative->deserialize(self,buffer);
    }

    template<class buf_t>
    void serialize(instance_ptr self, buf_t& buffer) {
        m_alternative->serialize(self,buffer);
    }

    bool cmp(instance_ptr left, instance_ptr right, int pyComparisonOp) {
        return m_alternative->cmp(left,right, pyComparisonOp);
    }

    void constructor(instance_ptr self);

    int64_t refcount(instance_ptr self) const {
        return m_alternative->refcount(self);
    }

    //returns an uninitialized object of type-index 'which'
    template<class subconstructor>
    void constructor(instance_ptr self, const subconstructor& s) const {
        if (m_alternative->all_alternatives_empty()) {
            *(uint8_t*)self = m_which;
            s(self);
        } else {
            *(layout**)self = (layout*)malloc(
                sizeof(layout) +
                elementType()->bytecount()
                );

            layout& record = **(layout**)self;
            record.refcount = 1;
            record.which = m_which;
            try {
                s(record.data);
            } catch(...) {
                free(*(layout**)self);
                throw;
            }
        }
    }

    void destroy(instance_ptr self) {
        m_alternative->destroy(self);
    }

    void copy_constructor(instance_ptr self, instance_ptr other) {
        m_alternative->copy_constructor(self, other);
    }

    void assign(instance_ptr self, instance_ptr other) {
        m_alternative->assign(self, other);
    }

    instance_ptr eltPtr(instance_ptr self) const {
        return m_alternative->eltPtr(self);
    }

    static ConcreteAlternative* Make(Alternative* alt, int64_t which);

    Type* elementType() const {
        return m_alternative->subtypes()[m_which].second;
    }

    Alternative* getAlternative() const {
        return m_alternative;
    }

    int64_t which() const {
        return m_which;
    }

private:
    Alternative* m_alternative;

    int64_t m_which;
};
