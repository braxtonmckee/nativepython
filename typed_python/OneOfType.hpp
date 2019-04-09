#pragma once

#include "Type.hpp"

class OneOfType : public Type {
public:
    OneOfType(const std::vector<Type*>& types) :
                    Type(TypeCategory::catOneOf),
                    m_types(types)
    {
        if (m_types.size() > 255) {
            throw std::runtime_error("OneOf types are limited to 255 alternatives in this implementation");
        }

        forwardTypesMayHaveChanged();
    }

    bool isBinaryCompatibleWithConcrete(Type* other);

    template<class visitor_type>
    void _visitContainedTypes(const visitor_type& visitor) {
        for (auto& typePtr: m_types) {
            visitor(typePtr);
        }
    }

    template<class visitor_type>
    void _visitReferencedTypes(const visitor_type& visitor) {
        _visitContainedTypes(visitor);
    }

    void _forwardTypesMayHaveChanged();

    std::string computeName() const;

    template<class buf_t>
    void deserialize(instance_ptr self, buf_t& buffer) {
        uint8_t which = buffer.read_uint8();
        if (which >= m_types.size()) {
            throw std::runtime_error("Corrupt OneOf");
        }
        *(uint8_t*)self = which;
        m_types[which]->deserialize(self+1, buffer);
    }

    template<class buf_t>
    void serialize(instance_ptr self, buf_t& buffer) {
        buffer.write_uint8(*(uint8_t*)self);
        m_types[*((uint8_t*)self)]->serialize(self+1, buffer);
    }

    void repr(instance_ptr self, ReprAccumulator& stream);

    int32_t hash32(instance_ptr left);

    bool cmp(instance_ptr left, instance_ptr right, int pyComparisonOp);

    std::pair<Type*, instance_ptr> unwrap(instance_ptr self) {
        return std::make_pair(m_types[*(uint8_t*)self], self+1);
    }

    size_t computeBytecount() const;

    void constructor(instance_ptr self);

    void destroy(instance_ptr self);

    void copy_constructor(instance_ptr self, instance_ptr other);

    void assign(instance_ptr self, instance_ptr other);

    const std::vector<Type*>& getTypes() const {
        return m_types;
    }

    static OneOfType* Make(const std::vector<Type*>& types);

private:
    std::vector<Type*> m_types;
};

