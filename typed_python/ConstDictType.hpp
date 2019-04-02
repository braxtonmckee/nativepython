#pragma once

#include "Type.hpp"
#include "ReprAccumulator.hpp"

class ConstDict : public Type {
    class layout {
    public:
        std::atomic<int64_t> refcount;
        int32_t hash_cache;
        int32_t count;
        int32_t subpointers; //if 0, then all values are inline as pairs of (key,value)
                             //otherwise, its an array of '(key, ConstDict(key,value))'
        uint8_t data[];
    };

public:
    ConstDict(Type* key, Type* value) :
            Type(TypeCategory::catConstDict),
            m_key(key),
            m_value(value)
    {
        forwardTypesMayHaveChanged();
    }

    template<class visitor_type>
    void _visitContainedTypes(const visitor_type& visitor) {
    }

    template<class visitor_type>
    void _visitReferencedTypes(const visitor_type& visitor) {
        visitor(m_key);
        visitor(m_value);
    }

    void _forwardTypesMayHaveChanged();

    bool isBinaryCompatibleWithConcrete(Type* other);

    static ConstDict* Make(Type* key, Type* value);

    template<class buf_t>
    void serialize(instance_ptr self, buf_t& buffer) {
        int32_t ct = count(self);
        buffer.write_uint32(ct);
        for (long k = 0; k < ct;k++) {
            m_key->serialize(kvPairPtrKey(self,k),buffer);
            m_value->serialize(kvPairPtrValue(self,k),buffer);
        }
    }

    template<class buf_t>
    void deserialize(instance_ptr self, buf_t& buffer) {
        int32_t ct = buffer.read_uint32();

        if (!buffer.canConsume(ct) && m_bytes_per_key_value_pair) {
            throw std::runtime_error("Corrupt data (dictcount)");
        }

        constructor(self, ct, false);

        for (long k = 0; k < ct;k++) {
            m_key->deserialize(kvPairPtrKey(self,k),buffer);
            m_value->deserialize(kvPairPtrValue(self,k),buffer);
        }

        incKvPairCount(self, ct);
    }

    void repr(instance_ptr self, ReprAccumulator& stream);

    int32_t hash32(instance_ptr left);

    bool cmp(instance_ptr left, instance_ptr right, int pyComparisonOp);

    void addDicts(instance_ptr lhs, instance_ptr rhs, instance_ptr output) const;

    TupleOfType* tupleOfKeysType() const {
        return TupleOfType::Make(m_key);
    }

    void subtractTupleOfKeysFromDict(instance_ptr lhs, instance_ptr rhs, instance_ptr output) const;

    instance_ptr kvPairPtrKey(instance_ptr self, int64_t i) const;

    instance_ptr kvPairPtrValue(instance_ptr self, int64_t i) const;

    void incKvPairCount(instance_ptr self, int by = 1) const;

    void sortKvPairs(instance_ptr self) const;

    instance_ptr keyTreePtr(instance_ptr self, int64_t i) const;

    bool instanceIsSubtrees(instance_ptr self) const;

    int64_t refcount(instance_ptr self) const;

    int64_t count(instance_ptr self) const;


    int64_t size(instance_ptr self) const;

    int64_t lookupIndexByKey(instance_ptr self, instance_ptr key) const;

    instance_ptr lookupValueByKey(instance_ptr self, instance_ptr key) const;

    void constructor(instance_ptr self, int64_t space, bool isPointerTree) const;

    void constructor(instance_ptr self);

    void destroy(instance_ptr self);

    void copy_constructor(instance_ptr self, instance_ptr other);

    void assign(instance_ptr self, instance_ptr other);


    Type* keyValuePairType() const { return m_key_value_pair_type; }
    Type* keyType() const { return m_key; }
    Type* valueType() const { return m_value; }

private:
    Type* m_key;
    Type* m_value;
    Type* m_key_value_pair_type;
    size_t m_bytes_per_key;
    size_t m_bytes_per_key_value_pair;
    size_t m_bytes_per_key_subtree_pair;
};

