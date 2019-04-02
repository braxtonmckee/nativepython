#include "AllTypes.hpp"

void ConstDict::_forwardTypesMayHaveChanged() {
    m_name = "ConstDict(" + m_key->name() + "->" + m_value->name() + ")";
    m_size = sizeof(void*);
    m_is_default_constructible = true;
    m_bytes_per_key = m_key->bytecount();
    m_bytes_per_key_value_pair = m_key->bytecount() + m_value->bytecount();
    m_bytes_per_key_subtree_pair = m_key->bytecount() + this->bytecount();
    m_key_value_pair_type = Tuple::Make({m_key, m_value});
}

bool ConstDict::isBinaryCompatibleWithConcrete(Type* other) {
    if (other->getTypeCategory() != m_typeCategory) {
        return false;
    }

    ConstDict* otherO = (ConstDict*)other;

    return m_key->isBinaryCompatibleWith(otherO->m_key) &&
        m_value->isBinaryCompatibleWith(otherO->m_value);
}

// static
ConstDict* ConstDict::Make(Type* key, Type* value) {
    static std::mutex guard;

    std::lock_guard<std::mutex> lock(guard);

    static std::map<std::pair<Type*, Type*>, ConstDict*> m;

    auto lookup_key = std::make_pair(key,value);

    auto it = m.find(lookup_key);
    if (it == m.end()) {
        it = m.insert(std::make_pair(lookup_key, new ConstDict(key, value))).first;
    }

    return it->second;
}

void ConstDict::repr(instance_ptr self, ReprAccumulator& stream) {
    PushReprState isNew(stream, self);

    if (!isNew) {
        stream << m_name << "(" << (void*)self << ")";
        return;
    }

    stream << "{";

    int32_t ct = count(self);

    for (long k = 0; k < ct;k++) {
        if (k > 0) {
            stream << ", ";
        }

        m_key->repr(kvPairPtrKey(self,k),stream);
        stream << ": ";
        m_value->repr(kvPairPtrValue(self,k),stream);
    }

    stream << "}";
}

int32_t ConstDict::hash32(instance_ptr left) {
    if (size(left) == 0) {
        return 0x123456;
    }

    if ((*(layout**)left)->hash_cache == -1) {
        Hash32Accumulator acc((int)getTypeCategory());

        int32_t count = size(left);
        acc.add(count);
        for (long k = 0; k < count;k++) {
            acc.add(m_key->hash32(kvPairPtrKey(left,k)));
            acc.add(m_value->hash32(kvPairPtrValue(left,k)));
        }

        (*(layout**)left)->hash_cache = acc.get();
        if ((*(layout**)left)->hash_cache == -1) {
            (*(layout**)left)->hash_cache = -2;
        }
    }

    return (*(layout**)left)->hash_cache;
}

//to make this fast(er), we do dict size comparison first, then keys, then values
bool ConstDict::cmp(instance_ptr left, instance_ptr right, int pyComparisonOp) {
    if (pyComparisonOp == Py_NE) {
        return !cmp(left, right, Py_EQ);
    }


    if (size(left) < size(right)) {
        return cmpResultToBoolForPyOrdering(pyComparisonOp, -1);
    }
    if (size(left) > size(right)) {
        return cmpResultToBoolForPyOrdering(pyComparisonOp, 1);
    }

    if (*(layout**)left == *(layout**)right) {
        return cmpResultToBoolForPyOrdering(pyComparisonOp, 0);
    }

    int ct = count(left);

    if (pyComparisonOp == Py_EQ) {
        for (long k = 0; k < ct; k++) {
            if (m_key->cmp(kvPairPtrKey(left,k), kvPairPtrKey(right,k), Py_NE) ||
                    m_value->cmp( kvPairPtrValue(left,k), kvPairPtrValue(right,k), Py_NE)) {
                return false;
            }
        }

        return true;
    } else {
        for (long k = 0; k < ct; k++) {
            if (m_key->cmp(kvPairPtrKey(left,k), kvPairPtrKey(right,k), Py_NE)) {
                if (m_key->cmp(kvPairPtrKey(left,k), kvPairPtrKey(right,k), Py_LT)) {
                    return cmpResultToBoolForPyOrdering(pyComparisonOp, -1);
                }
                return cmpResultToBoolForPyOrdering(pyComparisonOp, 1);
            }
        }

        for (long k = 0; k < ct; k++) {
            if (m_value->cmp( kvPairPtrValue(left,k), kvPairPtrValue(right,k), Py_NE)) {
                if (m_value->cmp( kvPairPtrValue(left,k), kvPairPtrValue(right,k), Py_LT)) {
                    return cmpResultToBoolForPyOrdering(pyComparisonOp, -1);
                }
                return cmpResultToBoolForPyOrdering(pyComparisonOp, 1);
            }
        }

        return cmpResultToBoolForPyOrdering(pyComparisonOp, 0);
    }
}

void ConstDict::addDicts(instance_ptr lhs, instance_ptr rhs, instance_ptr output) const {
    std::vector<instance_ptr> keep;

    int64_t lhsCount = count(lhs);
    int64_t rhsCount = count(rhs);

    for (long k = 0; k < lhsCount; k++) {
        instance_ptr lhsVal = kvPairPtrKey(lhs, k);

        if (!lookupValueByKey(rhs, lhsVal)) {
            keep.push_back(lhsVal);
        }
    }

    constructor(output, rhsCount + keep.size(), false);

    for (long k = 0; k < rhsCount; k++) {
        m_key->copy_constructor(kvPairPtrKey(output,k), kvPairPtrKey(rhs, k));
        m_value->copy_constructor(kvPairPtrValue(output,k), kvPairPtrValue(rhs, k));
    }
    for (long k = 0; k < keep.size(); k++) {
        m_key->copy_constructor(kvPairPtrKey(output,k + rhsCount), keep[k]);
        m_value->copy_constructor(kvPairPtrValue(output,k + rhsCount), keep[k] + m_bytes_per_key);
    }
    incKvPairCount(output, keep.size() + rhsCount);

    sortKvPairs(output);
}

void ConstDict::subtractTupleOfKeysFromDict(instance_ptr lhs, instance_ptr rhs, instance_ptr output) const {
    TupleOfType* tupleType = tupleOfKeysType();

    int64_t lhsCount = count(lhs);
    int64_t rhsCount = tupleType->count(rhs);

    std::set<int> remove;

    for (long k = 0; k < rhsCount; k++) {
        int64_t index = lookupIndexByKey(lhs, tupleType->eltPtr(rhs, k));
        if (index != -1) {
            remove.insert(index);
        }
    }

    constructor(output, lhsCount - remove.size(), false);

    long written = 0;
    for (long k = 0; k < lhsCount; k++) {
        if (remove.find(k) == remove.end()) {
            m_key->copy_constructor(kvPairPtrKey(output,written), kvPairPtrKey(lhs, k));
            m_value->copy_constructor(kvPairPtrValue(output,written), kvPairPtrValue(lhs, k));

            written++;
        }
    }

    incKvPairCount(output, written);
}

instance_ptr ConstDict::kvPairPtrKey(instance_ptr self, int64_t i) const {
    if (!(*(layout**)self)) {
        return self;
    }

    layout& record = **(layout**)self;

    return record.data + m_bytes_per_key_value_pair * i;
}

instance_ptr ConstDict::kvPairPtrValue(instance_ptr self, int64_t i) const {
    if (!(*(layout**)self)) {
        return self;
    }

    layout& record = **(layout**)self;

    return record.data + m_bytes_per_key_value_pair * i + m_bytes_per_key;
}

void ConstDict::incKvPairCount(instance_ptr self, int by) const {
    if (by == 0) {
        return;
    }

    layout& record = **(layout**)self;
    record.count += by;
}

void ConstDict::sortKvPairs(instance_ptr self) const {
    if (!*(layout**)self) {
        return;
    }

    layout& record = **(layout**)self;

    assert(!record.subpointers);

    if (record.count <= 1) {
        return;
    }
    else if (record.count == 2) {
        if (m_key->cmp(kvPairPtrKey(self, 0), kvPairPtrKey(self,1), Py_GT)) {
            m_key->swap(kvPairPtrKey(self,0), kvPairPtrKey(self,1));
            m_value->swap(kvPairPtrValue(self,0), kvPairPtrValue(self,1));
        }
        return;
    } else {
        std::vector<int> indices;
        for (long k=0;k<record.count;k++) {
            indices.push_back(k);
        }

        std::sort(indices.begin(), indices.end(), [&](int l, int r) {
            return m_key->cmp(kvPairPtrKey(self,l),kvPairPtrKey(self,r), Py_LT);
            });

        //create a temporary buffer
        std::vector<uint8_t> d;
        d.resize(m_bytes_per_key_value_pair * record.count);

        //final_lookup contains the location of each value in the original sort
        for (long k = 0; k < indices.size(); k++) {
            m_key->swap(kvPairPtrKey(self, indices[k]), &d[m_bytes_per_key_value_pair*k]);
            m_value->swap(kvPairPtrValue(self, indices[k]), &d[m_bytes_per_key_value_pair*k+m_bytes_per_key]);
        }

        //now move them back
        for (long k = 0; k < indices.size(); k++) {
            m_key->swap(kvPairPtrKey(self, k), &d[m_bytes_per_key_value_pair*k]);
            m_value->swap(kvPairPtrValue(self, k), &d[m_bytes_per_key_value_pair*k+m_bytes_per_key]);
        }
    }
}

instance_ptr ConstDict::keyTreePtr(instance_ptr self, int64_t i) const {
    if (!(*(layout**)self)) {
        return self;
    }

    layout& record = **(layout**)self;

    return record.data + m_bytes_per_key_subtree_pair * i;
}

bool ConstDict::instanceIsSubtrees(instance_ptr self) const {
    if (!(*(layout**)self)) {
        return self;
    }

    layout& record = **(layout**)self;

    return record.subpointers != 0;
}

int64_t ConstDict::refcount(instance_ptr self) const {
    if (!(*(layout**)self)) {
        return 0;
    }

    layout& record = **(layout**)self;

    return record.refcount;
}

int64_t ConstDict::count(instance_ptr self) const {
    if (!(*(layout**)self)) {
        return 0;
    }

    layout& record = **(layout**)self;

    if (record.subpointers) {
        return record.subpointers;
    }

    return record.count;
}

int64_t ConstDict::size(instance_ptr self) const {
    if (!(*(layout**)self)) {
        return 0;
    }

    return (*(layout**)self)->count;
}

int64_t ConstDict::lookupIndexByKey(instance_ptr self, instance_ptr key) const {
    if (!(*(layout**)self)) {
        return -1;
    }

    layout& record = **(layout**)self;

    assert(record.subpointers == 0); //this is not implemented yet

    long low = 0;
    long high = record.count;

    while (low < high) {
        long mid = (low+high)/2;

        if (m_key->cmp(kvPairPtrKey(self, mid), key, Py_EQ)) {
            return mid;
        } else if (m_key->cmp(kvPairPtrKey(self, mid), key, Py_LT)) {
            low = mid+1;
        } else {
            high = mid;
        }
    }

    return -1;
}

instance_ptr ConstDict::lookupValueByKey(instance_ptr self, instance_ptr key) const {
    int64_t offset = lookupIndexByKey(self, key);
    if (offset == -1) {
        return 0;
    }
    return kvPairPtrValue(self, offset);
}

void ConstDict::constructor(instance_ptr self, int64_t space, bool isPointerTree) const {
    if (space == 0) {
        (*(layout**)self) = nullptr;
        return;
    }

    int bytesPer = isPointerTree ? m_bytes_per_key_subtree_pair : m_bytes_per_key_value_pair;

    (*(layout**)self) = (layout*)malloc(sizeof(layout) + bytesPer * space);

    layout& record = **(layout**)self;

    record.count = 0;
    record.subpointers = 0;
    record.refcount = 1;
    record.hash_cache = -1;
}

void ConstDict::constructor(instance_ptr self) {
    (*(layout**)self) = nullptr;
}

void ConstDict::destroy(instance_ptr self) {
    if (!(*(layout**)self)) {
        return;
    }

    layout& record = **(layout**)self;

    if (record.refcount.fetch_sub(1) == 1) {
        if (record.subpointers == 0) {
            m_key->destroy(record.count, [&](long ix) {
                return record.data + m_bytes_per_key_value_pair * ix;
            });
            m_value->destroy(record.count, [&](long ix) {
                return record.data + m_bytes_per_key_value_pair * ix + m_bytes_per_key;
            });
        } else {
            m_key->destroy(record.subpointers, [&](long ix) {
                return record.data + m_bytes_per_key_subtree_pair * ix;
            });
            ((Type*)this)->destroy(record.subpointers, [&](long ix) {
                return record.data + m_bytes_per_key_subtree_pair * ix + m_bytes_per_key;
            });
        }

        free((*(layout**)self));
    }
}

void ConstDict::copy_constructor(instance_ptr self, instance_ptr other) {
    (*(layout**)self) = (*(layout**)other);
    if (*(layout**)self) {
        (*(layout**)self)->refcount++;
    }
}

void ConstDict::assign(instance_ptr self, instance_ptr other) {
    layout* old = (*(layout**)self);

    (*(layout**)self) = (*(layout**)other);

    if (*(layout**)self) {
        (*(layout**)self)->refcount++;
    }

    destroy((instance_ptr)&old);
}

