#include "AllTypes.hpp"

bool TupleOrListOfType::isBinaryCompatibleWithConcrete(Type* other) {
    if (other->getTypeCategory() != m_typeCategory) {
        return false;
    }

    TupleOfType* otherO = (TupleOfType*)other;

    return m_element_type->isBinaryCompatibleWith(otherO->m_element_type);
}

void TupleOrListOfType::repr(instance_ptr self, ReprAccumulator& stream) {
    PushReprState isNew(stream, self);

    if (!isNew) {
        if (m_is_tuple) {
            stream << m_name << "(" << (void*)self << ")";
        } else {
            stream << m_name << "[" << (void*)self << "]";
        }

        return;
    }

    stream << (m_is_tuple ? "(" : "[");

    int32_t ct = count(self);

    for (long k = 0; k < ct;k++) {
        if (k > 0) {
            stream << ", ";
        }

        m_element_type->repr(eltPtr(self,k),stream);
    }

    stream << (m_is_tuple ? ")" : "]");
}

int32_t TupleOrListOfType::hash32(instance_ptr left) {
    if (!(*(layout**)left)) {
        return 0x123;
    }

    if ((*(layout**)left)->hash_cache == -1) {
        Hash32Accumulator acc((int)getTypeCategory());

        int32_t ct = count(left);
        acc.add(ct);

        for (long k = 0; k < ct;k++) {
            acc.add(m_element_type->hash32(eltPtr(left, k)));
        }

        (*(layout**)left)->hash_cache = acc.get();
        if ((*(layout**)left)->hash_cache == -1) {
            (*(layout**)left)->hash_cache = -2;
        }
    }

    return (*(layout**)left)->hash_cache;
}

bool TupleOrListOfType::cmp(instance_ptr left, instance_ptr right, int pyComparisonOp) {
    if (!(*(layout**)left) && (*(layout**)right)) {
        return cmpResultToBoolForPyOrdering(pyComparisonOp, -1);
    }
    if (!(*(layout**)right) && (*(layout**)left)) {
        return cmpResultToBoolForPyOrdering(pyComparisonOp, 1);
    }
    if (!(*(layout**)right) && !(*(layout**)left)) {
        return cmpResultToBoolForPyOrdering(pyComparisonOp, 0);
    }

    layout& left_layout = **(layout**)left;
    layout& right_layout = **(layout**)right;

    if (&left_layout == &right_layout) {
        return cmpResultToBoolForPyOrdering(pyComparisonOp, 0);
    }

    if (pyComparisonOp == Py_NE) {
        return !cmp(left, right, Py_EQ);
    }

    size_t bytesPer = m_element_type->bytecount();

    if (pyComparisonOp == Py_EQ) {
        if (left_layout.count != right_layout.count) {
            return false;
        }

        for (long k = 0; k < left_layout.count && k < right_layout.count; k++) {
            if (m_element_type->cmp(left_layout.data + bytesPer * k,
                                           right_layout.data + bytesPer * k, Py_NE)) {
                return false;
            }
        }

        return true;
    }

    for (long k = 0; k < left_layout.count && k < right_layout.count; k++) {
        if (m_element_type->cmp(left_layout.data + bytesPer * k,
                                       right_layout.data + bytesPer * k, Py_NE)) {
            return cmpResultToBoolForPyOrdering(pyComparisonOp,
                m_element_type->cmp(left_layout.data + bytesPer * k,
                                       right_layout.data + bytesPer * k, Py_LT)
                    ? -1 : 1
                );
        }
    }

    if (left_layout.count < right_layout.count) {
        return cmpResultToBoolForPyOrdering(pyComparisonOp,-1);
    }

    if (left_layout.count > right_layout.count) {
        return cmpResultToBoolForPyOrdering(pyComparisonOp,1);
    }

    return cmpResultToBoolForPyOrdering(pyComparisonOp,0);
}

// static
TupleOfType* TupleOfType::Make(Type* elt) {
    static std::mutex guard;

    std::lock_guard<std::mutex> lock(guard);

    static std::map<Type*, TupleOfType*> m;

    auto it = m.find(elt);
    if (it == m.end()) {
        it = m.insert(std::make_pair(elt, new TupleOfType(elt))).first;
    }

    return it->second;
}

// static
ListOfType* ListOfType::Make(Type* elt) {
    static std::mutex guard;

    std::lock_guard<std::mutex> lock(guard);

    static std::map<Type*, ListOfType*> m;

    auto it = m.find(elt);
    if (it == m.end()) {
        it = m.insert(std::make_pair(elt, new ListOfType(elt))).first;
    }

    return it->second;
}

int64_t TupleOrListOfType::count(instance_ptr self) const {
    if (!(*(layout**)self)) {
        return 0;
    }

    return (*(layout**)self)->count;
}

int64_t TupleOrListOfType::refcount(instance_ptr self) const {
    if (!(*(layout**)self)) {
        return 0;
    }

    return (*(layout**)self)->refcount;
}

void TupleOrListOfType::constructor(instance_ptr self) {
    constructor(self, 0, [](instance_ptr i, int64_t k) {});
}

void TupleOrListOfType::destroy(instance_ptr selfPtr) {
    layout_ptr& self = *(layout_ptr*)selfPtr;

    if (!self) {
        return;
    }

    if (self->refcount.fetch_sub(1) == 1) {
        m_element_type->destroy(self->count, [&](int64_t k) {return eltPtr(self,k);});
        free(self->data);
        free(self);
    }
}

void TupleOrListOfType::copy_constructor(instance_ptr self, instance_ptr other) {
    (*(layout**)self) = (*(layout**)other);
    if (*(layout**)self) {
        (*(layout**)self)->refcount++;
    }
}

void TupleOrListOfType::assign(instance_ptr self, instance_ptr other) {
    if (self == other)
        return;

    layout* old = (*(layout**)self);

    (*(layout**)self) = (*(layout**)other);

    if (*(layout**)self) {
        (*(layout**)self)->refcount++;
    }

    destroy((instance_ptr)&old);
}

void TupleOrListOfType::reserve(instance_ptr self, size_t target) {
    layout_ptr& self_layout = *(layout_ptr*)self;

    if (target < self_layout->count) {
        target = self_layout->count;
    }

    self_layout->data = (uint8_t*)realloc(self_layout->data, getEltType()->bytecount() * target);
    self_layout->reserved = target;
}


//static
void ListOfType::copyListObject(instance_ptr target, instance_ptr src) {
    constructor(target, count(src), [&](instance_ptr tgt, long k) {
        return m_element_type->copy_constructor(tgt, eltPtr(src,k));
    });
}

void ListOfType::setSizeUnsafe(instance_ptr self, size_t count) {
    layout_ptr& self_layout = *(layout_ptr*)self;

    self_layout->count = count;
}

void ListOfType::append(instance_ptr self, instance_ptr other) {
    layout_ptr& self_layout = *(layout_ptr*)self;

    if (!self_layout) {
        self_layout = (layout_ptr)malloc(sizeof(layout) + getEltType()->bytecount() * 1);

        self_layout->count = 1;
        self_layout->refcount = 1;
        self_layout->reserved = 1;
        self_layout->hash_cache = -1;

        getEltType()->copy_constructor(eltPtr(self, 0), other);
    } else {
        if (self_layout->count == self_layout->reserved) {
            int64_t new_reserved = self_layout->reserved * 1.25 + 1;
            self_layout->data = (uint8_t*)realloc(self_layout->data, getEltType()->bytecount() * new_reserved);
            self_layout->reserved = new_reserved;
        }

        getEltType()->copy_constructor(eltPtr(self, self_layout->count), other);
        self_layout->count++;
    }
}

size_t ListOfType::reserved(instance_ptr self) {
    layout_ptr& self_layout = *(layout_ptr*)self;

    return self_layout->reserved;
}
void ListOfType::remove(instance_ptr self, size_t index) {
    layout_ptr& self_layout = *(layout_ptr*)self;

    getEltType()->destroy(eltPtr(self, index));
    memmove(eltPtr(self, index), eltPtr(self, index+1), (self_layout->count - index - 1) * getEltType()->bytecount());
    self_layout->count--;
}

void ListOfType::resize(instance_ptr self, size_t count) {
    layout_ptr& self_layout = *(layout_ptr*)self;

    if (count > self_layout->reserved) {
        reserve(self, count);
    }

    if (count < self_layout->count) {
        getEltType()->destroy(self_layout->count - count, [&](int64_t k) {return eltPtr(self,k + count);});
        self_layout->count = count;
    }
    else if (count > self_layout->count) {
        getEltType()->constructor(count - self_layout->count, [&](int64_t k) {return eltPtr(self,k + self_layout->count);});
        self_layout->count = count;
    }
}

void ListOfType::resize(instance_ptr self, size_t count, instance_ptr value) {
    layout_ptr& self_layout = *(layout_ptr*)self;

    if (count > self_layout->reserved) {
        reserve(self, count);
    }

    if (count < self_layout->count) {
        getEltType()->destroy(self_layout->count - count, [&](int64_t k) {return eltPtr(self,k + count);});
        self_layout->count = count;
    }
    else if (count > self_layout->count) {
        getEltType()->copy_constructor(
            count - self_layout->count,
            [&](int64_t k) {return eltPtr(self,k + self_layout->count);},
            [&](int64_t k) {return value;}
            );
        self_layout->count = count;
    }
}


