#include "AllTypes.hpp"

bool Alternative::isBinaryCompatibleWithConcrete(Type* other) {
    if (other->getTypeCategory() == TypeCategory::catConcreteAlternative) {
        other = other->getBaseType();
    }

    if (other->getTypeCategory() != m_typeCategory) {
        return false;
    }

    Alternative* otherO = (Alternative*)other;

    if (m_subtypes.size() != otherO->m_subtypes.size()) {
        return false;
    }

    for (long k = 0; k < m_subtypes.size(); k++) {
        if (m_subtypes[k].first != otherO->m_subtypes[k].first) {
            return false;
        }
        if (!m_subtypes[k].second->isBinaryCompatibleWith(otherO->m_subtypes[k].second)) {
            return false;
        }
    }

    return true;
}

int64_t Alternative::refcount(instance_ptr i) const {
    if (m_all_alternatives_empty) {
        return 0;
    }
    return ((layout**)i)[0]->refcount;
}

void Alternative::_forwardTypesMayHaveChanged() {
    m_size = sizeof(void*);

    m_is_default_constructible = false;
    m_all_alternatives_empty = true;
    m_arg_positions.clear();
    m_default_construction_ix = 0;
    m_default_construction_type = nullptr;

    for (auto& subtype_pair: m_subtypes) {
        if (subtype_pair.second->bytecount() > 0) {
            m_all_alternatives_empty = false;
        }

        if (m_arg_positions.find(subtype_pair.first) != m_arg_positions.end()) {
            throw std::runtime_error("Can't create an alternative with " +
                    subtype_pair.first + " defined twice.");
        }

        m_arg_positions[subtype_pair.first] = m_arg_positions.size();

        if (subtype_pair.second->is_default_constructible() && !m_is_default_constructible) {
            m_is_default_constructible = true;
            m_default_construction_ix = m_arg_positions[subtype_pair.first];
        }
    }

    m_size = (m_all_alternatives_empty ? 1 : sizeof(void*));
}

bool Alternative::cmp(instance_ptr left, instance_ptr right, int pyComparisonOp) {
    if (m_all_alternatives_empty) {
        if (*(uint8_t*)left < *(uint8_t*)right) {
            return cmpResultToBoolForPyOrdering(pyComparisonOp, -1);
        }
        if (*(uint8_t*)left > *(uint8_t*)right) {
            return cmpResultToBoolForPyOrdering(pyComparisonOp, 1);
        }
        return cmpResultToBoolForPyOrdering(pyComparisonOp, 0);
    }

    layout& record_l = **(layout**)left;
    layout& record_r = **(layout**)right;

    if ( &record_l == &record_r ) {
        return cmpResultToBoolForPyOrdering(pyComparisonOp, 0);
    }

    if (record_l.which < record_r.which) {
        return cmpResultToBoolForPyOrdering(pyComparisonOp, -1);
    }
    if (record_l.which > record_r.which) {
        return cmpResultToBoolForPyOrdering(pyComparisonOp, 1);
    }

    return m_subtypes[record_l.which].second->cmp(record_l.data, record_r.data, pyComparisonOp);
}

void Alternative::repr(instance_ptr self, ReprAccumulator& stream) {
    PushReprState isNew(stream, self);

    if (!isNew) {
        stream << m_name << "(" << (void*)self << ")";
        return;
    }

    stream << m_subtypes[which(self)].first;
    m_subtypes[which(self)].second->repr(eltPtr(self), stream);
}

int32_t Alternative::hash32(instance_ptr left) {
    Hash32Accumulator acc((int)TypeCategory::catAlternative);

    acc.add(which(left));
    acc.add(m_subtypes[which(left)].second->hash32(eltPtr(left)));

    return acc.get();
}

instance_ptr Alternative::eltPtr(instance_ptr self) const {
    if (m_all_alternatives_empty) {
        return self;
    }

    layout& record = **(layout**)self;

    return record.data;
}

int64_t Alternative::which(instance_ptr self) const {
    if (m_all_alternatives_empty) {
        return *(uint8_t*)self;
    }

    layout& record = **(layout**)self;

    return record.which;
}

void Alternative::destroy(instance_ptr self) {
    if (m_all_alternatives_empty) {
        return;
    }

    layout& record = **(layout**)self;

    if (record.refcount.fetch_sub(1) == 1) {
        m_subtypes[record.which].second->destroy(record.data);
        free(*(layout**)self);
    }
}

void Alternative::copy_constructor(instance_ptr self, instance_ptr other) {
    if (m_all_alternatives_empty) {
        *(uint8_t*)self = *(uint8_t*)other;
        return;
    }

    (*(layout**)self) = (*(layout**)other);

    if (*(layout**)self) {
        (*(layout**)self)->refcount++;
    }
}

void Alternative::assign(instance_ptr self, instance_ptr other) {
    layout* old = (*(layout**)self);

    (*(layout**)self) = (*(layout**)other);

    if (*(layout**)self) {
        (*(layout**)self)->refcount++;
    }

    destroy((instance_ptr)&old);
}

// static
Alternative* Alternative::Make(std::string name,
                               const std::vector<std::pair<std::string, NamedTuple*> >& types,
                               const std::map<std::string, Function*>& methods //methods preclude us from being in the memo
                              ) {
    return new Alternative(name, types, methods);
}

Type* Alternative::pickConcreteSubclassConcrete(instance_ptr data) {
    if (!m_subtypes_concrete.size()) {
        for (long k = 0; k < m_subtypes.size(); k++) {
            m_subtypes_concrete.push_back(ConcreteAlternative::Make(this, k));
        }
    }

    uint8_t i = which(data);

    return m_subtypes_concrete[i];
}

void Alternative::constructor(instance_ptr self) {
    if (!m_default_construction_type) {
        m_default_construction_type = ConcreteAlternative::Make(this, m_default_construction_ix);
    }

    m_default_construction_type->constructor(self);
}

