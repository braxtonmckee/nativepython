#include "AllTypes.hpp"

bool OneOf::isBinaryCompatibleWithConcrete(Type* other) {
    if (other->getTypeCategory() != TypeCategory::catOneOf) {
        return false;
    }

    OneOf* otherO = (OneOf*)other;

    if (m_types.size() != otherO->m_types.size()) {
        return false;
    }

    for (long k = 0; k < m_types.size(); k++) {
        if (!m_types[k]->isBinaryCompatibleWith(otherO->m_types[k])) {
            return false;
        }
    }

    return true;
}

void OneOf::_forwardTypesMayHaveChanged() {
    m_size = computeBytecount();
    m_name = computeName();

    m_is_default_constructible = false;

    for (auto typePtr: m_types) {
        if (typePtr->is_default_constructible()) {
            m_is_default_constructible = true;
            break;
        }
    }
}

std::string OneOf::computeName() const {
    std::string res = "OneOf(";
    bool first = true;
    for (auto t: m_types) {
        if (first) {
            first = false;
        } else {
            res += ", ";
        }

        res += t->name();
    }

    res += ")";

    return res;
}

void OneOf::repr(instance_ptr self, ReprAccumulator& stream) {
    m_types[*((uint8_t*)self)]->repr(self+1, stream);
}

int32_t OneOf::hash32(instance_ptr left) {
    Hash32Accumulator acc((int)getTypeCategory());

    acc.add(*(uint8_t*)left);
    acc.add(m_types[*((uint8_t*)left)]->hash32(left+1));

    return acc.get();
}

char OneOf::cmp(instance_ptr left, instance_ptr right) {
    if (((uint8_t*)left)[0] < ((uint8_t*)right)[0]) {
        return -1;
    }
    if (((uint8_t*)left)[0] > ((uint8_t*)right)[0]) {
        return 1;
    }

    return m_types[*((uint8_t*)left)]->cmp(left+1,right+1);
}

size_t OneOf::computeBytecount() const {
    size_t res = 0;

    for (auto t: m_types)
        res = std::max(res, t->bytecount());

    return res + 1;
}

void OneOf::constructor(instance_ptr self) {
    if (!m_is_default_constructible) {
        throw std::runtime_error(m_name + " is not default-constructible");
    }

    for (size_t k = 0; k < m_types.size(); k++) {
        if (m_types[k]->is_default_constructible()) {
            *(uint8_t*)self = k;
            m_types[k]->constructor(self+1);
            return;
        }
    }
}

void OneOf::destroy(instance_ptr self) {
    uint8_t which = *(uint8_t*)(self);
    m_types[which]->destroy(self+1);
}

void OneOf::copy_constructor(instance_ptr self, instance_ptr other) {
    uint8_t which = *(uint8_t*)self = *(uint8_t*)other;
    m_types[which]->copy_constructor(self+1, other+1);
}

void OneOf::assign(instance_ptr self, instance_ptr other) {
    uint8_t which = *(uint8_t*)self;
    if (which == *(uint8_t*)other) {
        m_types[which]->assign(self+1,other+1);
    } else {
        m_types[which]->destroy(self+1);

        uint8_t otherWhich = *(uint8_t*)other;
        *(uint8_t*)self = otherWhich;
        m_types[otherWhich]->copy_constructor(self+1,other+1);
    }
}

// static
OneOf* OneOf::Make(const std::vector<Type*>& types) {
    std::vector<Type*> flat_typelist;
    std::set<Type*> seen;

    //make sure we only get each type once and don't have any other 'OneOf' in there...
    std::function<void (const std::vector<Type*>)> visit = [&](const std::vector<Type*>& subvec) {
        for (auto t: subvec) {
            if (t->getTypeCategory() == catOneOf) {
                visit( ((OneOf*)t)->getTypes() );
            } else if (seen.find(t) == seen.end()) {
                flat_typelist.push_back(t);
                seen.insert(t);
            }
        }
    };

    visit(types);

    static std::mutex guard;

    std::lock_guard<std::mutex> lock(guard);

    typedef const std::vector<Type*> keytype;

    static std::map<keytype, OneOf*> m;

    auto it = m.find(flat_typelist);
    if (it == m.end()) {
        it = m.insert(std::make_pair(flat_typelist, new OneOf(flat_typelist))).first;
    }

    return it->second;
}

