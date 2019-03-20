#pragma once

#include "Type.hpp"
#include "ReprAccumulator.hpp"

//a class held directly inside of another object
class HeldClass : public Type {
public:
    HeldClass(std::string inName,
          const std::vector<std::tuple<std::string, Type*, Instance> >& members,
          const std::map<std::string, Function*>& memberFunctions,
          const std::map<std::string, Function*>& staticFunctions,
          const std::map<std::string, Function*>& propertyFunctions,
          const std::map<std::string, PyObject*>& classMembers
          ) :
            Type(catHeldClass),
            m_members(members),
            m_memberFunctions(memberFunctions),
            m_staticFunctions(staticFunctions),
            m_propertyFunctions(propertyFunctions),
            m_classMembers(classMembers),
            m_hasComparisonOperators(false)
    {
        m_name = inName;
        m_is_simple = false;

        if (m_memberFunctions.find("__eq__") != m_memberFunctions.end()) { m_hasComparisonOperators = true; }
        if (m_memberFunctions.find("__ne__") != m_memberFunctions.end()) { m_hasComparisonOperators = true; }
        if (m_memberFunctions.find("__lt__") != m_memberFunctions.end()) { m_hasComparisonOperators = true; }
        if (m_memberFunctions.find("__gt__") != m_memberFunctions.end()) { m_hasComparisonOperators = true; }
        if (m_memberFunctions.find("__le__") != m_memberFunctions.end()) { m_hasComparisonOperators = true; }
        if (m_memberFunctions.find("__ge__") != m_memberFunctions.end()) { m_hasComparisonOperators = true; }

        forwardTypesMayHaveChanged();
    }

    bool isBinaryCompatibleWithConcrete(Type* other);

    template<class visitor_type>
    void _visitContainedTypes(const visitor_type& visitor) {
        for (auto& o: m_members) {
            visitor(std::get<1>(o));
        }
    }

    template<class visitor_type>
    void _visitReferencedTypes(const visitor_type& visitor) {
        for (auto& o: m_members) {
            visitor(std::get<1>(o));
        }
        for (auto& o: m_memberFunctions) {
            Type* t = std::get<1>(o);
            visitor(t);
            assert(t == std::get<1>(o));
        }
        for (auto& o: m_staticFunctions) {
            Type* t = std::get<1>(o);
            visitor(t);
            assert(t == std::get<1>(o));
        }
    }

    void _forwardTypesMayHaveChanged();

    static HeldClass* Make(
            std::string inName,
            const std::vector<std::tuple<std::string, Type*, Instance> >& members,
            const std::map<std::string, Function*>& memberFunctions,
            const std::map<std::string, Function*>& staticFunctions,
            const std::map<std::string, Function*>& propertyFunctions,
            const std::map<std::string, PyObject*>& classMembers
            )
    {
        return new HeldClass(inName, members, memberFunctions, staticFunctions, propertyFunctions, classMembers);
    }

    HeldClass* renamed(std::string newName) {
        return Make(newName,
            m_members,
            m_memberFunctions,
            m_staticFunctions,
            m_propertyFunctions,
            m_classMembers
            );
    }

    instance_ptr eltPtr(instance_ptr self, int64_t ix) const {
        return self + m_byte_offsets[ix];
    }

    bool cmp(instance_ptr left, instance_ptr right, int pyComparisonOp);

    template<class buf_t>
    void deserialize(instance_ptr self, buf_t& buffer) {
        for (long k = 0; k < m_members.size();k++) {
            bool isInitialized = buffer.read_uint8();
            if (isInitialized) {
                std::get<1>(m_members[k])->deserialize(eltPtr(self,k),buffer);
                setInitializationFlag(self, k);
            }
        }
    }

    template<class buf_t>
    void serialize(instance_ptr self, buf_t& buffer) {
        for (long k = 0; k < m_members.size();k++) {
            bool isInitialized = checkInitializationFlag(self, k);
            if (isInitialized) {
                buffer.write_uint8(true);
                std::get<1>(m_members[k])->serialize(eltPtr(self,k),buffer);
            } else {
                buffer.write_uint8(false);
            }
        }
    }

    void repr(instance_ptr self, ReprAccumulator& stream);

    int32_t hash32(instance_ptr left);

    template<class sub_constructor>
    void constructor(instance_ptr self, const sub_constructor& initializer) const {
        for (int64_t k = 0; k < m_members.size(); k++) {
            try {
                initializer(eltPtr(self, k), k);
                setInitializationFlag(self, k);
            } catch(...) {
                for (long k2 = k-1; k2 >= 0; k2--) {
                    std::get<1>(m_members[k2])->destroy(eltPtr(self,k2));
                }
                throw;
            }
        }
    }

    void setAttribute(instance_ptr self, int memberIndex, instance_ptr other) const;

    void emptyConstructor(instance_ptr self);

    //don't default construct classes
    static bool wantsToDefaultConstruct(Type* t) {
        return t->is_default_constructible() && t->getTypeCategory() != TypeCategory::catClass;
    }

    void constructor(instance_ptr self);

    void destroy(instance_ptr self);

    void copy_constructor(instance_ptr self, instance_ptr other);

    void assign(instance_ptr self, instance_ptr other);

    bool checkInitializationFlag(instance_ptr self, int memberIndex) const {
        int byte = memberIndex / 8;
        int bit = memberIndex % 8;
        return bool( ((uint8_t*)self)[byte] & (1 << bit) );
    }

    void setInitializationFlag(instance_ptr self, int memberIndex) const;

    void clearInitializationFlag(instance_ptr self, int memberIndex) const;

    Type* getMemberType(int index) const {
        return std::get<1>(m_members[index]);
    }

    const std::string& getMemberName(int index) const {
        return std::get<0>(m_members[index]);
    }

    bool memberHasDefaultValue(int index) const {
        return std::get<2>(m_members[index]).type()->getTypeCategory() != TypeCategory::catNone;
    }

    const Instance& getMemberDefaultValue(int index) const {
        return std::get<2>(m_members[index]);
    }

    const std::vector<std::tuple<std::string, Type*, Instance> >& getMembers() const {
        return m_members;
    }

    const std::map<std::string, Function*>& getMemberFunctions() const {
        return m_memberFunctions;
    }

    const std::map<std::string, Function*>& getStaticFunctions() const {
        return m_staticFunctions;
    }

    const std::map<std::string, PyObject*>& getClassMembers() const {
        return m_classMembers;
    }

    const std::map<std::string, Function*>& getPropertyFunctions() const {
        return m_propertyFunctions;
    }

    const std::vector<size_t>& getOffsets() const {
        return m_byte_offsets;
    }

    int memberNamed(const char* c) const;

    bool hasAnyComparisonOperators() const {
        return m_hasComparisonOperators;
    }

private:
    std::vector<size_t> m_byte_offsets;

    std::vector<std::tuple<std::string, Type*, Instance> > m_members;

    std::map<std::string, Function*> m_memberFunctions;
    std::map<std::string, Function*> m_staticFunctions;
    std::map<std::string, Function*> m_propertyFunctions;
    std::map<std::string, PyObject*> m_classMembers;

    bool m_hasComparisonOperators;
};

