#pragma once

#include "Type.hpp"

class Instance;

class Class : public Type {
    class layout {
    public:
        std::atomic<int64_t> refcount;
        unsigned char data[];
    };

public:
    Class(HeldClass* inClass) :
            Type(catClass),
            m_heldClass(inClass)
    {
        m_size = sizeof(layout*);
        m_is_default_constructible = inClass->is_default_constructible();
        m_name = m_heldClass->name();
        m_is_simple = false;

        forwardTypesMayHaveChanged();
    }

    bool isBinaryCompatibleWithConcrete(Type* other);

    template<class visitor_type>
    void _visitContainedTypes(const visitor_type& visitor) {
    }

    template<class visitor_type>
    void _visitReferencedTypes(const visitor_type& visitor) {
        Type* t = m_heldClass;
        visitor(t);
        assert(t == m_heldClass);
    }

    void _forwardTypesMayHaveChanged();

    static Class* Make(
            std::string inName,
            const std::vector<std::tuple<std::string, Type*, Instance> >& members,
            const std::map<std::string, Function*>& memberFunctions,
            const std::map<std::string, Function*>& staticFunctions,
            const std::map<std::string, Function*>& propertyFunctions,
            const std::map<std::string, PyObject*>& classMembers
            )
    {
        return new Class(HeldClass::Make(inName, members, memberFunctions, staticFunctions, propertyFunctions, classMembers));
    }

    Class* renamed(std::string newName) {
        return new Class(m_heldClass->renamed(newName));
    }

    instance_ptr eltPtr(instance_ptr self, int64_t ix) const;

    void setAttribute(instance_ptr self, int64_t ix, instance_ptr elt) const;

    bool checkInitializationFlag(instance_ptr self, int64_t ix) const;

    bool cmp(instance_ptr left, instance_ptr right, int pyComparisonOp);

    template<class buf_t>
    void deserialize(instance_ptr self, buf_t& buffer) {
        *(layout**)self = (layout*)malloc(
            sizeof(layout) + m_heldClass->bytecount()
            );

        layout& record = **(layout**)self;
        record.refcount = 1;

        m_heldClass->deserialize(record.data, buffer);
    }

    template<class buf_t>
    void serialize(instance_ptr self, buf_t& buffer) {
        layout& l = **(layout**)self;
        m_heldClass->serialize(l.data, buffer);
    }

    void repr(instance_ptr self, ReprAccumulator& stream);

    int32_t hash32(instance_ptr left);

    template<class sub_constructor>
    void constructor(instance_ptr self, const sub_constructor& initializer) const {
        *(layout**)self = (layout*)malloc(sizeof(layout) + m_heldClass->bytecount());
        layout& l = **(layout**)self;
        l.refcount = 1;

        try {
            m_heldClass->constructor(l.data, initializer);
        } catch (...) {
            free(*(layout**)self);
        }
    }

    void emptyConstructor(instance_ptr self);

    void constructor(instance_ptr self);

    void destroy(instance_ptr self);

    int64_t refcount(instance_ptr self);

    void copy_constructor(instance_ptr self, instance_ptr other);

    void assign(instance_ptr self, instance_ptr other);

    Type* getMemberType(int index) const {
        return m_heldClass->getMemberType(index);
    }

    const std::string& getMemberName(int index) const {
        return m_heldClass->getMemberName(index);
    }

    const std::vector<std::tuple<std::string, Type*, Instance> >& getMembers() const {
        return m_heldClass->getMembers();
    }

    const std::map<std::string, Function*>& getMemberFunctions() const {
        return m_heldClass->getMemberFunctions();
    }

    const std::map<std::string, Function*>& getStaticFunctions() const {
        return m_heldClass->getStaticFunctions();
    }

    const std::map<std::string, PyObject*>& getClassMembers() const {
        return m_heldClass->getClassMembers();
    }

    const std::map<std::string, Function*>& getPropertyFunctions() const {
        return m_heldClass->getPropertyFunctions();
    }

    int memberNamed(const char* c) const {
        return m_heldClass->memberNamed(c);
    }

    HeldClass* getHeldClass() const {
        return m_heldClass;
    }



private:
    HeldClass* m_heldClass;
};

