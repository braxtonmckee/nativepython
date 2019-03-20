#pragma once

#include "Type.hpp"
#include "ReprAccumulator.hpp"

class BoundMethod : public Type {
public:
    BoundMethod(Type* inFirstArg, Function* inFunc) : Type(TypeCategory::catBoundMethod)
    {
        m_function = inFunc;
        m_is_default_constructible = false;
        m_first_arg = inFirstArg;
        m_size = inFirstArg->bytecount();
        m_is_simple = false;

        forwardTypesMayHaveChanged();
    }

    template<class visitor_type>
    void _visitContainedTypes(const visitor_type& visitor) {
        visitor(m_first_arg);
    }

    template<class visitor_type>
    void _visitReferencedTypes(const visitor_type& visitor) {
        Type* c = m_first_arg;
        Type* f = m_function;

        visitor(c);
        visitor(f);

        assert(c == m_first_arg);
        assert(f == m_function);
    }

    void _forwardTypesMayHaveChanged() {
        m_name = "BoundMethod(" + m_first_arg->name() + "." + m_function->name() + ")";
        m_size = m_first_arg->bytecount();
    }

    static BoundMethod* Make(Type* c, Function* f) {
        static std::mutex guard;

        std::lock_guard<std::mutex> lock(guard);

        typedef std::pair<Type*, Function*> keytype;

        static std::map<keytype, BoundMethod*> m;

        auto it = m.find(keytype(c,f));

        if (it == m.end()) {
            it = m.insert(
                std::make_pair(keytype(c,f), new BoundMethod(c, f))
                ).first;
        }

        return it->second;
    }

    void repr(instance_ptr self, ReprAccumulator& stream) {
        stream << m_name;
    }

    int32_t hash32(instance_ptr left) {
        return m_first_arg->hash32(left);
    }

    template<class buf_t>
    void deserialize(instance_ptr self, buf_t& buffer) {
        m_first_arg->deserialize(self,buffer);
    }

    template<class buf_t>
    void serialize(instance_ptr self, buf_t& buffer) {
        m_first_arg->serialize(self,buffer);
    }

    bool cmp(instance_ptr left, instance_ptr right, int pyComparisonOp) {
        return m_first_arg->cmp(left,right,pyComparisonOp);
    }

    void constructor(instance_ptr self) {
        m_first_arg->constructor(self);
    }

    void destroy(instance_ptr self) {
        m_first_arg->destroy(self);
    }

    void copy_constructor(instance_ptr self, instance_ptr other) {
        m_first_arg->copy_constructor(self, other);
    }

    void assign(instance_ptr self, instance_ptr other) {
        m_first_arg->assign(self, other);
    }

    Type* getFirstArgType() const {
        return m_first_arg;
    }

    Function* getFunction() const {
        return m_function;
    }

private:
    Function* m_function;
    Type* m_first_arg;
};



