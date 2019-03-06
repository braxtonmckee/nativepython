#pragma once

#include <atomic>
#include <string>
#include "Type.hpp"

class DeserializationBuffer;

typedef uint8_t* instance_ptr;


class Instance {
private:
    class layout {
    public:
        std::atomic<int64_t> refcount;
        Type* type;
        uint8_t data[];
    };

    Instance(layout* l);

    static layout* allocateNoneLayout();

    static layout* noneLayout();

public:
    static Instance deserialized(Type* t, DeserializationBuffer& buf);

    static Instance create(bool val);

    static Instance create(long val);

    static Instance create(double val);

    static Instance create(Type*t, instance_ptr data);

    static Instance create(Type*t);

    Instance();

    Instance(const Instance& other);

    Instance(instance_ptr p, Type* t);

    template<class initializer_type>
    static Instance createAndInitialize(Type* t, const initializer_type& initFun) {
        return Instance(t, initFun);
    }

    template<class initializer_type>
    Instance(Type* t, const initializer_type& initFun) : mLayout(nullptr) {
        t->assertForwardsResolved();

        layout* l = (layout*)malloc(sizeof(layout) + t->bytecount());

        try {
            initFun(l->data);
        } catch(...) {
            free(l);
            throw;
        }

        l->refcount = 1;
        l->type = t;

        mLayout = l;
    }

    ~Instance();

    Instance& operator=(const Instance& other);

    bool operator<(const Instance& other) const;

    std::string repr() const;

    int32_t hash32() const;

    Type* type() const {
        return mLayout->type;
    }

    instance_ptr data() const {
        return mLayout->data;
    }

    int64_t refcount() const {
        return mLayout->refcount;
    }

private:
    layout* mLayout;
};
