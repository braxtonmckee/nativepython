#include "AllTypes.hpp"
#include "Instance.hpp"

Instance::layout* Instance::allocateNoneLayout() {
    layout* result = (layout*)malloc(sizeof(layout));
    result->refcount = 0;
    result->type = None::Make();

    return result;
}

Instance::layout* Instance::noneLayout() {
    static layout* noneLayout = allocateNoneLayout();
    noneLayout->refcount++;

    return noneLayout;
}

Instance Instance::deserialized(Type* t, DeserializationBuffer& buf) {
    t->assertForwardsResolved();

    return createAndInitialize(t, [&](instance_ptr tgt) {
        t->deserialize(tgt, buf);
    });
}

Instance Instance::create(bool val) {
    return create(Bool::Make(), (instance_ptr)&val);
}

Instance Instance::create(long val) {
    return create(Int64::Make(), (instance_ptr)&val);
}

Instance Instance::create(double val) {
    return create(Float64::Make(), (instance_ptr)&val);
}

Instance Instance::create(Type*t, instance_ptr data) {
    t->assertForwardsResolved();

    return createAndInitialize(t, [&](instance_ptr tgt) {
        t->copy_constructor(tgt, data);
    });
}
Instance Instance::create(Type*t) {
    t->assertForwardsResolved();

    return createAndInitialize(t, [&](instance_ptr tgt) {
        t->constructor(tgt);
    });
}

Instance::Instance() {
    // by default, None
    mLayout = noneLayout();
    mLayout->refcount++;
}

Instance::Instance(const Instance& other) : mLayout(other.mLayout) {
    mLayout->refcount++;
}

Instance::Instance(instance_ptr p, Type* t) : mLayout(nullptr) {
    t->assertForwardsResolved();

    layout* l = (layout*)malloc(sizeof(layout) + t->bytecount());

    try {
        t->copy_constructor(l->data, p);
    } catch(...) {
        free(l);
        throw;
    }

    l->refcount = 1;
    l->type = t;

    mLayout = l;
}

Instance::~Instance() {
    if (mLayout->refcount.fetch_sub(1) == 1) {
        mLayout->type->destroy(mLayout->data);
        free(mLayout);
    }
}

Instance& Instance::operator=(const Instance& other) {
    other.mLayout->refcount++;

    if (mLayout->refcount.fetch_sub(1) == 1) {
        mLayout->type->destroy(mLayout->data);
        free(mLayout);
    }

    mLayout = other.mLayout;
    return *this;
}

bool Instance::operator<(const Instance& other) const {
    if (mLayout->type < other.mLayout->type) {
        return true;
    }
    if (mLayout->type > other.mLayout->type) {
        return false;
    }
    return mLayout->type->cmp(mLayout->data, other.mLayout->data, Py_LT);
}

std::string Instance::repr() const {
    std::ostringstream s;
    ReprAccumulator accumulator(s);

    accumulator << std::showpoint;
    mLayout->type->repr(mLayout->data, accumulator);
    return s.str();
}

int32_t Instance::hash32() const {
    return mLayout->type->hash32(mLayout->data);
}
