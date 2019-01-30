#include "AllTypes.hpp"

int32_t Bytes::hash32(instance_ptr left) {
    Hash32Accumulator acc((int)getTypeCategory());

    if (!(*(layout**)left)) {
        return 0x1234;
    }

    if ((*(layout**)left)->hash_cache == -1) {
        Hash32Accumulator acc((int)getTypeCategory());

        acc.addBytes(eltPtr(left, 0), count(left));

        (*(layout**)left)->hash_cache = acc.get();
        if ((*(layout**)left)->hash_cache == -1) {
            (*(layout**)left)->hash_cache = -2;
        }
    }

    return (*(layout**)left)->hash_cache;
}

char Bytes::cmp(instance_ptr left, instance_ptr right) {
    if ( !(*(layout**)left) && !(*(layout**)right) ) {
        return 0;
    }
    if ( !(*(layout**)left) && (*(layout**)right) ) {
        return -1;
    }
    if ( (*(layout**)left) && !(*(layout**)right) ) {
        return 1;
    }
    if ( (*(layout**)left) == (*(layout**)right) ) {
        return 0;
    }

    char res = byteCompare(eltPtr(left, 0), eltPtr(right, 0), std::min(count(left), count(right)));

    if (res) {
        return res;
    }

    if (count(left) < count(right)) {
        return -1;
    }

    if (count(left) > count(right)) {
        return 1;
    }

    return 0;
}

Bytes::layout* Bytes::concatenate(layout* lhs, layout* rhs) {
    if (!rhs && !lhs) {
        return lhs;
    }
    if (!rhs) {
        lhs->refcount++;
        return lhs;
    }
    if (!lhs) {
        rhs->refcount++;
        return rhs;
    }

    layout* new_layout = (layout*)malloc(sizeof(layout) + rhs->bytecount + lhs->bytecount);
    new_layout->refcount = 1;
    new_layout->hash_cache = -1;
    new_layout->bytecount = lhs->bytecount + rhs->bytecount;

    memcpy(new_layout->data, lhs->data, lhs->bytecount);
    memcpy(new_layout->data + lhs->bytecount, rhs->data, rhs->bytecount);

    return new_layout;
}

Bytes::layout* Bytes::createFromPtr(const char* data, int64_t length) {
    layout* new_layout = (layout*)malloc(sizeof(layout) + length);
    new_layout->refcount = 1;
    new_layout->hash_cache = -1;
    new_layout->bytecount = length;

    memcpy(new_layout->data, data, length);

    return new_layout;
}

void Bytes::constructor(instance_ptr self, int64_t count, const char* data) const {
    if (count == 0) {
        *(layout**)self = nullptr;
        return;
    }
    (*(layout**)self) = (layout*)malloc(sizeof(layout) + count);

    (*(layout**)self)->bytecount = count;
    (*(layout**)self)->refcount = 1;
    (*(layout**)self)->hash_cache = -1;

    if (data) {
        ::memcpy((*(layout**)self)->data, data, count);
    }
}

instance_ptr Bytes::eltPtr(instance_ptr self, int64_t i) const {
    //we don't want to have to return null here, but there is no actual memory to back this.
    if (*(layout**)self == nullptr) {
        return self;
    }

    return (*(layout**)self)->data + i;
}

int64_t Bytes::count(instance_ptr self) const {
    if (*(layout**)self == nullptr) {
        return 0;
    }

    return (*(layout**)self)->bytecount;
}

void Bytes::destroy(instance_ptr self) {
    if (!*(layout**)self) {
        return;
    }

    (*(layout**)self)->refcount--;

    if ((*(layout**)self)->refcount == 0) {
        free((*(layout**)self));
    }
}

void Bytes::copy_constructor(instance_ptr self, instance_ptr other) {
    (*(layout**)self) = (*(layout**)other);
    if (*(layout**)self) {
        (*(layout**)self)->refcount++;
    }
}

void Bytes::assign(instance_ptr self, instance_ptr other) {
    layout* old = (*(layout**)self);

    (*(layout**)self) = (*(layout**)other);

    if (*(layout**)self) {
        (*(layout**)self)->refcount++;
    }

    destroy((instance_ptr)&old);
}

