#include "AllTypes.hpp"

Type* Value::MakeInt64(int64_t i) {
    return Make(Instance::create(Int64::Make(), (instance_ptr)&i));
}

Type* Value::MakeFloat64(double i) {
    return Make(Instance::create(Float64::Make(), (instance_ptr)&i));
}

Type* Value::MakeBool(bool i) {
    return Make(Instance::create(Bool::Make(), (instance_ptr)&i));
}

Type* Value::MakeBytes(char* data, size_t count) {
    return Make(Instance::createAndInitialize(Bytes::Make(), [&](instance_ptr i) {
        Bytes::Make()->constructor(i, count, data);
    }));
}

Type* Value::MakeString(size_t bytesPerCodepoint, size_t count, char* data) {
    return Make(Instance::createAndInitialize(StringType::Make(), [&](instance_ptr i) {
        StringType::Make()->constructor(i, bytesPerCodepoint, count, data);
    }));
}

