#pragma once

#include "Type.hpp"

template<class T>
class RegisterType : public Type {
public:
    RegisterType(TypeCategory kind) : Type(kind)
    {
        m_size = sizeof(T);
        m_is_default_constructible = true;
    }

    bool isBinaryCompatibleWithConcrete(Type* other) {
        if (other->getTypeCategory() != m_typeCategory) {
            return false;
        }

        return true;
    }

    void _forwardTypesMayHaveChanged() {}

    template<class visitor_type>
    void _visitReferencedTypes(const visitor_type& v) {}

    template<class visitor_type>
    void _visitContainedTypes(const visitor_type& v) {}

    bool cmp(instance_ptr left, instance_ptr right, int pyComparisonOp) {
        if ( (*(T*)left) < (*(T*)right) ) {
            return cmpResultToBoolForPyOrdering(pyComparisonOp, -1);
        }
        if ( (*(T*)left) > (*(T*)right) ) {
            return cmpResultToBoolForPyOrdering(pyComparisonOp, 1);
        }

        return cmpResultToBoolForPyOrdering(pyComparisonOp, 0);
    }

    int32_t hash32(instance_ptr left) {
        Hash32Accumulator acc((int)getTypeCategory());

        acc.addRegister(*(T*)left);

        return acc.get();
    }

    void constructor(instance_ptr self) {
        new ((T*)self) T();
    }

    void destroy(instance_ptr self) {}

    void copy_constructor(instance_ptr self, instance_ptr other) {
        *((T*)self) = *((T*)other);
    }

    void assign(instance_ptr self, instance_ptr other) {
        *((T*)self) = *((T*)other);
    }

    template<class buf_t>
    void deserialize(instance_ptr self, buf_t& buffer) {
        buffer.readInto((T*)self);
    }

    template<class buf_t>
    void serialize(instance_ptr self, buf_t& buffer) {
        buffer.write(*(T*)self);
    }
};

class Bool : public RegisterType<bool> {
public:
    Bool() : RegisterType(TypeCategory::catBool)
    {
        m_name = "Bool";
    }

    void repr(instance_ptr self, ReprAccumulator& stream) {
        stream << (*(bool*)self ? "True":"False");
    }

    static Bool* Make() { static Bool res; return &res; }
};

class UInt8 : public RegisterType<uint8_t> {
public:
    UInt8() : RegisterType(TypeCategory::catUInt8)
    {
        m_name = "UInt8";
    }

    void repr(instance_ptr self, ReprAccumulator& stream) {
        stream << (uint64_t)*(uint8_t*)self << "u8";
    }

    static UInt8* Make() { static UInt8 res; return &res; }
};

class UInt16 : public RegisterType<uint16_t> {
public:
    UInt16() : RegisterType(TypeCategory::catUInt16)
    {
        m_name = "UInt16";
    }

    void repr(instance_ptr self, ReprAccumulator& stream) {
        stream << (uint64_t)*(uint16_t*)self << "u16";
    }

    static UInt16* Make() { static UInt16 res; return &res; }
};

class UInt32 : public RegisterType<uint32_t> {
public:
    UInt32() : RegisterType(TypeCategory::catUInt32)
    {
        m_name = "UInt32";
    }

    void repr(instance_ptr self, ReprAccumulator& stream) {
        stream << (uint64_t)*(uint32_t*)self << "u32";
    }

    static UInt32* Make() { static UInt32 res; return &res; }
};

class UInt64 : public RegisterType<uint64_t> {
public:
    UInt64() : RegisterType(TypeCategory::catUInt64)
    {
        m_name = "UInt64";
    }

    void repr(instance_ptr self, ReprAccumulator& stream) {
        stream << *(uint64_t*)self << "u64";
    }

    static UInt64* Make() { static UInt64 res; return &res; }
};

class Int8 : public RegisterType<int8_t> {
public:
    Int8() : RegisterType(TypeCategory::catInt8)
    {
        m_name = "Int8";
        m_size = 1;
    }

    void repr(instance_ptr self, ReprAccumulator& stream) {
        stream << (int64_t)*(int8_t*)self << "i8";
    }

    static Int8* Make() { static Int8 res; return &res; }
};

class Int16 : public RegisterType<int16_t> {
public:
    Int16() : RegisterType(TypeCategory::catInt16)
    {
        m_name = "Int16";
    }

    void repr(instance_ptr self, ReprAccumulator& stream) {
        stream << (int64_t)*(int16_t*)self << "i16";
    }

    static Int16* Make() { static Int16 res; return &res; }
};

class Int32 : public RegisterType<int32_t> {
public:
    Int32() : RegisterType(TypeCategory::catInt32)
    {
        m_name = "Int32";
    }

    void repr(instance_ptr self, ReprAccumulator& stream) {
        stream << (int64_t)*(int32_t*)self << "i32";
    }

    static Int32* Make() { static Int32 res; return &res; }
};

class Int64 : public RegisterType<int64_t> {
public:
    Int64() : RegisterType(TypeCategory::catInt64)
    {
        m_name = "Int64";
    }

    void repr(instance_ptr self, ReprAccumulator& stream) {
        stream << *(int64_t*)self;
    }

    static Int64* Make() { static Int64 res; return &res; }
};

class Float32 : public RegisterType<float> {
public:
    Float32() : RegisterType(TypeCategory::catFloat32)
    {
        m_name = "Float32";
    }

    void repr(instance_ptr self, ReprAccumulator& stream) {
        stream << *(float*)self << "f32";
    }

    static Float32* Make() { static Float32 res; return &res; }
};

class Float64 : public RegisterType<double> {
public:
    Float64() : RegisterType(TypeCategory::catFloat64)
    {
        m_name = "Float64";
    }

    void repr(instance_ptr self, ReprAccumulator& stream) {
        stream << *(double*)self;
    }

    static Float64* Make() { static Float64 res; return &res; }
};

template<class T>
class GetRegisterType {};

template<> class GetRegisterType<bool> { public: Type* operator()() const { return Bool::Make(); } };
template<> class GetRegisterType<int8_t> { public: Type* operator()() const { return Int8::Make(); } };
template<> class GetRegisterType<int16_t> { public: Type* operator()() const { return Int16::Make(); } };
template<> class GetRegisterType<int32_t> { public: Type* operator()() const { return Int32::Make(); } };
template<> class GetRegisterType<int64_t> { public: Type* operator()() const { return Int64::Make(); } };
template<> class GetRegisterType<uint8_t> { public: Type* operator()() const { return UInt8::Make(); } };
template<> class GetRegisterType<uint16_t> { public: Type* operator()() const { return UInt16::Make(); } };
template<> class GetRegisterType<uint32_t> { public: Type* operator()() const { return UInt32::Make(); } };
template<> class GetRegisterType<uint64_t> { public: Type* operator()() const { return UInt64::Make(); } };
template<> class GetRegisterType<float> { public: Type* operator()() const { return Float32::Make(); } };
template<> class GetRegisterType<double> { public: Type* operator()() const { return Float64::Make(); } };

template<>
class TypeDetails<int64_t> {
public:
    static Type* getType() { return Int64::Make(); }

    static const uint64_t bytecount = sizeof(int64_t);
};

template<>
class TypeDetails<uint64_t> {
public:
    static Type* getType() { return UInt64::Make(); }

    static const uint64_t bytecount = sizeof(uint64_t);
};

template<>
class TypeDetails<int32> {
public:
    static Type* getType() { return Int32::Make(); }

    static const uint64_t bytecount = sizeof(int32_t);
};

template<>
class TypeDetails<uint32_t> {
public:
    static Type* getType() { return UInt32::Make(); }

    static const uint64_t bytecount = sizeof(uint32_t);
};

template<>
class TypeDetails<int16> {
public:
    static Type* getType() { return Int16::Make(); }

    static const uint64_t bytecount = sizeof(int16_t);
};

template<>
class TypeDetails<uint16_t> {
public:
    static Type* getType() { return UInt16::Make(); }

    static const uint64_t bytecount = sizeof(uint16_t);
};

template<>
class TypeDetails<int8> {
public:
    static Type* getType() { return Int8::Make(); }

    static const uint64_t bytecount = sizeof(int8_t);
};

template<>
class TypeDetails<uint8_t> {
public:
    static Type* getType() { return UInt8::Make(); }

    static const uint64_t bytecount = sizeof(uint8_t);
};

template<>
class TypeDetails<bool> {
public:
    static Type* getType() { return Bool::Make(); }

    static const uint64_t bytecount = sizeof(bool);
};

template<>
class TypeDetails<float> {
public:
    static Type* getType() { return Float32::Make(); }

    static const uint64_t bytecount = sizeof(float);
};

template<>
class TypeDetails<double> {
public:
    static Type* getType() { return Float64::Make(); }

    static const uint64_t bytecount = sizeof(double);
};
