#pragma once

#include <Python.h>
#include <string>
#include <vector>
#include <algorithm>
#include <functional>
#include <mutex>
#include <set>
#include <map>
#include <tuple>
#include <utility>
#include <atomic>
#include <iostream>
#include "ReprAccumulator.hpp"
#include "SerializationContext.hpp"
#include "HashAccumulator.hpp"
#include "util.hpp"

class SerializationBuffer;
class DeserializationBuffer;

class Type;
class None;
class Bool;
class UInt8;
class UInt16;
class UInt32;
class UInt64;
class Int8;
class Int16;
class Int32;
class Int64;
class Float32;
class Float64;
class StringType;
class Bytes;
class OneOf;
class Value;
class TupleOfType;
class PointerTo;
class ListOfType;
class NamedTuple;
class Tuple;
class Dict;
class ConstDict;
class Alternative;
class ConcreteAlternative;
class PythonSubclass;
class PythonObjectOfType;
class Class;
class HeldClass;
class Function;
class BoundMethod;
class Forward;

typedef uint8_t* instance_ptr;

typedef void (*compiled_code_entrypoint)(instance_ptr, instance_ptr*);

void updateTypeRepForType(Type* t, PyTypeObject* pyType);

class Type {
public:
    enum TypeCategory {
        catNone,
        catBool,
        catUInt8,
        catUInt16,
        catUInt32,
        catUInt64,
        catInt8,
        catInt16,
        catInt32,
        catInt64,
        catString,
        catBytes,
        catFloat32,
        catFloat64,
        catValue,
        catOneOf,
        catTupleOf,
        catPointerTo,
        catListOf,
        catNamedTuple,
        catTuple,
        catDict,
        catConstDict,
        catAlternative,
        catConcreteAlternative, //concrete Alternative subclass
        catPythonSubclass, //subclass of a nativepython type
        catPythonObjectOfType, //a python object that matches 'isinstance' on a particular type
        catBoundMethod,
        catClass,
        catHeldClass,
        catFunction,
        catForward
    };

    TypeCategory getTypeCategory() const {
        return m_typeCategory;
    }

    bool isComposite() const {
        return (
            m_typeCategory == catTuple ||
            m_typeCategory == catNamedTuple
            );
    }

    const std::string& name() const {
        return m_name;
    }

    size_t bytecount() const {
        return m_size;
    }

    Type* pickConcreteSubclass(instance_ptr data) {
        assertForwardsResolved();

        return this->check([&](auto& subtype) {
            return subtype.pickConcreteSubclassConcrete(data);
        });
    }

    Type* pickConcreteSubclassConcrete(instance_ptr data) {
        return this;
    }

    void repr(instance_ptr self, ReprAccumulator& out);

    bool cmp(instance_ptr left, instance_ptr right, int pyComparisonOp);

    int32_t hash32(instance_ptr left);

    template<class buf_t>
    void serialize(instance_ptr left, buf_t& buffer) {
        assertForwardsResolved();

        return this->check([&](auto& subtype) {
            return subtype.serialize(left, buffer);
        });
    }

    template<class buf_t>
    void deserialize(instance_ptr left, buf_t& buffer) {
        assertForwardsResolved();

        return this->check([&](auto& subtype) {
            return subtype.deserialize(left, buffer);
        });
    }

    void swap(instance_ptr left, instance_ptr right);

    static char byteCompare(uint8_t* l, uint8_t* r, size_t count);

    template<class T>
    auto check(const T& f) -> decltype(f(*this)) {
        switch (m_typeCategory) {
            case catNone:
                return f(*(None*)this);
            case catBool:
                return f(*(Bool*)this);
            case catUInt8:
                return f(*(UInt8*)this);
            case catUInt16:
                return f(*(UInt16*)this);
            case catUInt32:
                return f(*(UInt32*)this);
            case catUInt64:
                return f(*(UInt64*)this);
            case catInt8:
                return f(*(Int8*)this);
            case catInt16:
                return f(*(Int16*)this);
            case catInt32:
                return f(*(Int32*)this);
            case catInt64:
                return f(*(Int64*)this);
            case catString:
                return f(*(StringType*)this);
            case catBytes:
                return f(*(Bytes*)this);
            case catFloat32:
                return f(*(Float32*)this);
            case catFloat64:
                return f(*(Float64*)this);
            case catValue:
                return f(*(Value*)this);
            case catOneOf:
                return f(*(OneOf*)this);
            case catTupleOf:
                return f(*(TupleOfType*)this);
            case catPointerTo:
                return f(*(PointerTo*)this);
            case catListOf:
                return f(*(ListOfType*)this);
            case catNamedTuple:
                return f(*(NamedTuple*)this);
            case catTuple:
                return f(*(Tuple*)this);
            case catDict:
                return f(*(Dict*)this);
            case catConstDict:
                return f(*(ConstDict*)this);
            case catAlternative:
                return f(*(Alternative*)this);
            case catConcreteAlternative:
                return f(*(ConcreteAlternative*)this);
            case catPythonSubclass:
                return f(*(PythonSubclass*)this);
            case catPythonObjectOfType:
                return f(*(PythonObjectOfType*)this);
            case catClass:
                return f(*(Class*)this);
            case catHeldClass:
                return f(*(HeldClass*)this);
            case catFunction:
                return f(*(Function*)this);
            case catBoundMethod:
                return f(*(BoundMethod*)this);
            case catForward:
                return f(*(Forward*)this);
            default:
                throw std::runtime_error("Invalid type found");
        }
    }

    Type* getBaseType() const {
        return m_base;
    }

    void assertForwardsResolved() const {
        if (m_references_unresolved_forwards) {
            throw std::logic_error("Type " + m_name + " has unresolved forwards.");
        }
        if (m_failed_resolution) {
            throw std::logic_error("Type " + m_name + " failed to resolve.");
        }
    }

    //this MUST be called while holding the GIL
    template<class resolve_py_callable_to_type>
    Type* guaranteeForwardsResolved(const resolve_py_callable_to_type& resolver) {
        if (m_failed_resolution) {
            throw std::runtime_error("Type failed to resolve the first time it was triggered.");
        }

        if (m_checking_for_references_unresolved_forwards) {
            //it's ok to bail early. the call stack will recurse
            //back to this point, and during the unwind will ensure
            //that we check that any forwards are resolved.
            return this;
        }

        if (m_references_unresolved_forwards) {
            m_checking_for_references_unresolved_forwards = true;
            m_references_unresolved_forwards = false;

            Type* res;

            try {
                res = this->check([&](auto& subtype) {
                    return subtype.guaranteeForwardsResolvedConcrete(resolver);
                });
            } catch(...) {
                m_checking_for_references_unresolved_forwards = false;
                m_failed_resolution = true;
                throw;
            }

            m_checking_for_references_unresolved_forwards = false;

            forwardTypesMayHaveChanged();

            if (res != this) {
                return res->guaranteeForwardsResolved(resolver);
            }

            return res;
        }

        return this;
    }

    template<class resolve_py_callable_to_type>
    Type* guaranteeForwardsResolvedConcrete(resolve_py_callable_to_type& resolver) {
        typedef Type* type_ptr;

        bool didSomething = false;

        visitReferencedTypes([&](type_ptr& t) {
            t = t->guaranteeForwardsResolved(resolver);
        });

        forwardTypesMayHaveChanged();

        return this;
    }

    void _forwardTypesMayHaveChanged() {}

    template<class visitor_type>
    void _visitReferencedTypes(const visitor_type& v) {}

    template<class visitor_type>
    void _visitContainedTypes(const visitor_type& v) {}


    // call subtype.constructor
    void constructor(instance_ptr self);

    template<class ptr_func>
    void constructor(int64_t count, const ptr_func& ptrToChild) {
        assertForwardsResolved();

        this->check([&](auto& subtype) {
            for (long k = 0; k < count; k++) {
                subtype.constructor(ptrToChild(k));
            }
        });
    }

    // call subtype.destroy
    void destroy(instance_ptr self);

    template<class ptr_func>
    void destroy(int64_t count, const ptr_func& ptrToChild) {
        assertForwardsResolved();

        this->check([&](auto& subtype) {
            for (long k = 0; k < count; k++) {
                subtype.destroy(ptrToChild(k));
            }
        });
    }

    template<class visitor_type>
    void visitContainedTypes(const visitor_type& v) {
        this->check([&](auto& subtype) {
            subtype._visitContainedTypes(v);
        });
    }

    template<class visitor_type>
    void visitReferencedTypes(const visitor_type& v) {
        this->check([&](auto& subtype) {
            subtype._visitReferencedTypes(v);
        });
    }

    void forwardTypesMayHaveChanged();

    // call subtype.copy_constructor
    void copy_constructor(instance_ptr self, instance_ptr other);

    template<class ptr_func_dest, class ptr_func_src>
    void copy_constructor(int64_t count, const ptr_func_dest& ptrToTarget, const ptr_func_src& ptrToSrc) {
        assertForwardsResolved();

        this->check([&](auto& subtype) {
            for (long k = 0; k < count; k++) {
                subtype.copy_constructor(ptrToTarget(k), ptrToSrc(k));
            }
        });
    }

    // call subtype.assign
    void assign(instance_ptr self, instance_ptr other);

    PyTypeObject* getTypeRep() const {
        return mTypeRep;
    }

    void setTypeRep(PyTypeObject* o) {
        mTypeRep = o;
    }

    bool is_default_constructible() const {
        return m_is_default_constructible;
    }

    bool references_unresolved_forwards() const {
        return m_references_unresolved_forwards;
    }

    bool isBinaryCompatibleWith(Type* other);

    bool isBinaryCompatibleWithConcrete(Type* other) {
        return false;
    }

protected:
    Type(TypeCategory in_typeCategory) :
            m_typeCategory(in_typeCategory),
            m_size(0),
            m_is_default_constructible(false),
            m_name("Undefined"),
            mTypeRep(nullptr),
            m_base(nullptr),
            m_references_unresolved_forwards(false),
            m_checking_for_references_unresolved_forwards(false),
            m_failed_resolution(false)
        {}

    TypeCategory m_typeCategory;

    size_t m_size;

    bool m_is_default_constructible;

    std::string m_name;

    PyTypeObject* mTypeRep;

    Type* m_base;

    bool m_references_unresolved_forwards;

    bool m_checking_for_references_unresolved_forwards;

    bool m_failed_resolution;

    std::set<Type*> mUses;

    enum BinaryCompatibilityCategory { Incompatible, Checking, Compatible };

    std::map<Type*, BinaryCompatibilityCategory> mIsBinaryCompatible;

};

