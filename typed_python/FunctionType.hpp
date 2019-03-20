#pragma once

#include "Type.hpp"
#include "ReprAccumulator.hpp"


class Function : public Type {
public:
    class FunctionArg {
    public:
        FunctionArg(std::string name, Type* typeFilterOrNull, PyObject* defaultValue, bool isStarArg, bool isKwarg) :
            m_name(name),
            m_typeFilter(typeFilterOrNull),
            m_defaultValue(defaultValue),
            m_isStarArg(isStarArg),
            m_isKwarg(isKwarg)
        {
            assert(!(isStarArg && isKwarg));
        }

        std::string getName() const {
            return m_name;
        }

        PyObject* getDefaultValue() const {
            return m_defaultValue;
        }

        Type* getTypeFilter() const {
            return m_typeFilter;
        }

        bool getIsStarArg() const {
            return m_isStarArg;
        }

        bool getIsKwarg() const {
            return m_isKwarg;
        }

        bool getIsNormalArg() const {
            return !m_isKwarg && !m_isStarArg;
        }

        template<class visitor_type>
        void _visitReferencedTypes(const visitor_type& visitor) {
            if (m_typeFilter) {
                visitor(m_typeFilter);
            }
        }

    private:
        std::string m_name;
        Type* m_typeFilter;
        PyObject* m_defaultValue;
        bool m_isStarArg;
        bool m_isKwarg;
    };

    class CompiledSpecialization {
    public:
        CompiledSpecialization(
                    compiled_code_entrypoint funcPtr,
                    Type* returnType,
                    const std::vector<Type*>& argTypes
                    ) :
            mFuncPtr(funcPtr),
            mReturnType(returnType),
            mArgTypes(argTypes)
        {}

        compiled_code_entrypoint getFuncPtr() const {
            return mFuncPtr;
        }

        Type* getReturnType() const {
            return mReturnType;
        }

        const std::vector<Type*>& getArgTypes() const {
            return mArgTypes;
        }

    private:
        compiled_code_entrypoint mFuncPtr;
        Type* mReturnType;
        std::vector<Type*> mArgTypes;
    };

    class Overload {
    public:
        Overload(
            PyFunctionObject* functionObj,
            Type* returnType,
            const std::vector<FunctionArg>& args
            ) :
                mFunctionObj(functionObj),
                mReturnType(returnType),
                mArgs(args),
                mCompiledCodePtr(nullptr)
        {
        }

        PyFunctionObject* getFunctionObj() const {
            return mFunctionObj;
        }

        Type* getReturnType() const {
            return mReturnType;
        }

        const std::vector<FunctionArg>& getArgs() const {
            return mArgs;
        }

        template<class visitor_type>
        void _visitReferencedTypes(const visitor_type& visitor) {
            if (mReturnType) {
                visitor(mReturnType);
            }
            for (auto& a: mArgs) {
                a._visitReferencedTypes(visitor);
            }
        }

        const std::vector<CompiledSpecialization>& getCompiledSpecializations() const {
            return mCompiledSpecializations;
        }

        void addCompiledSpecialization(compiled_code_entrypoint e, Type* returnType, const std::vector<Type*>& argTypes) {
            mCompiledSpecializations.push_back(CompiledSpecialization(e,returnType,argTypes));
        }

    private:
        PyFunctionObject* mFunctionObj;
        Type* mReturnType;
        std::vector<FunctionArg> mArgs;
        std::vector<CompiledSpecialization> mCompiledSpecializations;
        compiled_code_entrypoint mCompiledCodePtr; //accepts a pointer to packed arguments and another pointer with the return value
    };

    class Matcher {
    public:
        Matcher(const Overload& overload) :
                mOverload(overload),
                mArgs(overload.getArgs())
        {
            m_used.resize(overload.getArgs().size());
            m_matches = true;
        }

        bool stillMatches() const {
            return m_matches;
        }

        //called at the end to see if this was a valid match
        bool definitelyMatches() const {
            if (!m_matches) {
                return false;
            }

            for (long k = 0; k < m_used.size(); k++) {
                if (!m_used[k] && !mArgs[k].getDefaultValue() && mArgs[k].getIsNormalArg()) {
                    return false;
                }
            }

            return true;
        }

        //push the state machine forward.
        Type* requiredTypeForArg(const char* name) {
            if (!name) {
                for (long k = 0; k < m_used.size(); k++) {
                    if (!m_used[k]) {
                        if (mArgs[k].getIsNormalArg()) {
                            m_used[k] = true;
                            return mArgs[k].getTypeFilter();
                        }
                        else if (mArgs[k].getIsStarArg()) {
                            //this doesn't consume the star arg
                            return mArgs[k].getTypeFilter();
                        }
                        else {
                            //this is a kwarg, but we didn't give a name.
                            m_matches = false;
                            return nullptr;
                        }
                    }
                }
            }
            else if (name) {
                for (long k = 0; k < m_used.size(); k++) {
                    if (!m_used[k]) {
                        if (mArgs[k].getIsNormalArg() && mArgs[k].getName() == name) {
                            m_used[k] = true;
                            return mArgs[k].getTypeFilter();
                        }
                        else if (mArgs[k].getIsNormalArg()) {
                            //just keep going
                        }
                        else if (mArgs[k].getIsStarArg()) {
                            //just keep going
                        } else {
                            //this is a kwarg
                            return mArgs[k].getTypeFilter();
                        }
                    }
                }
            }

            m_matches = false;
            return nullptr;
        }

    private:
        const Overload& mOverload;
        const std::vector<FunctionArg>& mArgs;
        std::vector<char> m_used;
        bool m_matches;
    };

    Function(std::string inName,
            const std::vector<Overload>& overloads
            ) :
        Type(catFunction),
        mOverloads(overloads)
    {
        m_name = inName;
        m_is_simple = false;
        m_is_default_constructible = true;
        m_size = 0;

        forwardTypesMayHaveChanged();
    }

    template<class visitor_type>
    void _visitContainedTypes(const visitor_type& visitor) {
    }

    template<class visitor_type>
    void _visitReferencedTypes(const visitor_type& visitor) {
        for (auto& o: mOverloads) {
            o._visitReferencedTypes(visitor);
        }
    }

    void _forwardTypesMayHaveChanged() {
    }

    static Function* merge(Function* f1, Function* f2) {
        std::vector<Overload> overloads(f1->mOverloads);
        for (auto o: f2->mOverloads) {
            overloads.push_back(o);
        }
        return new Function(f1->m_name, overloads);
    }

    bool cmp(instance_ptr left, instance_ptr right, int pyComparisonOp) {
        return cmpResultToBoolForPyOrdering(pyComparisonOp, 0);
    }

    template<class buf_t>
    void deserialize(instance_ptr self, buf_t& buffer) {
    }

    template<class buf_t>
    void serialize(instance_ptr self, buf_t& buffer) {
    }

    void repr(instance_ptr self, ReprAccumulator& stream) {
        stream << "<function " << m_name << ">";
    }

    int32_t hash32(instance_ptr left) {
        Hash32Accumulator acc((int)getTypeCategory());

        acc.addRegister((uint64_t)mPyFunc);

        return acc.get();
    }

    void constructor(instance_ptr self) {
    }

    void destroy(instance_ptr self) {
    }

    void copy_constructor(instance_ptr self, instance_ptr other) {
    }

    void assign(instance_ptr self, instance_ptr other) {
    }

    const PyFunctionObject* getPyFunc() const {
        return mPyFunc;
    }

    const std::vector<Overload>& getOverloads() const {
        return mOverloads;
    }

    void addCompiledSpecialization(
                    long whichOverload,
                    compiled_code_entrypoint entrypoint,
                    Type* returnType,
                    const std::vector<Type*>& argTypes
                    ) {
        if (whichOverload < 0 || whichOverload >= mOverloads.size()) {
            throw std::runtime_error("Invalid overload index.");
        }

        mOverloads[whichOverload].addCompiledSpecialization(entrypoint, returnType, argTypes);
    }

private:
    PyFunctionObject* mPyFunc;
    std::vector<Overload> mOverloads;
};

