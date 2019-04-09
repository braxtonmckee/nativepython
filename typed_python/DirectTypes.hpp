#pragma once

#include "Type.hpp"
#include "PyInstance.hpp"

class String {
    public:
        static StringType* getType() {
            static StringType* t = StringType::Make();
            return t;
        }

        String() {
            mLayout = nullptr;
        }

        explicit String(const char *pc) {
            getType()->constructor((instance_ptr)&mLayout, 1, strlen(pc), pc);
        }

        explicit String(std::string& s) {
            getType()->constructor((instance_ptr)&mLayout, 1, s.length(), s.data());
        }

        explicit String(StringType::layout* l) {
            getType()->assign(
               (instance_ptr)&mLayout,
               (instance_ptr)&l
               );
        }

        ~String() {
            StringType::destroyStatic((instance_ptr)&mLayout);
        }

        String(const String& other)
        {
            getType()->copy_constructor(
               (instance_ptr)&mLayout,
               (instance_ptr)&other.mLayout
               );
        }

        String& operator=(const String& other)
        {
            getType()->assign(
               (instance_ptr)&mLayout,
               (instance_ptr)&other.mLayout
               );
            return *this;
        }

        template<class buf_t>
        void serialize(buf_t& buffer) {
            getType()->serialize<buf_t>((instance_ptr)&mLayout, buffer);
        }

        template<class buf_t>
        void deserialize(buf_t& buffer) {
            getType()->deserialize<buf_t>((instance_ptr)&mLayout, buffer);
        }

        StringType::layout* getLayout() const {
            return mLayout;
        }
    private:
        StringType::layout* mLayout;
};

template<>
class TypeDetails<String> {
public:
    static Type* getType() {
        static Type* t = StringType::Make();
        if (t->bytecount() != bytecount) {
            throw std::runtime_error("somehow we have the wrong bytecount!");
        }
        return t;
    }

    static const uint64_t bytecount = sizeof(void*);
};

//templates for TupleOf, ListOf, Dict, ConstDict, OneOf
template<class element_type>
class ListOf {
public:
    static ListOfType* getType() {
        static ListOfType* t = ListOfType::Make(TypeDetails<element_type>::getType());

        return t;
    }

    static ListOf<element_type> fromPython(PyObject* p) {
        ListOfType::layout* l = nullptr;
        PyInstance::copyConstructFromPythonInstance(getType(), (instance_ptr)&l, p, true);
    }

    element_type& operator[](int64_t offset) {
       return *(element_type*)((uint8_t*)mLayout->data + TypeDetails<element_type>::bytecount * offset);
    }
 
    const element_type& operator[](int64_t offset) const {
       return *(element_type*)((uint8_t*)mLayout->data + TypeDetails<element_type>::bytecount * offset);
    }

    size_t size() const {
        return !mLayout ? 0 : sizeof(*mLayout) + TypeDetails<element_type>::bytecount * mLayout->count;
    }

    void append(const element_type& e) {
        getType()->append((instance_ptr)&mLayout, (instance_ptr)&e);
    }

    template<class buf_t>
    void serialize(buf_t& buffer) {
        getType()->serialize<buf_t>((instance_ptr)&mLayout, buffer);
    }

    template<class buf_t>
    void deserialize(buf_t& buffer) {
        getType()->deserialize<buf_t>((instance_ptr)&mLayout, buffer);
    }

    ListOf() {
        mLayout = nullptr;
    }

    ~ListOf() {
        getType()->destroy((instance_ptr)&mLayout);
    }

    ListOf(const ListOf& other)
    {
        getType()->copy_constructor(
               (instance_ptr)&mLayout,
               (instance_ptr)&other.mLayout
               );
    }

    ListOf& operator=(const ListOf& other)
    {
        getType()->assign(
               (instance_ptr)&mLayout,
               (instance_ptr)&other.mLayout
               );
        return *this;
    }

private:
    ListOfType::layout* mLayout;
};

template<class element_type>
class TypeDetails<ListOf<element_type>> {
public:
    static Type* getType() {
        static Type* t = ListOfType::Make(TypeDetails<element_type>::getType());
        if (t->bytecount() != bytecount) {
            throw std::runtime_error("somehow we have the wrong bytecount!");
        }
        return t;
    }

    static const uint64_t bytecount = sizeof(void*);
};

template<class element_type>
class TupleOf {
public:
    static TupleOfType* getType() {
        static TupleOfType* t = TupleOfType::Make(TypeDetails<element_type>::getType());

        return t;
    }

    //static TupleOf<element_type> fromPython(PyObject* p) {
        //pyInstance::copyConstructFromPythonInstance(Type* eltType, instance_ptr tgt, PyObject* pyRepresentation, bool isExplicit);
    //}

    element_type& operator[](int64_t offset) {
       return *(element_type*)((uint8_t*)mLayout->data + TypeDetails<element_type>::bytecount * offset);
    }

    const element_type& operator[](int64_t offset) const {
       return *(element_type*)((uint8_t*)mLayout->data + TypeDetails<element_type>::bytecount * offset);
    }

    size_t size() const {
        return !mLayout ? 0 : sizeof(*mLayout) + TypeDetails<element_type>::bytecount * mLayout->count;
    }

    template<class buf_t>
    void serialize(buf_t& buffer) {
        TupleOf::serialize<buf_t>((instance_ptr)&mLayout, buffer);
    }

    template<class buf_t>
    void deserialize(buf_t& buffer) {
        TupleOf::deserialize<buf_t>((instance_ptr)&mLayout, buffer);
    }

    TupleOf() {
        mLayout = nullptr;
    }

    ~TupleOf() {
        getType()->destroy((instance_ptr)&mLayout);
    }

    TupleOf(const TupleOf& other)
    {
        getType()->copy_constructor(
               (instance_ptr)&mLayout,
               (instance_ptr)&other.mLayout
               );
    }

    TupleOf& operator=(const TupleOf& other)
    {
        getType()->assign(
               (instance_ptr)&mLayout,
               (instance_ptr)&other.mLayout
               );
        return *this;
    }

private:
    TupleOfType::layout* mLayout;
};

template<class element_type>
class TypeDetails<TupleOf<element_type>> {
public:
    static Type* getType() {
        static Type* t = TupleOf<element_type>::getType();
        if (t->bytecount() != bytecount) {
            throw std::runtime_error("somehow we have the wrong bytecount!");
        }
        return t;
    }

    static const uint64_t bytecount = sizeof(void*);
};

template<class t1, class t2>
class OneOf {
public:
    class layout {
        uint8_t which;
        uint8_t data[std::max(TypeDetails<t1>::bytecount, TypeDetails<t2>::bytecount)];
    };
private:
    layout mLayout;
public:
    static OneOfType* getType() {
        static OneOfType* t = OneOfType::Make((std::vector<Type*>){TypeDetails<t1>::getType(), TypeDetails<t2>::getType()});

        return t;
    }

//    static OneOf<t1, t2> fromPython(PyObject* p) {
//        OneOfType::layout* l = nullptr;
//        PyInstance::copyConstructFromPythonInstance(getType(), (instance_ptr)&l, p, true);
//    }
//
    std::pair<Type*, instance_ptr> unwrap() {
        return getType()->unwrap((instance_ptr)&mLayout);
    }

    size_t size() const {
        return sizeof(layout);
    }

    template<class buf_t>
    void serialize(buf_t& buffer) {
        getType()->serialize<buf_t>((instance_ptr)&mLayout, buffer);
    }

    template<class buf_t>
    void deserialize(buf_t& buffer) {
        getType()->deserialize<buf_t>((instance_ptr)&mLayout, buffer);
    }

    OneOf() {
        getType()->constructor((instance_ptr)&mLayout);
    }

    ~OneOf() {
        getType()->destroy((instance_ptr)&mLayout);
    }

    OneOf(const OneOf& other)
    {
        getType()->copy_constructor(
               (instance_ptr)&mLayout,
               (instance_ptr)&other.mLayout
               );
    }

    OneOf& operator=(const OneOf& other)
    {
        getType()->assign(
               (instance_ptr)&mLayout,
               (instance_ptr)&other.mLayout
               );
        return *this;
    }
};

template<class t1, class t2>
class TypeDetails<OneOf<t1, t2>> {
public:
    static Type* getType() {
        static Type* t = OneOf<t1, t2>::getType();
        if (t->bytecount() != bytecount) {
            throw std::runtime_error("somehow we have the wrong bytecount!");
        }
        return t;
    }

    static const uint64_t bytecount = sizeof(int8_t) + std::max(TypeDetails<t1>::bytecount, TypeDetails<t2>::bytecount);
};

/*

//generate code for NamedTuple, Alternative, Class, etc.
class NamedTuple {
public:
   
};


So for NamedTuple(X=bool,Y=int).

class NamedTupleBoolAndInt {
Public:
    bool& X() { return *(bool*)data;
    int64_t& Y() { return *(int64_t*)(data+1);
Private:
    Uint8_t data[9];
}


# explicitly list all the named tuples, Alternatives, Classes you want
# to define.
typed_python_codegen(
    Blah=NamedTuple(X=int, Y=float),
    ServerToClientMessage=messages.ServerToClient,
    #if something is missing, this won’t compile
    ...
);



What would be ideal:

In python, define something like

ClientToServer = Alternative(
    "ClientToServer",
    TransactionData={
        "writes": ConstDict(ObjectFieldId, OneOf(None, bytes)),
        "set_adds": ConstDict(IndexId, TupleOf(ObjectId)),
        "set_removes": ConstDict(IndexId, TupleOf(ObjectId)),
        "key_versions": TupleOf(ObjectFieldId),
        "index_versions": TupleOf(IndexId),
        "transaction_guid": int
    },
    CompleteTransaction={
        "as_of_version": int,
        "transaction_guid": int
    },
    Heartbeat={},
    LoadLazyObject={ 'schema': str, 'typename': str, 'identity': ObjectId },
    Subscribe={
        'schema': str,
        'typename': OneOf(None, str),
        'fieldname_and_value': OneOf(None, Tuple(str, bytes)),
        'isLazy': bool  # load values when we first request them, instead of blocking on all the data.
    },
    Flush={'guid': int},
    Authenticate={'token': str}
)


Generate code like

contents = typed_python_codegen(
   ClientToServer=ClientToServer,
   ObjectFieldId=ObjectFieldId,
   ObjectId=ObjectId,
   TupleStrAndBytes=Tuple(str,bytes),
   ) 
With open(“messages.hpp”, “w”) as f:
    f.write(contents)

In c++ somewhere

#include “messages.hpp”



ClientToServer processAMessage(ClientToServer inMessage) {
   / ******
   Want to be able to:
Match on kind of client to server message
Produce new ones
Iterate over the dictionary and tuple of internals
etc.
}

*/
