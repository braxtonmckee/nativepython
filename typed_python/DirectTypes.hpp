#pragma once

#include "Type.hpp"

/* usage:
    str s("abc");
    str t;
    // perform String operations on mLayout
    if (String::islower(s.mLayout))
        ...
    // can convert String::layout* to str
    str a = str(StringConcat(s.mLayout, t.mLayout));

*/
/*
template <class T>
class DirectWrapper {
    public:
        T::layout* mLayout
        static T* getType() {
            static * t = T::Make();
            return t;
        }
        DirectWrapper(T& s) {
            mLayout = nullptr;
        }
        DirectWrapper(T::layout* l) {
            mLayout = nullptr;
        }
        ~DirectWrapper() {
            getType()->destroy((instance_ptr)&mLayout);
        }
        DirectWrapper(const DirectWrapper& other)
        {
	        getType()->copy_constructor(
               (instance_ptr)&mLayout,
               (instance_ptr)&other.mLayout
               );
        }

        DirectWrapper& operator=(const DirectWrapper& other)
        {
	        getType()->assign(
               (instance_ptr)&mLayout,
               (instance_ptr)&other.mLayout
               );
            return *this;
        }
};
typedef DirectWrapper<String> str;
*/
class str {
    public:
        String::layout* mLayout;
        static String* getType() {
            static String* t = String::Make();
            return t;
        }

        str() {
            mLayout = nullptr;
        }

        str(const char *pc) {
            getType()->constructor((instance_ptr)&mLayout, 1, strlen(pc), pc);
        }

        str(std::string& s) {
            getType()->constructor((instance_ptr)&mLayout, 1, s.length(), s.data());
        }

        str(String::layout* l) {
	        getType()->assign(
               (instance_ptr)&mLayout,
               (instance_ptr)&l
               );
        }

        ~str() {
            String::destroyStatic((instance_ptr)&mLayout);
        }

        str(const str& other)
        {
	        getType()->copy_constructor(
               (instance_ptr)&mLayout,
               (instance_ptr)&other.mLayout
               );
        }

        str& operator=(const str& other)
        {
	        getType()->assign(
               (instance_ptr)&mLayout,
               (instance_ptr)&other.mLayout
               );
            return *this;
        }

        template<class buf_t>
        void serialize(instance_ptr self, buf_t& buffer) {
            getType()->serialize<buf_t>((instance_ptr)&mLayout, buffer);
        }

        template<class buf_t>
        void deserialize(instance_ptr self, buf_t& buffer) {
            getType()->serialize<buf_t>((instance_ptr)&mLayout, buffer);
        }
};

template<class element_type>
class TypeDetails {
public:
    static Type* getType() { throw std::runtime_error("specialize me");}
    
    static const uint64_t bytecount = 0;
};

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
class TypeDetails<bool> {
public:
    static Type* getType() { return Bool::Make(); }

    static const uint64_t bytecount = sizeof(bool);
};
template<>
class TypeDetails<double> {
public:
    static Type* getType() { return Float64::Make(); }

    static const uint64_t bytecount = sizeof(double);
};

template<>
class TypeDetails<float> {
public:
    static Type* getType() { return Float32::Make(); }

    static const uint64_t bytecount = sizeof(float);
};

template<>
class TypeDetails<str> {
public:
    static Type* getType() {
        static Type* t = String::Make();
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

    //static ListOf<element_type> fromPython(PyObject* p) {
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

    void append(const element_type& e) {
        getType()->append((instance_ptr)&mLayout, (instance_ptr)&e);
    }

    template<class buf_t>
    void serialize(instance_ptr self, buf_t& buffer) {
        getType()->serialize<buf_t>((instance_ptr)&mLayout, buffer);
    }

    template<class buf_t>
    void deserialize(instance_ptr self, buf_t& buffer) {
        getType()->serialize<buf_t>((instance_ptr)&mLayout, buffer);
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
    void serialize(instance_ptr self, buf_t& buffer) {
        TupleOf::serialize<buf_t>((instance_ptr)&mLayout, buffer);
    }

    template<class buf_t>
    void deserialize(instance_ptr self, buf_t& buffer) {
        TupleOf::serialize<buf_t>((instance_ptr)&mLayout, buffer);
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

// some generated NamedTuples, from
//gen_named_tuple_type("NamedTupleBoolAndInt", X="int64_t", Y="float")
class NamedTupleBoolAndInt {
private:
	static const int size1 = sizeof(int64_t);
	static const int size2 = sizeof(float);
	uint8_t data[size1 + size2];
public:
	int64_t& X() { return *(int64_t*)(data); }
	float& Y() { return *(float*)(data + size1); }
};

//gen_named_tuple_type("NamedTupleBoolIntStr", X="int64_t", Y="float", Z="str")
class NamedTupleBoolIntStr {
private:
	static const int size1 = sizeof(int64_t);
	static const int size2 = sizeof(float);
	static const int size3 = sizeof(str);
	uint8_t data[size1 + size2 + size3];
public:
	int64_t& X() { return *(int64_t*)(data); }
	float& Y() { return *(float*)(data + size1); }
	str& Z() { return *(str*)(data + size1 + size2); }
};

//gen_named_tuple_type("NamedTupleListAndTupleOfStr", items="ListOf<str>", elements="TupleOf<str>")
class NamedTupleListAndTupleOfStr {
private:
	static const int size1 = sizeof(ListOf<str>);
	static const int size2 = sizeof(TupleOf<str>);
	uint8_t data[size1 + size2];
public:
	ListOf<str>& items() { return *(ListOf<str>*)(data); }
	TupleOf<str>& elements() { return *(TupleOf<str>*)(data + size1); }
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


void split(ListOf<String>* out, const String* s, const String* sep) {

}
 
   
   

*/
