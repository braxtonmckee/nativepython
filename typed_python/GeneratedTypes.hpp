// "NamedTupleTwoStrings"
//    X=String
//    Y=String
class NamedTupleTwoStrings {
public:
    typedef String X_type;
    typedef String Y_type;
    X_type& X() const { return *(X_type*)(data); }
    Y_type& Y() const { return *(Y_type*)(data + size1); }
private:
    static const int size1 = sizeof(X_type);
    static const int size2 = sizeof(Y_type);
    uint8_t data[size1 + size2];
public:
    NamedTupleTwoStrings& operator = (const NamedTupleTwoStrings& other) {
        X() = other.X();
        Y() = other.Y();
        return *this;
    }

    NamedTupleTwoStrings(const NamedTupleTwoStrings& other) {
        new (&X()) X_type(other.X());
        new (&Y()) Y_type(other.Y());
    }

    ~NamedTupleTwoStrings() {
        X().~X_type();
        Y().~Y_type();
    }

    NamedTupleTwoStrings() {
        bool initX = false;
        bool initY = false;
        try {
            new (&X()) X_type();
            initX = true;
            new (&Y()) Y_type();
            initY = true;
        } catch(...) {
            try {
                if (initX) X().~X_type();
                if (initY) Y().~Y_type();
            } catch(...) {
            }
            throw;
        }
    }
};

template <>
class TypeDetails<NamedTupleTwoStrings> {
public:
    static Type* getType() {
        static Type* t = NamedTuple::Make({
                TypeDetails<NamedTupleTwoStrings::X_type>::getType(),
                TypeDetails<NamedTupleTwoStrings::Y_type>::getType()
            },{
                "X",
                "Y"
            });
        if (t->bytecount() != bytecount) {
            throw std::runtime_error("somehow we have the wrong bytecount!");
        }
        return t;
    }
    static const uint64_t bytecount = 
        sizeof(NamedTupleTwoStrings::X_type) +
        sizeof(NamedTupleTwoStrings::Y_type);
};

// "NamedTupleIntFloat"
//    a=OneOf<int64_t, double>
//    b=double
class NamedTupleIntFloat {
public:
    typedef OneOf<int64_t, double> a_type;
    typedef double b_type;
    a_type& a() const { return *(a_type*)(data); }
    b_type& b() const { return *(b_type*)(data + size1); }
private:
    static const int size1 = sizeof(a_type);
    static const int size2 = sizeof(b_type);
    uint8_t data[size1 + size2];
public:
    NamedTupleIntFloat& operator = (const NamedTupleIntFloat& other) {
        a() = other.a();
        b() = other.b();
        return *this;
    }

    NamedTupleIntFloat(const NamedTupleIntFloat& other) {
        new (&a()) a_type(other.a());
        new (&b()) b_type(other.b());
    }

    ~NamedTupleIntFloat() {
        a().~a_type();
        b().~b_type();
    }

    NamedTupleIntFloat() {
        bool inita = false;
        bool initb = false;
        try {
            new (&a()) a_type();
            inita = true;
            new (&b()) b_type();
            initb = true;
        } catch(...) {
            try {
                if (inita) a().~a_type();
                if (initb) b().~b_type();
            } catch(...) {
            }
            throw;
        }
    }
};

template <>
class TypeDetails<NamedTupleIntFloat> {
public:
    static Type* getType() {
        static Type* t = NamedTuple::Make({
                TypeDetails<NamedTupleIntFloat::a_type>::getType(),
                TypeDetails<NamedTupleIntFloat::b_type>::getType()
            },{
                "a",
                "b"
            });
        if (t->bytecount() != bytecount) {
            throw std::runtime_error("somehow we have the wrong bytecount!");
        }
        return t;
    }
    static const uint64_t bytecount = 
        sizeof(NamedTupleIntFloat::a_type) +
        sizeof(NamedTupleIntFloat::b_type);
};

// "NamedTupleBoolListOfInt"
//    X=bool
//    Y=ListOf<int64_t>
class NamedTupleBoolListOfInt {
public:
    typedef bool X_type;
    typedef ListOf<int64_t> Y_type;
    X_type& X() const { return *(X_type*)(data); }
    Y_type& Y() const { return *(Y_type*)(data + size1); }
private:
    static const int size1 = sizeof(X_type);
    static const int size2 = sizeof(Y_type);
    uint8_t data[size1 + size2];
public:
    NamedTupleBoolListOfInt& operator = (const NamedTupleBoolListOfInt& other) {
        X() = other.X();
        Y() = other.Y();
        return *this;
    }

    NamedTupleBoolListOfInt(const NamedTupleBoolListOfInt& other) {
        new (&X()) X_type(other.X());
        new (&Y()) Y_type(other.Y());
    }

    ~NamedTupleBoolListOfInt() {
        X().~X_type();
        Y().~Y_type();
    }

    NamedTupleBoolListOfInt() {
        bool initX = false;
        bool initY = false;
        try {
            new (&X()) X_type();
            initX = true;
            new (&Y()) Y_type();
            initY = true;
        } catch(...) {
            try {
                if (initX) X().~X_type();
                if (initY) Y().~Y_type();
            } catch(...) {
            }
            throw;
        }
    }
};

template <>
class TypeDetails<NamedTupleBoolListOfInt> {
public:
    static Type* getType() {
        static Type* t = NamedTuple::Make({
                TypeDetails<NamedTupleBoolListOfInt::X_type>::getType(),
                TypeDetails<NamedTupleBoolListOfInt::Y_type>::getType()
            },{
                "X",
                "Y"
            });
        if (t->bytecount() != bytecount) {
            throw std::runtime_error("somehow we have the wrong bytecount!");
        }
        return t;
    }
    static const uint64_t bytecount = 
        sizeof(NamedTupleBoolListOfInt::X_type) +
        sizeof(NamedTupleBoolListOfInt::Y_type);
};

// "NamedTupleAttrAndValues"
//    attributes=TupleOf<String>
//    values=TupleOf<int64_t>
class NamedTupleAttrAndValues {
public:
    typedef TupleOf<String> attributes_type;
    typedef TupleOf<int64_t> values_type;
    attributes_type& attributes() const { return *(attributes_type*)(data); }
    values_type& values() const { return *(values_type*)(data + size1); }
private:
    static const int size1 = sizeof(attributes_type);
    static const int size2 = sizeof(values_type);
    uint8_t data[size1 + size2];
public:
    NamedTupleAttrAndValues& operator = (const NamedTupleAttrAndValues& other) {
        attributes() = other.attributes();
        values() = other.values();
        return *this;
    }

    NamedTupleAttrAndValues(const NamedTupleAttrAndValues& other) {
        new (&attributes()) attributes_type(other.attributes());
        new (&values()) values_type(other.values());
    }

    ~NamedTupleAttrAndValues() {
        attributes().~attributes_type();
        values().~values_type();
    }

    NamedTupleAttrAndValues() {
        bool initattributes = false;
        bool initvalues = false;
        try {
            new (&attributes()) attributes_type();
            initattributes = true;
            new (&values()) values_type();
            initvalues = true;
        } catch(...) {
            try {
                if (initattributes) attributes().~attributes_type();
                if (initvalues) values().~values_type();
            } catch(...) {
            }
            throw;
        }
    }
};

template <>
class TypeDetails<NamedTupleAttrAndValues> {
public:
    static Type* getType() {
        static Type* t = NamedTuple::Make({
                TypeDetails<NamedTupleAttrAndValues::attributes_type>::getType(),
                TypeDetails<NamedTupleAttrAndValues::values_type>::getType()
            },{
                "attributes",
                "values"
            });
        if (t->bytecount() != bytecount) {
            throw std::runtime_error("somehow we have the wrong bytecount!");
        }
        return t;
    }
    static const uint64_t bytecount = 
        sizeof(NamedTupleAttrAndValues::attributes_type) +
        sizeof(NamedTupleAttrAndValues::values_type);
};

