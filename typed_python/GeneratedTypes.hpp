// "NamedTupleBoolAndInt"
//    X=int64_t
//    Y=float
class NamedTupleBoolAndInt {
public:
    typedef int64_t X_type;
    typedef float Y_type;
    X_type& X() const { return *(X_type*)(data); }
    Y_type& Y() const { return *(Y_type*)(data + size1); }
private:
    static const int size1 = sizeof(X_type);
    static const int size2 = sizeof(Y_type);
    uint8_t data[size1 + size2];
public:
    NamedTupleBoolAndInt& operator = (const NamedTupleBoolAndInt& other) {
        X() = other.X();
        Y() = other.Y();
        return *this;
    }

    NamedTupleBoolAndInt(const NamedTupleBoolAndInt& other) {
        new (&X()) X_type(other.X());
        new (&Y()) Y_type(other.Y());
    }

    ~NamedTupleBoolAndInt() {
        X().~X_type();
        Y().~Y_type();
    }

    NamedTupleBoolAndInt() {
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
class TypeDetails<NamedTupleBoolAndInt> {
public:
    static Type* getType() {
        static Type* t = NamedTuple::Make({
                TypeDetails<NamedTupleBoolAndInt::X_type>::getType(),
                TypeDetails<NamedTupleBoolAndInt::Y_type>::getType()
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
        sizeof(NamedTupleBoolAndInt::X_type) +
        sizeof(NamedTupleBoolAndInt::Y_type);
};

// "NamedTupleBoolIntStr"
//    X=int64_t
//    Y=float
//    Z=String
class NamedTupleBoolIntStr {
public:
    typedef int64_t X_type;
    typedef float Y_type;
    typedef String Z_type;
    X_type& X() const { return *(X_type*)(data); }
    Y_type& Y() const { return *(Y_type*)(data + size1); }
    Z_type& Z() const { return *(Z_type*)(data + size1 + size2); }
private:
    static const int size1 = sizeof(X_type);
    static const int size2 = sizeof(Y_type);
    static const int size3 = sizeof(Z_type);
    uint8_t data[size1 + size2 + size3];
public:
    NamedTupleBoolIntStr& operator = (const NamedTupleBoolIntStr& other) {
        X() = other.X();
        Y() = other.Y();
        Z() = other.Z();
        return *this;
    }

    NamedTupleBoolIntStr(const NamedTupleBoolIntStr& other) {
        new (&X()) X_type(other.X());
        new (&Y()) Y_type(other.Y());
        new (&Z()) Z_type(other.Z());
    }

    ~NamedTupleBoolIntStr() {
        X().~X_type();
        Y().~Y_type();
        Z().~Z_type();
    }

    NamedTupleBoolIntStr() {
        bool initX = false;
        bool initY = false;
        bool initZ = false;
        try {
            new (&X()) X_type();
            initX = true;
            new (&Y()) Y_type();
            initY = true;
            new (&Z()) Z_type();
            initZ = true;
        } catch(...) {
            try {
                if (initX) X().~X_type();
                if (initY) Y().~Y_type();
                if (initZ) Z().~Z_type();
            } catch(...) {
            }
            throw;
        }
    }
};

template <>
class TypeDetails<NamedTupleBoolIntStr> {
public:
    static Type* getType() {
        static Type* t = NamedTuple::Make({
                TypeDetails<NamedTupleBoolIntStr::X_type>::getType(),
                TypeDetails<NamedTupleBoolIntStr::Y_type>::getType(),
                TypeDetails<NamedTupleBoolIntStr::Z_type>::getType()
            },{
                "X",
                "Y",
                "Z"
            });
        if (t->bytecount() != bytecount) {
            throw std::runtime_error("somehow we have the wrong bytecount!");
        }
        return t;
    }
    static const uint64_t bytecount = 
        sizeof(NamedTupleBoolIntStr::X_type) +
        sizeof(NamedTupleBoolIntStr::Y_type) +
        sizeof(NamedTupleBoolIntStr::Z_type);
};

// "NamedTupleListAndTupleOfStr"
//    items=ListOf<String>
//    elements=TupleOf<String>
class NamedTupleListAndTupleOfStr {
public:
    typedef ListOf<String> items_type;
    typedef TupleOf<String> elements_type;
    items_type& items() const { return *(items_type*)(data); }
    elements_type& elements() const { return *(elements_type*)(data + size1); }
private:
    static const int size1 = sizeof(items_type);
    static const int size2 = sizeof(elements_type);
    uint8_t data[size1 + size2];
public:
    NamedTupleListAndTupleOfStr& operator = (const NamedTupleListAndTupleOfStr& other) {
        items() = other.items();
        elements() = other.elements();
        return *this;
    }

    NamedTupleListAndTupleOfStr(const NamedTupleListAndTupleOfStr& other) {
        new (&items()) items_type(other.items());
        new (&elements()) elements_type(other.elements());
    }

    ~NamedTupleListAndTupleOfStr() {
        items().~items_type();
        elements().~elements_type();
    }

    NamedTupleListAndTupleOfStr() {
        bool inititems = false;
        bool initelements = false;
        try {
            new (&items()) items_type();
            inititems = true;
            new (&elements()) elements_type();
            initelements = true;
        } catch(...) {
            try {
                if (inititems) items().~items_type();
                if (initelements) elements().~elements_type();
            } catch(...) {
            }
            throw;
        }
    }
};

template <>
class TypeDetails<NamedTupleListAndTupleOfStr> {
public:
    static Type* getType() {
        static Type* t = NamedTuple::Make({
                TypeDetails<NamedTupleListAndTupleOfStr::items_type>::getType(),
                TypeDetails<NamedTupleListAndTupleOfStr::elements_type>::getType()
            },{
                "items",
                "elements"
            });
        if (t->bytecount() != bytecount) {
            throw std::runtime_error("somehow we have the wrong bytecount!");
        }
        return t;
    }
    static const uint64_t bytecount = 
        sizeof(NamedTupleListAndTupleOfStr::items_type) +
        sizeof(NamedTupleListAndTupleOfStr::elements_type);
};

// "NamedTupleTwoParts"
//    first=NamedTupleBoolAndInt
//    second=NamedTupleListAndTupleOfStr
class NamedTupleTwoParts {
public:
    typedef NamedTupleBoolAndInt first_type;
    typedef NamedTupleListAndTupleOfStr second_type;
    first_type& first() const { return *(first_type*)(data); }
    second_type& second() const { return *(second_type*)(data + size1); }
private:
    static const int size1 = sizeof(first_type);
    static const int size2 = sizeof(second_type);
    uint8_t data[size1 + size2];
public:
    NamedTupleTwoParts& operator = (const NamedTupleTwoParts& other) {
        first() = other.first();
        second() = other.second();
        return *this;
    }

    NamedTupleTwoParts(const NamedTupleTwoParts& other) {
        new (&first()) first_type(other.first());
        new (&second()) second_type(other.second());
    }

    ~NamedTupleTwoParts() {
        first().~first_type();
        second().~second_type();
    }

    NamedTupleTwoParts() {
        bool initfirst = false;
        bool initsecond = false;
        try {
            new (&first()) first_type();
            initfirst = true;
            new (&second()) second_type();
            initsecond = true;
        } catch(...) {
            try {
                if (initfirst) first().~first_type();
                if (initsecond) second().~second_type();
            } catch(...) {
            }
            throw;
        }
    }
};

template <>
class TypeDetails<NamedTupleTwoParts> {
public:
    static Type* getType() {
        static Type* t = NamedTuple::Make({
                TypeDetails<NamedTupleTwoParts::first_type>::getType(),
                TypeDetails<NamedTupleTwoParts::second_type>::getType()
            },{
                "first",
                "second"
            });
        if (t->bytecount() != bytecount) {
            throw std::runtime_error("somehow we have the wrong bytecount!");
        }
        return t;
    }
    static const uint64_t bytecount = 
        sizeof(NamedTupleTwoParts::first_type) +
        sizeof(NamedTupleTwoParts::second_type);
};

