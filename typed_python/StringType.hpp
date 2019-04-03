#pragma once

#include "Type.hpp"

class StringType : public Type {
public:
    class layout {
    public:
        std::atomic<int64_t> refcount;
        int32_t hash_cache;
        int32_t pointcount;
        int32_t bytes_per_codepoint; //1 implies
        uint8_t data[];

        uint8_t* eltPtr(int64_t i) {
            return data + i * bytes_per_codepoint;
        }
    };

    StringType() : Type(TypeCategory::catString)
    {
        m_name = "String";
        m_is_default_constructible = true;
        m_size = sizeof(void*);
    }

    static int64_t bytesPerCodepointRequiredForUtf8(const uint8_t* utf8Str, int64_t length);

    static void decodeUtf8To(uint8_t* target, uint8_t* utf8Str, int64_t bytes_per_codepoint, int64_t length);

    //return an increffed layout containing a copy of lhs at the desired number of codepoints
    static layout* upgradeCodePoints(layout* lhs, int32_t newBytesPerCodepoint);

    //return an increffed concatenated layout of lhs and rhs
    static layout* concatenate(layout* lhs, layout* rhs);

    //return an increffed lowercase conversion layout of l
    static layout* lower(layout *l);

    //return an increffed uppercase conversion layout of l
    static layout* upper(layout *l);

    //return the lowest index in the string where substring sub is found within l[start, end]
    static int64_t find(layout* l, layout* sub, int64_t start, int64_t end);
    static void split(ListOfType::layout *outList, layout* l, layout* sep, int64_t max);
    static void split_3(ListOfType::layout *outList, layout* l, int64_t max);

    static bool isalpha(layout *l);
    static bool isalnum(layout *l);
    static bool isdecimal(layout *l);
    static bool isdigit(layout *l);
    static bool islower(layout *l);
    static bool isnumeric(layout *l);
    static bool isprintable(layout *l);
    static bool isspace(layout *l);
    static bool istitle(layout *l);
    static bool isupper(layout *l);

    //return an increffed string containing the one codepoint at 'offset'. this function
    //will correctly map negative indices, but performs no other boundschecking.
    static layout* getitem(layout* lhs, int64_t offset);
    static layout* getsubstr(layout* lhs, int64_t start, int64_t stop);

    //return an increffed string containing the data from the utf-encoded string
    static layout* createFromUtf8(const char* utfEncodedString, int64_t len);

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


    static StringType* Make() { static StringType res; return &res; }

    template<class buf_t>
    void serialize(instance_ptr self, buf_t& buffer) {
        buffer.write_uint32(count(self));
        buffer.write_uint8(bytes_per_codepoint(self));
        buffer.write_bytes(eltPtr(self,0), bytes_per_codepoint(self) * count(self));
    }

    template<class buf_t>
    void deserialize(instance_ptr self, buf_t& buffer) {
        int32_t ct = buffer.read_uint32();
        uint8_t bytes_per = buffer.read_uint8();

        if (bytes_per != 1 && bytes_per != 2 && bytes_per != 4) {
            throw std::runtime_error("Corrupt data (bytes per unicode character): "
                + std::to_string(bytes_per) + " " + std::to_string(ct) + ". pos is " + std::to_string(buffer.pos()));
        }

        if (!buffer.canConsume(ct)) {
            throw std::runtime_error("Corrupt data (stringsize)");
        }

        constructor(self, bytes_per, ct, nullptr);

        if (ct) {
            buffer.read_bytes(eltPtr(self,0), bytes_per * ct);
        }
    }

    int32_t hash32(instance_ptr left);

    bool cmp(instance_ptr left, instance_ptr right, int pyComparisonOp);

    static char cmpStatic(layout* left, layout* right);

    void constructor(instance_ptr self, int64_t bytes_per_codepoint, int64_t count, const char* data) const;

    void repr(instance_ptr self, ReprAccumulator& stream);

    instance_ptr eltPtr(instance_ptr self, int64_t i) const;

    int64_t bytes_per_codepoint(instance_ptr self) const;

    int64_t count(instance_ptr self) const;

    void constructor(instance_ptr self) {
        *(layout**)self = 0;
    }

    static void destroyStatic(instance_ptr self);

    void destroy(instance_ptr self);

    void copy_constructor(instance_ptr self, instance_ptr other);

    void assign(instance_ptr self, instance_ptr other);
};

