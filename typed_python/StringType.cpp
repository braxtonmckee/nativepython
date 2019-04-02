#include "AllTypes.hpp"
#include  <iostream>
#include "UnicodeProps.hpp"
using namespace std;

String::layout* String::upgradeCodePoints(layout* lhs, int32_t newBytesPerCodepoint) {
    if (!lhs) {
        return lhs;
    }

    if (newBytesPerCodepoint == lhs->bytes_per_codepoint) {
        lhs->refcount++;
        return lhs;
    }

    int64_t new_byteCount = sizeof(layout) + lhs->pointcount * newBytesPerCodepoint;

    layout* new_layout = (layout*)malloc(new_byteCount);
    new_layout->refcount = 1;
    new_layout->hash_cache = -1;
    new_layout->bytes_per_codepoint = newBytesPerCodepoint;
    new_layout->pointcount = lhs->pointcount;

    if (lhs->bytes_per_codepoint == 1 && new_layout->bytes_per_codepoint == 2) {
        for (long i = 0; i < lhs->pointcount; i++) {
            ((uint16_t*)new_layout->data)[i] = ((uint8_t*)lhs->data)[i];
        }
    }
    if (lhs->bytes_per_codepoint == 1 && new_layout->bytes_per_codepoint == 4) {
        for (long i = 0; i < lhs->pointcount; i++) {
            ((uint32_t*)new_layout->data)[i] = ((uint8_t*)lhs->data)[i];
        }
    }
    if (lhs->bytes_per_codepoint == 2 && new_layout->bytes_per_codepoint == 4) {
        for (long i = 0; i < lhs->pointcount; i++) {
            ((uint32_t*)new_layout->data)[i] = ((uint16_t*)lhs->data)[i];
        }
    }

    return new_layout;
}

String::layout* String::concatenate(layout* lhs, layout* rhs) {
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

    if (lhs->bytes_per_codepoint < rhs->bytes_per_codepoint) {
        layout* newLayout = upgradeCodePoints(lhs, rhs->bytes_per_codepoint);

        layout* result = concatenate(newLayout, rhs);
        String::destroyStatic((instance_ptr)&newLayout);
        return result;
    }

    if (rhs->bytes_per_codepoint < lhs->bytes_per_codepoint) {
        layout* newLayout = upgradeCodePoints(rhs, lhs->bytes_per_codepoint);

        layout* result = concatenate(lhs, newLayout);
        String::destroyStatic((instance_ptr)&newLayout);
        return result;
    }

    //they're the same
    int64_t new_byteCount = sizeof(layout) + (rhs->pointcount + lhs->pointcount) * lhs->bytes_per_codepoint;

    layout* new_layout = (layout*)malloc(new_byteCount);
    new_layout->refcount = 1;
    new_layout->hash_cache = -1;
    new_layout->bytes_per_codepoint = lhs->bytes_per_codepoint;
    new_layout->pointcount = lhs->pointcount + rhs->pointcount;

    memcpy(new_layout->data, lhs->data, lhs->pointcount * lhs->bytes_per_codepoint);
    memcpy(new_layout->data + lhs->pointcount * lhs->bytes_per_codepoint,
        rhs->data, rhs->pointcount * lhs->bytes_per_codepoint);

    return new_layout;
}

String::layout* String::lower(layout *l) {
    if (!l) {
        return l;
    }

    int64_t new_byteCount = sizeof(layout) + l->pointcount * l->bytes_per_codepoint;
    layout* new_layout = (layout*)malloc(new_byteCount);
    new_layout->refcount = 1;
    new_layout->hash_cache = -1;
    new_layout->bytes_per_codepoint = l->bytes_per_codepoint;
    new_layout->pointcount = l->pointcount;

    if (l->bytes_per_codepoint == 1) {
        for (uint8_t *src = l->data, *dest = new_layout->data, *end = src + l->pointcount; src < end; ) {
            *dest++ = (uint8_t)tolower(*src++);
        }
    }
    else if (l->bytes_per_codepoint == 2) {
        for (uint16_t *src = (uint16_t *)l->data, *dest = (uint16_t *)new_layout->data, *end = src + l->pointcount; src < end; ) {
            *dest++ = (uint16_t)towlower(*src++);
        }
    }
    else if (l->bytes_per_codepoint == 4) {
        for (uint32_t *src = (uint32_t *)l->data, *dest = (uint32_t *)new_layout->data, *end = src + l->pointcount; src < end; ) {
            *dest++ = (uint32_t)towlower(*src++);
        }
    }

    return new_layout;
}

String::layout* String::upper(layout *l) {
    if (!l) {
        return l;
    }

    int64_t new_byteCount = sizeof(layout) + l->pointcount * l->bytes_per_codepoint;
    layout* new_layout = (layout*)malloc(new_byteCount);
    new_layout->refcount = 1;
    new_layout->hash_cache = -1;
    new_layout->bytes_per_codepoint = l->bytes_per_codepoint;
    new_layout->pointcount = l->pointcount;

    if (l->bytes_per_codepoint == 1) {
        for (uint8_t *src = l->data, *dest = new_layout->data, *end = src + l->pointcount; src < end; ) {
            *dest++ = (uint8_t)toupper(*src++);
        }
    }
    else if (l->bytes_per_codepoint == 2) {
        for (uint16_t *src = (uint16_t *)l->data, *dest = (uint16_t *)new_layout->data, *end = src + l->pointcount; src < end; ) {
            *dest++ = (uint16_t)towupper(*src++);
        }
    }
    else if (l->bytes_per_codepoint == 4) {
        for (uint32_t *src = (uint32_t *)l->data, *dest = (uint32_t *)new_layout->data, *end = src + l->pointcount; src < end; ) {
            *dest++ = (uint32_t)towupper(*src++);
        }
    }

    return new_layout;
}

int64_t String::find_2(layout *l, layout *sub) {
    return find(l, sub, 0, l ? l->pointcount : 0);
}

int64_t String::find_3(layout *l, layout *sub, int64_t start) {
    return find(l, sub, start, l ? l->pointcount : 0);
}

uint32_t getpoint(String::layout *l, uint64_t i) {
    if (l->bytes_per_codepoint == 1)
        return ((uint8_t *)l->data)[i];
    else if (l->bytes_per_codepoint == 2)
        return ((uint16_t *)l->data)[i];
    else if (l->bytes_per_codepoint == 4)
        return ((uint32_t *)l->data)[i];
    return 0;
}

int64_t String::find(layout *l, layout *sub, int64_t start, int64_t stop) {
    if (!l || !l->pointcount) {
        if (!sub || !sub->pointcount)
            return start > 0 ? -1 : 0;
        return -1;
    }
    if (start < 0) {
        start += l->pointcount;
        if (start < 0) start = 0;
    }
    if (stop < 0) {
        stop += l->pointcount;
        if (stop < 0) stop = 0;
    }
    if (stop < start || start > l->pointcount)
        return -1;
    if (!sub || !sub->pointcount)
        return start >= 0 ? start : 0;

    if (stop > l->pointcount)
        stop = l->pointcount;
    stop -= (sub->pointcount - 1);
    if (start < 0 || stop < 0 || start >= stop || sub->pointcount > l->pointcount || start > l->pointcount - sub->pointcount)
        return -1;

    for (int64_t i = start; i < stop; i++) {
        bool match = true;
        for (int64_t j = 0; j < sub->pointcount; j++) {
            if (getpoint(l, i+j) != getpoint(sub, j)) {
                match = false;
                break;
            }
        }
        if (match)
            return i;
    }

    return -1;
}

void String::split_2(ListOfType::layout* outList, layout* l) {
    split_3max(outList, l, -1);
}

void String::split_3max(ListOfType::layout* outList, layout* l, int64_t max) {
    if (!outList)
        throw std::invalid_argument("missing return argument");
    static auto listofstring = ListOfType::Make(String::Make());
    listofstring->resize((instance_ptr)&outList, 0, 0);

    if (!l || !l->pointcount)
        ;
    else if (max == 0) {
        // unexpected standard behavior:
        //   "   abc   ".split(maxsplit=0) = "abc   "  *** not "abc" nor "   abc   " ***
        int64_t cur = 0;
        while (cur < l->pointcount && uprops[getpoint(l, cur)] & Uprops_SPACE)
            cur++;
        String::layout* remainder = String::getsubstr(l, cur, l->pointcount);
        listofstring->append((instance_ptr)&outList, (instance_ptr)&remainder);
    }
    else {
        int64_t cur = 0;
        int64_t count = 0;
        while (cur < l->pointcount) {
            int64_t match = cur;
            while (!(uprops[getpoint(l, match)] & Uprops_SPACE)) {
                match++;
                if (match >= l->pointcount) {
                    match = -1;
                    break;
                }
            }
            if (match < 0)
                break;
            if (cur != match) {
                String::layout* piece = String::getsubstr(l, cur, match);
                listofstring->append((instance_ptr)&outList, (instance_ptr)&piece);
                count++;
            }
            cur = match + 1;
            if (max >= 0 && count >= max)
                break;
        }
        while (cur < l->pointcount && uprops[getpoint(l, cur)] & Uprops_SPACE)
            cur++;
        if (cur < l->pointcount) {
            String::layout* remainder = String::getsubstr(l, cur, l->pointcount);
            listofstring->append((instance_ptr)&outList, (instance_ptr)&remainder);
        }
    }
}

void String::split_3(ListOfType::layout* outList, layout* l, layout* sep) {
    split(outList, l, sep, -1);
}

void String::split(ListOfType::layout* outList, layout* l, layout* sep, int64_t max) {
    if (!outList)
        throw std::invalid_argument("missing return argument");
    static auto listofstring = ListOfType::Make(String::Make());
    listofstring->resize((instance_ptr)&outList, 0, 0);

    if (!sep || !sep->pointcount)
        throw std::invalid_argument("ValueError: empty separator");
    else if (!l || !l->pointcount || max == 0)
        listofstring->append((instance_ptr)&outList, (instance_ptr)&l);
    else {
        int64_t cur = 0;
        int64_t count = 0;
        while (cur < l->pointcount) {
            int64_t match = String::find_3(l, sep, cur);
            if (match < 0)
                break;
            String::layout* piece = String::getsubstr(l, cur, match);

            listofstring->append((instance_ptr)&outList, (instance_ptr)&piece);
            cur = match + sep->pointcount;
            count++;
            if (max >= 0 && count >= max)
                break;
        }
        String::layout* remainder = String::getsubstr(l, cur, l->pointcount);
        listofstring->append((instance_ptr)&outList, (instance_ptr)&remainder);
    }
}

bool String::isalpha(layout *l) {
    if (!l || !l->pointcount)
        return false;
    for (int64_t i = 0; i < l->pointcount; i++)
        if (!(uprops[getpoint(l, i)] & Uprops_ALPHA))
            return false;
    return true;
}

bool String::isalnum(layout *l) {
    if (!l || !l->pointcount)
        return false;
    for (int64_t i = 0; i < l->pointcount; i++) {
        auto flags = uprops[getpoint(l, i)];
        if (!(flags & Uprops_ALPHA)
            && !(flags & Uprops_NUMERIC)
            && !(flags & Uprops_DECIMAL)
            && !(flags & Uprops_DIGIT)) // for completeness; looks to me like DECIMAL and DIGIT are subsets of NUMERIC
            return false;
    }
    return true;
}

bool String::isdecimal(layout *l) {
    if (!l || !l->pointcount)
        return false;
    for (int64_t i = 0; i < l->pointcount; i++)
        if (!(uprops[getpoint(l, i)] & Uprops_DECIMAL))
            return false;
    return true;
}

bool String::isdigit(layout *l) {
    if (!l || !l->pointcount)
        return false;
    for (int64_t i = 0; i < l->pointcount; i++) {
        auto flags = uprops[getpoint(l, i)];
        if (!(flags & Uprops_DECIMAL)
            && !(flags & Uprops_DIGIT)) // For completeness; looks to me like DECIMAL is a subset of DIGIT
            return false;
    }
    return true;
}

bool String::islower(layout *l) {
    if (!l || !l->pointcount)
        return false;
    bool found_one = false;
    for (int64_t i = 0; i < l->pointcount; i++) {
        auto flags = uprops[getpoint(l, i)];
        if (flags & Uprops_UPPER)
            return false;
        if (flags & Uprops_TITLE)
            return false;
        if (flags & Uprops_LOWER)
            found_one = true;
    }
    return found_one;
}

bool String::isnumeric(layout *l) {
    if (!l || !l->pointcount)
        return false;
    for (int64_t i = 0; i < l->pointcount; i++) {
        auto flags = uprops[getpoint(l, i)];
        if (!(flags & Uprops_DECIMAL)
            && !(flags & Uprops_DIGIT)
            && !(flags & Uprops_NUMERIC))
            return false;
    }
    return true;
}

bool String::isprintable(layout *l) {
    if (!l || !l->pointcount)
        return true;
    for (int64_t i = 0; i < l->pointcount; i++)
        if (!(uprops[getpoint(l, i)] & Uprops_PRINTABLE))
            return false;
    return true;
}

bool String::isspace(layout *l) {
    if (!l || !l->pointcount)
        return false;
    for (int64_t i = 0; i < l->pointcount; i++)
        if (!(uprops[getpoint(l, i)] & Uprops_SPACE))
            return false;
    return true;
}

bool String::istitle(layout *l) {
    if (!l || !l->pointcount)
        return false;
    bool last_cased = false;
    bool found_one = false;
    for (int64_t i = 0; i < l->pointcount; i++) {
        auto flags = uprops[getpoint(l, i)];
        bool upper = flags & Uprops_UPPER;
        bool lower = flags & Uprops_LOWER;
        bool title = flags & Uprops_TITLE;
        if (upper && last_cased)
            return false;
        if (lower && !last_cased)
            return false;
        if (title && last_cased)
            return false;
        last_cased = upper || lower || title;
        if (last_cased)
            found_one = true;
    }

    return found_one;
}

bool String::isupper(layout *l) {
    if (!l || !l->pointcount)
        return false;
    bool found_one = false;
    for (int64_t i = 0; i < l->pointcount; i++) {
        auto flags = uprops[getpoint(l, i)];
        if (flags & Uprops_LOWER)
            return false;
        if (flags & Uprops_TITLE)
            return false;
        if (flags & Uprops_UPPER)
            found_one = true;
    }
    return found_one;
}

String::layout* String::getitem(layout* lhs, int64_t offset) {
    if (!lhs) {
        return lhs;
    }

    if (lhs->pointcount == 1) {
        lhs->refcount++;
        return lhs;
    }

    if (offset < 0) {
        offset += lhs->pointcount;
    }

    int64_t new_byteCount = sizeof(layout) + 1 * lhs->bytes_per_codepoint;

    layout* new_layout = (layout*)malloc(new_byteCount);
    new_layout->refcount = 1;
    new_layout->hash_cache = -1;
    new_layout->bytes_per_codepoint = lhs->bytes_per_codepoint;
    new_layout->pointcount = 1;

    //we could figure out if we can represent this with a smaller encoding.
    if (new_layout->bytes_per_codepoint == 1) {
        ((uint8_t*)new_layout->data)[0] = ((uint8_t*)lhs->data)[offset];
    }
    if (new_layout->bytes_per_codepoint == 2) {
        ((uint16_t*)new_layout->data)[0] = ((uint16_t*)lhs->data)[offset];
    }
    if (new_layout->bytes_per_codepoint == 4) {
        ((uint32_t*)new_layout->data)[0] = ((uint32_t*)lhs->data)[offset];
    }

    return new_layout;
}

String::layout* String::getsubstr(layout* l, int64_t start, int64_t stop) {
    if (!l) {
        return l;
    }

    if (start < 0) {
        start += l->pointcount;
        if (start < 0) start = 0;
    }
    if (stop < 0) {
        stop += l->pointcount;
        if (stop < 0) stop = 0;
    }
    if (stop < start || start > l->pointcount)
        stop = start = 0;

    if (stop > l->pointcount)
        stop = l->pointcount;

//    if (!alwaysAlloc && start==0  && stop == l->pointcount) {
//        l->refcount++;
//        return l;
//    }

    size_t datalength = stop - start;
    size_t datasize = datalength * l->bytes_per_codepoint;
    int64_t new_byteCount = sizeof(layout) + datasize;

    layout* new_layout = (layout*)malloc(new_byteCount);
    new_layout->refcount = 1;
    new_layout->hash_cache = -1;
    new_layout->bytes_per_codepoint = l->bytes_per_codepoint;
    new_layout->pointcount = datalength;

    memcpy(new_layout->data, l->data + start * l->bytes_per_codepoint, datalength);

    return new_layout;
}


int64_t String::bytesPerCodepointRequiredForUtf8(const uint8_t* utf8Str, int64_t length) {
    int64_t bytes_per_codepoint = 1;
    while (length > 0) {
        if (utf8Str[0] >> 7 == 0) {
            //one byte encoded here
            length -= 1;
            utf8Str++;
        }

        if (utf8Str[0] >> 5 == 0b110) {
            length -= 1;
            utf8Str+=2;
            bytes_per_codepoint = std::max<int64_t>(2, bytes_per_codepoint);
        }

        if (utf8Str[0] >> 4 == 0b1110) {
            length -= 1;
            utf8Str += 3;
            bytes_per_codepoint = std::max<int64_t>(2, bytes_per_codepoint);
        }

        if (utf8Str[0] >> 3 == 0b11110) {
            length -= 1;
            utf8Str+=4;
            bytes_per_codepoint = std::max<int64_t>(4, bytes_per_codepoint);
        }
    }
    return bytes_per_codepoint;
}

template<class T>
void decodeUtf8ToTyped(T* target, uint8_t* utf8Str, int64_t bytes_per_codepoint, int64_t length) {
    while (length > 0) {
        if ((utf8Str[0] >> 7) == 0) {
            //one byte encoded here
            target[0] = utf8Str[0];

            length -= 1;
            target++;
            utf8Str++;
        }
        else if ((utf8Str[0] >> 5) == 0b110) {
            target[0] = (uint32_t(utf8Str[0] & 0b11111) << 6) + uint32_t(utf8Str[1] & 0b111111);
            length -= 1;
            target++;
            utf8Str+=2;
        }
        else if ((utf8Str[0] >> 4) == 0b1110) {
            target[0] =
                (uint32_t(utf8Str[0] & 0b1111) << 12) +
                (uint32_t(utf8Str[1] & 0b111111) << 6) +
                 uint32_t(utf8Str[2] & 0b111111);
            length -= 1;
            target++;
            utf8Str+=3;
        }
        else if ((utf8Str[0] >> 3) == 0b11110) {
            target[0] =
                (uint32_t(utf8Str[0] & 0b111) << 18) +
                (uint32_t(utf8Str[1] & 0b111111) << 12) +
                (uint32_t(utf8Str[2] & 0b111111) << 6) +
                 uint32_t(utf8Str[3] & 0b111111);
            length -= 1;
            target++;
            utf8Str+=4;
        }
    }
}

void String::decodeUtf8To(uint8_t* target, uint8_t* utf8Str, int64_t bytes_per_codepoint, int64_t length) {
    if (bytes_per_codepoint == 1) {
        decodeUtf8ToTyped(target, utf8Str, bytes_per_codepoint, length);
    }
    if (bytes_per_codepoint == 2) {
        decodeUtf8ToTyped((uint16_t*)target, utf8Str, bytes_per_codepoint, length);
    }
    if (bytes_per_codepoint == 4) {
        decodeUtf8ToTyped((uint32_t*)target, utf8Str, bytes_per_codepoint, length);
    }
}

String::layout* String::createFromUtf8(const char* utfEncodedString, int64_t length) {
    int64_t bytes_per_codepoint = bytesPerCodepointRequiredForUtf8((uint8_t*)utfEncodedString, length);

    int64_t new_byteCount = sizeof(layout) + length * bytes_per_codepoint;

    layout* new_layout = (layout*)malloc(new_byteCount);
    new_layout->refcount = 1;
    new_layout->hash_cache = -1;
    new_layout->bytes_per_codepoint = bytes_per_codepoint;
    new_layout->pointcount = length;

    if (bytes_per_codepoint == 1) {
        memcpy(new_layout->data, utfEncodedString, length);
    } else {
        decodeUtf8To((uint8_t*)new_layout->data, (uint8_t*)utfEncodedString, bytes_per_codepoint, length);
    }

    return new_layout;
}

int32_t String::hash32(instance_ptr left) {
    if (!(*(layout**)left)) {
        return 0x12345;
    }

    if ((*(layout**)left)->hash_cache == -1) {
        Hash32Accumulator acc((int)getTypeCategory());
        acc.addBytes(eltPtr(left, 0), bytes_per_codepoint(left) * count(left));
        (*(layout**)left)->hash_cache = acc.get();
        if ((*(layout**)left)->hash_cache == -1) {
            (*(layout**)left)->hash_cache = -2;
        }
    }

    return (*(layout**)left)->hash_cache;
}

template<class T1, class T2>
char typedArrayCompare(T1* l, T2* r, size_t count) {
    for (size_t i = 0; i < count; i++) {
        uint32_t lv = l[i];
        uint32_t rv = r[i];
        if (lv < rv) {
            return -1;
        }
        if (lv > rv) {
            return 1;
        }
    }
    return 0;
}

bool String::cmp(instance_ptr left, instance_ptr right, int pyComparisonOp) {
    return cmpResultToBoolForPyOrdering(pyComparisonOp, cmpStatic(*(layout**)left, *(layout**)right));
}

char String::cmpStatic(layout* left, layout* right) {
    if ( !left && !right ) {
        return 0;
    }
    if ( !left && right ) {
        return -1;
    }
    if ( left && !right ) {
        return 1;
    }

    int bytesPerLeft = left->bytes_per_codepoint;
    int bytesPerRight = right->bytes_per_codepoint;
    int commonCount = std::min(left->pointcount, right->pointcount);

    char res = 0;

    if (bytesPerLeft == 1 && bytesPerRight == 1) {
        res = byteCompare(left->data, right->data, bytesPerLeft * commonCount);
    } else if (bytesPerLeft == 1 && bytesPerRight == 2) {
        res = typedArrayCompare((uint8_t*)left->data, (uint16_t*)right->data, commonCount);
    } else if (bytesPerLeft == 1 && bytesPerRight == 4) {
        res = typedArrayCompare((uint8_t*)left->data, (uint32_t*)right->data, commonCount);
    } else if (bytesPerLeft == 2 && bytesPerRight == 1) {
        res = typedArrayCompare((uint16_t*)left->data, (uint8_t*)right->data, commonCount);
    } else if (bytesPerLeft == 2 && bytesPerRight == 2) {
        res = typedArrayCompare((uint16_t*)left->data, (uint16_t*)right->data, commonCount);
    } else if (bytesPerLeft == 2 && bytesPerRight == 4) {
        res = typedArrayCompare((uint16_t*)left->data, (uint32_t*)right->data, commonCount);
    } else if (bytesPerLeft == 4 && bytesPerRight == 1) {
        res = typedArrayCompare((uint32_t*)left->data, (uint8_t*)right->data, commonCount);
    } else if (bytesPerLeft == 4 && bytesPerRight == 2) {
        res = typedArrayCompare((uint32_t*)left->data, (uint16_t*)right->data, commonCount);
    } else if (bytesPerLeft == 4 && bytesPerRight == 4) {
        res = typedArrayCompare((uint32_t*)left->data, (uint32_t*)right->data, commonCount);
    } else {
        throw std::runtime_error("Nonsensical bytes-per-codepoint");
    }

    if (res) {
        return res;
    }

    if (left->pointcount < right->pointcount) {
        return -1;
    }

    if (left->pointcount > right->pointcount) {
        return 1;
    }

    return 0;
}

void String::constructor(instance_ptr self, int64_t bytes_per_codepoint, int64_t count, const char* data) const {
    if (count == 0) {
        *(layout**)self = nullptr;
        return;
    }

    (*(layout**)self) = (layout*)malloc(sizeof(layout) + count * bytes_per_codepoint);

    (*(layout**)self)->bytes_per_codepoint = bytes_per_codepoint;
    (*(layout**)self)->pointcount = count;
    (*(layout**)self)->hash_cache = -1;
    (*(layout**)self)->refcount = 1;

    if (data) {
        ::memcpy((*(layout**)self)->data, data, count * bytes_per_codepoint);
    }
}

void String::repr(instance_ptr self, ReprAccumulator& stream) {
    //as if it were bytes, which is totally wrong...
    stream << "\"";
    long bytes = count(self);
    uint8_t* base = eltPtr(self,0);

    static char hexDigits[] = "0123456789abcdef";

    for (long k = 0; k < bytes;k++) {
        if (base[k] == '"') {
            stream << "\\\"";
        } else if (base[k] == '\\') {
            stream << "\\\\";
        } else if (isprint(base[k])) {
            stream << base[k];
        } else {
            stream << "\\x" << hexDigits[base[k] / 16] << hexDigits[base[k] % 16];
        }
    }

    stream << "\"";
}

instance_ptr String::eltPtr(instance_ptr self, int64_t i) const {
    const static char* emptyPtr = "";

    if (*(layout**)self == nullptr) {
        return (instance_ptr)emptyPtr;
    }

    return (*(layout**)self)->eltPtr(i);
}

int64_t String::bytes_per_codepoint(instance_ptr self) const {
    if (*(layout**)self == nullptr) {
        return 1;
    }

    return (*(layout**)self)->bytes_per_codepoint;
}

int64_t String::count(instance_ptr self) const {
    if (*(layout**)self == nullptr) {
        return 0;
    }

    return (*(layout**)self)->pointcount;
}

void String::destroy(instance_ptr self) {
    destroyStatic(self);
}

void String::destroyStatic(instance_ptr self) {
    if (!*(layout**)self) {
        return;
    }

    if ((*(layout**)self)->refcount.fetch_sub(1) == 1) {
        free((*(layout**)self));
    }
}

void String::copy_constructor(instance_ptr self, instance_ptr other) {
    (*(layout**)self) = (*(layout**)other);
    if (*(layout**)self) {
        (*(layout**)self)->refcount++;
    }
}

void String::assign(instance_ptr self, instance_ptr other) {
    layout* old = (*(layout**)self);

    (*(layout**)self) = (*(layout**)other);

    if (*(layout**)self) {
        (*(layout**)self)->refcount++;
    }

    destroy((instance_ptr)&old);
}

bool Bytes::isBinaryCompatibleWithConcrete(Type* other) {
    if (other->getTypeCategory() != m_typeCategory) {
        return false;
    }

    return true;
}

void Bytes::repr(instance_ptr self, ReprAccumulator& stream) {
    stream << "b" << "'";
    long bytes = count(self);
    uint8_t* base = eltPtr(self,0);

    static char hexDigits[] = "0123456789abcdef";

    for (long k = 0; k < bytes;k++) {
        if (base[k] == '\'') {
            stream << "\\'";
        } else if (base[k] == '\r') {
            stream << "\\r";
        } else if (base[k] == '\n') {
            stream << "\\n";
        } else if (base[k] == '\t') {
            stream << "\\t";
        } else if (base[k] == '\\') {
            stream << "\\\\";
        } else if (isprint(base[k])) {
            stream << base[k];
        } else {
            stream << "\\x" << hexDigits[base[k] / 16] << hexDigits[base[k] % 16];
        }
    }

    stream << "'";
}

