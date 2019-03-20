#pragma once

#include <string.h>

/******************
HashValue

Stores a 160-bit hash.
******************/

class HashValue {
public:
    HashValue() {
        for (long i = 0; i < 5; i++) {
            m_digest[i] = 0;
        }
    }

    HashValue(const char* bytes) {
        strncpy((char*)m_digest, bytes, 20);
    }

    bool operator<(const HashValue& other) const {
        for (long i = 0; i < 5; i++) {
            if (m_digest[i] < other.m_digest[i]) {
                return true;
            }
            if (m_digest[i] > other.m_digest[i]) {
                return false;
            }
        }

        return false;
    }

    bool operator==(const HashValue& other) const {
        for (long i = 0; i < 5; i++) {
            if (m_digest[i] != other.m_digest[i]) {
                return false;
            }
        }

        return true;
    }

private:
    uint32_t m_digest[5];
};
