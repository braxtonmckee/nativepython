#pragma once

template<class element_type>
class TypeDetails {
public:
    static Type* getType() { throw std::runtime_error("specialize me");}

    static const uint64_t bytecount = 0;
};

// Specialize this in specific Type files.

