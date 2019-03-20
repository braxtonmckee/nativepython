#pragma once

#include <Python.h>

//scoped object to ensure we're not holding the GIL. If we've
//already released it, this is a no-op. Upon destruction, we
//reacquire it.
class PyEnsureGilReleased {
public:
    PyEnsureGilReleased();

    ~PyEnsureGilReleased();

private:
    bool m_should_reaquire;
};

//scoped object to ensure we're holding the GIL. If
//we released it in a thread above, this should reacquire it.
//if we already hold it, it should be a no-op
class PyEnsureGilAcquired {
public:
    PyEnsureGilAcquired();

    ~PyEnsureGilAcquired();

private:
    bool m_should_rerelease;
};

void assertHoldingTheGil();
