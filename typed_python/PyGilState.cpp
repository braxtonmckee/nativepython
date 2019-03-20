#include "PyGilState.hpp"

PyEnsureGilReleased::PyEnsureGilReleased() :
    m_should_reaquire(false)
{
    if (curPyThreadState == nullptr) {
        curPyThreadState = PyEval_SaveThread();
        m_should_reaquire = true;
    }
}

PyEnsureGilReleased::~PyEnsureGilReleased() {
    if (m_should_reaquire) {
        PyEval_RestoreThread(curPyThreadState);
        curPyThreadState = nullptr;
    }
}

PyEnsureGilAcquired::PyEnsureGilAcquired() :
    m_should_rerelease(false)
{
    if (curPyThreadState) {
        PyEval_RestoreThread(curPyThreadState);
        m_should_rerelease = true;
        curPyThreadState = nullptr;
    }
}

PyEnsureGilAcquired::~PyEnsureGilAcquired() {
    if (m_should_rerelease) {
        curPyThreadState = PyEval_SaveThread();
    }
}

void assertHoldingTheGil() {
    if (curPyThreadState) {
        throw std::runtime_error("We're not holding the gil!");
    }
}
