#include <stdint.h>
#include <cmath>
#include <Python.h>
#include <iostream>
#include "AllTypes.hpp"
#include "StringType.hpp"
#include "BytesType.hpp"

thread_local const char* nativepython_cur_exception_value = nullptr;

const char* nativepython_runtime_get_stashed_exception() {
    return nativepython_cur_exception_value;
}

extern "C" {

    int64_t nativepython_runtime_string_cmp(StringType::layout* lhs, StringType::layout* rhs) {
        return StringType::cmpStatic(lhs, rhs);
    }

    StringType::layout* nativepython_runtime_string_concat(StringType::layout* lhs, StringType::layout* rhs) {
        return StringType::concatenate(lhs, rhs);
    }

    StringType::layout* nativepython_runtime_string_lower(StringType::layout* l) {
        return StringType::lower(l);
    }

    StringType::layout* nativepython_runtime_string_upper(StringType::layout* l) {
        return StringType::upper(l);
    }

    int64_t nativepython_runtime_string_find(StringType::layout* l, StringType::layout* sub, int64_t start, int64_t end) {
        return StringType::find(l, sub, start, end);
    }

    int64_t nativepython_runtime_string_find_2(StringType::layout* l, StringType::layout* sub) {
        return StringType::find_2(l, sub);
    }

    int64_t nativepython_runtime_string_find_3(StringType::layout* l, StringType::layout* sub, int64_t start) {
        return StringType::find_3(l, sub, start);
    }

    void nativepython_runtime_string_split(ListOfType::layout* outList, StringType::layout* l, StringType::layout* sep, int64_t max) {
        StringType::split(outList, l, sep, max);
    }

    void nativepython_runtime_string_split_2(ListOfType::layout* outList, StringType::layout* l) {
        StringType::split_2(outList, l);
    }

    void nativepython_runtime_string_split_3(ListOfType::layout* outList, StringType::layout* l, StringType::layout* sep) {
        StringType::split_3(outList, l, sep);
    }

    void nativepython_runtime_string_split_3max(ListOfType::layout* outList, StringType::layout* l, int64_t max) {
        StringType::split_3max(outList, l, max);
    }

    bool nativepython_runtime_string_isalpha(StringType::layout* l) {
        return StringType::isalpha(l);
    }

    bool nativepython_runtime_string_isalnum(StringType::layout* l) {
        return StringType::isalnum(l);
    }

    bool nativepython_runtime_string_isdecimal(StringType::layout* l) {
        return StringType::isdecimal(l);
    }

    bool nativepython_runtime_string_isdigit(StringType::layout* l) {
        return StringType::isdigit(l);
    }

    bool nativepython_runtime_string_islower(StringType::layout* l) {
        return StringType::islower(l);
    }

    bool nativepython_runtime_string_isnumeric(StringType::layout* l) {
        return StringType::isnumeric(l);
    }

    bool nativepython_runtime_string_isprintable(StringType::layout* l) {
        return StringType::isprintable(l);
    }

    bool nativepython_runtime_string_isspace(StringType::layout* l) {
        return StringType::isspace(l);
    }

    bool nativepython_runtime_string_istitle(StringType::layout* l) {
        return StringType::istitle(l);
    }

    bool nativepython_runtime_string_isupper(StringType::layout* l) {
        return StringType::isupper(l);
    }

    StringType::layout* nativepython_runtime_string_getitem_int64(StringType::layout* lhs, int64_t index) {
        return StringType::getitem(lhs, index);
    }

    StringType::layout* nativepython_runtime_string_from_utf8_and_len(const char* utf8_str, int64_t len) {
        return StringType::createFromUtf8(utf8_str, len);
    }

    Bytes::layout* nativepython_runtime_bytes_concat(Bytes::layout* lhs, Bytes::layout* rhs) {
        return Bytes::concatenate(lhs, rhs);
    }

    Bytes::layout* nativepython_runtime_bytes_from_ptr_and_len(const char* utf8_str, int64_t len) {
        return Bytes::createFromPtr(utf8_str, len);
    }

    //a temporary kluge to allow us to communicate between exception throw sites and
    //the native-code invoker until we have a more complete exception model built out.
    void nativepython_runtime_stash_const_char_ptr_for_exception(const char* m) {
        nativepython_cur_exception_value = m;
    }

    void nativepython_runtime_incref_pyobj(PyObject* p) {
        PyEnsureGilAcquired getTheGil;

        incref(p);
    }

    PyObject* nativepython_runtime_get_pyobj_None() {
        return Py_None;
    }

    PyObject* nativepython_runtime_getattr_pyobj(PyObject* p, const char* a) {
        PyEnsureGilAcquired getTheGil;

        PyObject* res = PyObject_GetAttrString(p, a);

        if (!res) {
            PyErr_PrintEx(0);
            throw std::runtime_error("python code threw an exception");
        }

        return res;
    }

    void nativepython_runtime_decref_pyobj(PyObject* p) {
        PyEnsureGilAcquired getTheGil;

        decref(p);
    }

    double nativepython_runtime_pow_float64_float64(double l, double r) {
        return std::pow(l,r);
    }

    int64_t nativepython_runtime_pow_int64_int64(int64_t l, int64_t r) {
        return std::pow(l,r);
    }

    int64_t nativepython_runtime_mod_int64_int64(int64_t l, int64_t r) {
        if (r == 1 || r == -1 || r == 0 || l == 0) {
            return 0;
        }

        if (r < 0) {
            if (l < 0) {
                return -((-l) % (-r));
            }
            return - ((-r) - ((l-1) % (-r) + 1) );
        }

        if (l < 0) {
            return r - ((-l-1) % r + 1);
        }

        return l % r;
    }

    double nativepython_runtime_mod_float64_float64(double l, double r) {
        if (r == 0 || l == 0) {
            return 0;
        }

        if (r < 0) {
            if (l < 0) {
                return -(fmod((-l), (-r)));
            }
            double res = fmod(l, r) + r;
            if (res - r <= 0.0)
                res -= r;
            return res;
        }

        if (l < 0) {
            double res = fmod(l, r) + r;
            if (res - r >= 0.0)
                res -= r;
            return res;
        }

        return fmod(l, r);
    }

    PyObject* nativepython_runtime_int_to_pyobj(int64_t i) {
        return PyLong_FromLong(i);
    }

    int64_t nativepython_runtime_pyobj_to_int(PyObject* i) {
        if (PyLong_Check(i)) {
            return PyLong_AsLong(i);
        }

        throw std::runtime_error("Couldn't convert to an int64.");
    }

}