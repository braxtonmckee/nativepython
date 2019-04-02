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

    int64_t nativepython_runtime_string_cmp(String::layout* lhs, String::layout* rhs) {
        return String::cmpStatic(lhs, rhs);
    }

    String::layout* nativepython_runtime_string_concat(String::layout* lhs, String::layout* rhs) {
        return String::concatenate(lhs, rhs);
    }

    String::layout* nativepython_runtime_string_lower(String::layout* l) {
        return String::lower(l);
    }

    String::layout* nativepython_runtime_string_upper(String::layout* l) {
        return String::upper(l);
    }

    int64_t nativepython_runtime_string_find(String::layout* l, String::layout* sub, int64_t start, int64_t end) {
        return String::find(l, sub, start, end);
    }

    int64_t nativepython_runtime_string_find_2(String::layout* l, String::layout* sub) {
        return String::find_2(l, sub);
    }

    int64_t nativepython_runtime_string_find_3(String::layout* l, String::layout* sub, int64_t start) {
        return String::find_3(l, sub, start);
    }

    void nativepython_runtime_string_split(ListOfType::layout* outList, String::layout* l, String::layout* sep, int64_t max) {
        String::split(outList, l, sep, max);
    }

    void nativepython_runtime_string_split_2(ListOfType::layout* outList, String::layout* l) {
        String::split_2(outList, l);
    }

    void nativepython_runtime_string_split_3(ListOfType::layout* outList, String::layout* l, String::layout* sep) {
        String::split_3(outList, l, sep);
    }

    void nativepython_runtime_string_split_3max(ListOfType::layout* outList, String::layout* l, int64_t max) {
        String::split_3max(outList, l, max);
    }

    bool nativepython_runtime_string_isalpha(String::layout* l) {
        return String::isalpha(l);
    }

    bool nativepython_runtime_string_isalnum(String::layout* l) {
        return String::isalnum(l);
    }

    bool nativepython_runtime_string_isdecimal(String::layout* l) {
        return String::isdecimal(l);
    }

    bool nativepython_runtime_string_isdigit(String::layout* l) {
        return String::isdigit(l);
    }

    bool nativepython_runtime_string_islower(String::layout* l) {
        return String::islower(l);
    }

    bool nativepython_runtime_string_isnumeric(String::layout* l) {
        return String::isnumeric(l);
    }

    bool nativepython_runtime_string_isprintable(String::layout* l) {
        return String::isprintable(l);
    }

    bool nativepython_runtime_string_isspace(String::layout* l) {
        return String::isspace(l);
    }

    bool nativepython_runtime_string_istitle(String::layout* l) {
        return String::istitle(l);
    }

    bool nativepython_runtime_string_isupper(String::layout* l) {
        return String::isupper(l);
    }

    String::layout* nativepython_runtime_string_getitem_int64(String::layout* lhs, int64_t index) {
        return String::getitem(lhs, index);
    }

    String::layout* nativepython_runtime_string_from_utf8_and_len(const char* utf8_str, int64_t len) {
        return String::createFromUtf8(utf8_str, len);
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