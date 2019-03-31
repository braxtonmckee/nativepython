#pragma once

#include <stdint.h>
#include "HashValue.hpp"

enum { NO_TRANSACTION = -1 };
enum { NO_OBJECT = -1 };

typedef int64_t field_id;
typedef HashValue index_value;
typedef int64_t object_id;
typedef int64_t transaction_id;
