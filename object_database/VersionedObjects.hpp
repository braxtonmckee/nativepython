#pragma once

#include "../typed_python/Type.hpp"
#include "VersionedObjectsOfType.hpp"

/*************

VersionedObjects stores a collection of TypedPython objects that are each
indexed by a pair of int64s (objectid, and fieldid) and a version.

We provide functionality to
* perform a fast lookup of values by (object,field,version) tuples
* discarding values below a given global version
* merge in data with new version numbers
* tag that data exists with a given version number but that's not loaded

*************/

class VersionedObjects {
public:


private:

};