#pragma once

//the pattern of using 'check' to explicitly enumerate all of
//our subclasses by TypeCategory requires that if you include
//want to use any Type, you need to know about all of them.
#include "Type.hpp"
#include "RegisterTypes.hpp"
#include "ForwardType.hpp"
#include "OneOfType.hpp"
#include "CompositeType.hpp"
#include "PointerToType.hpp"
#include "TupleOrListOfType.hpp"
#include "ConstDictType.hpp"
#include "DictType.hpp"
#include "NoneType.hpp"
#include "Instance.hpp"
#include "StringType.hpp"
#include "BytesType.hpp"
#include "ValueType.hpp"
#include "AlternativeType.hpp"
#include "ConcreteAlternativeType.hpp"
#include "PythonSubclassType.hpp"
#include "PythonObjectOfTypeType.hpp"
#include "FunctionType.hpp"
#include "HeldClassType.hpp"
#include "ClassType.hpp"
#include "BoundMethodType.hpp"

#include "DirectTypes.hpp"
