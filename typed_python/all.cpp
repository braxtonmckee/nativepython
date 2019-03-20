/*****
a .cpp file that includes all the other .cpp files associated
with the project implementation.

So much code overlaps between the various translation units
that there's no point splitting them up - its faster to just
compile the entire group all at once.
******/

#include "_types.cpp"
#include "_runtime.cpp"
#include "Instance.cpp"
#include "util.cpp"

#include "PyInstance.cpp"
#include "PyConstDictInstance.cpp"
#include "PyDictInstance.cpp"
#include "PyTupleOrListOfInstance.cpp"
#include "PyPointerToInstance.cpp"
#include "PyCompositeTypeInstance.cpp"
#include "PyClassInstance.cpp"
#include "PyAlternativeInstance.cpp"
#include "PyFunctionInstance.cpp"
#include "PyBoundMethodInstance.cpp"
#include "PyGilState.cpp"

#include "AlternativeType.cpp"
#include "BytesType.cpp"
#include "ClassType.cpp"
#include "CompositeType.cpp"
#include "ConcreteAlternativeType.cpp"
#include "DictType.cpp"
#include "ConstDictType.cpp"
#include "HeldClassType.cpp"
#include "OneOfType.cpp"
#include "PythonObjectOfTypeType.cpp"
#include "PythonSerializationContext.cpp"
#include "PythonSubclassType.cpp"
#include "StringType.cpp"
#include "TupleOrListOfType.cpp"
#include "Type.cpp"


