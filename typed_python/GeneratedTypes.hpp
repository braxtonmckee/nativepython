class NamedTupleBoolAndInt {
private:
	static const int size1 = sizeof(int64_t);
	static const int size2 = sizeof(float);
	uint8_t data[size1 + size2];
public:
	int64_t& X() { return *(int64_t*)(data); }
	float& Y() { return *(float*)(data + size1); }
};

class NamedTupleBoolIntStr {
private:
	static const int size1 = sizeof(int64_t);
	static const int size2 = sizeof(float);
	static const int size3 = sizeof(String);
	uint8_t data[size1 + size2 + size3];
public:
	int64_t& X() { return *(int64_t*)(data); }
	float& Y() { return *(float*)(data + size1); }
	String& Z() { return *(String*)(data + size1 + size2); }
};

class NamedTupleListAndTupleOfStr {
private:
	static const int size1 = sizeof(ListOf<String>);
	static const int size2 = sizeof(TupleOf<String>);
	uint8_t data[size1 + size2];
public:
	ListOf<String>& items() { return *(ListOf<String>*)(data); }
	TupleOf<String>& elements() { return *(TupleOf<String>*)(data + size1); }
};

