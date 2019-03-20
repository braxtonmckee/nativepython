##########################################################################
#  CONFIGURATION

# Path to python binary
PYTHON ?= $(shell which python3)

COMMIT ?= $(shell git rev-parse HEAD)

# Path to virtual environment
VIRTUAL_ENV ?= .venv

TP_SRC_PATH ?= typed_python
ODB_SRC_PATH ?= object_database

TP_BUILD_PATH ?= build/temp.linux-x86_64-3.6/typed_python
TP_LIB_PATH ?= build/lib.linux-x86_64-3.6/typed_python

ODB_BUILD_PATH ?= build/temp.linux-x86_64-3.6/object_database
ODB_LIB_PATH ?= build/lib.linux-x86_64-3.6/object_database

CPP_FLAGS = -pthread -DNDEBUG -g -fwrapv -O2 -Wall -g -fstack-protector-strong -Wformat -Werror=format-security -Wdate-time -D_FORTIFY_SOURCE=2 -fPIC -I/usr/include/python3.6m -I$(VIRTUAL_ENV)/include/python3.6m -std=c++14 -Wno-sign-compare -Wno-narrowing -Wno-unused-variable -Wno-int-in-bool-context
CPP_FLAGS += -I/usr/local/lib/python3.6/dist-packages/numpy/core/include

UNICODEPROPS = $(TP_SRC_PATH)/UnicodeProps.hpp
TP_O_FILES = $(TP_BUILD_PATH)/all.o
ODB_O_FILES = $(ODB_BUILD_PATH)/all.o

##########################################################################
#  MAIN RULES

.PHONY: install
install: $(VIRTUAL_ENV)
	. $(VIRTUAL_ENV)/bin/activate; \
		pip install pipenv==2018.11.26; \
		pipenv install --dev --deploy

.PHONY: test
test: testcert.cert testcert.key install
	. $(VIRTUAL_ENV)/bin/activate; \
		./test.py -s

.PHONY: lint
lint:
	flake8 --show-source

.PHONY: vlint
vlint: $(VIRTUAL_ENV)
	. $(VIRTUAL_ENV)/bin/activate; \
		make lint

.PHONY: lib
lib: typed_python/_types.cpython-36m-x86_64-linux-gnu.so object_database/_types.cpython-36m-x86_64-linux-gnu.so

.PHONY: docker-build
docker-build:
	rm -rf build
	rm -rf nativepython.egg-info
	docker build . -t nativepython/cloud:"$(COMMIT)"
	docker tag nativepython/cloud:"$(COMMIT)"  nativepython/cloud:latest

.PHONY: docker-push
docker-push:
	docker push nativepython/cloud:"$(COMMIT)"
	docker push nativepython/cloud:latest

.PHONY: docker-test
docker-test:
	#run unit tests in the debugger
	docker run -it --rm --privileged --entrypoint bash nativepython/cloud:"$(COMMIT)" -c "gdb -ex run --args python ./test.py -v -s"

.PHONY: docker-web
docker-web:
	#run a dummy webframework
	docker run -it --rm --publish 8000:8000 --entrypoint object_database_webtest nativepython/cloud:"$(COMMIT)"

.PHONY: unicodeprops
unicodeprops: ./unicodeprops.py
	$(PYTHON) ./unicodeprops.py > $(UNICODEPROPS)

.PHONY: clean
clean:
	rm -rf build/
	rm -rf nativepython.egg-info/
	rm -f nose.*.log
	rm -f typed_python/_types.cpython-*.so
	rm -f object_database/_types.cpython-*.so
	rm -f testcert.cert testcert.key
	rm -rf .venv


##########################################################################
#  HELPER RULES

$(VIRTUAL_ENV): $(PYTHON)
	virtualenv $(VIRTUAL_ENV) --python=$(PYTHON)

$(TP_BUILD_PATH)/all.o: $(TP_SRC_PATH)/*.hpp $(TP_SRC_PATH)/*.cpp
	$(CC) $(CPP_FLAGS) -c $(TP_SRC_PATH)/all.cpp $ -o $@

$(ODB_BUILD_PATH)/all.o: $(ODB_SRC_PATH)/*.hpp $(ODB_SRC_PATH)/*.cpp $(TP_SRC_PATH)/*.hpp
	$(CC) $(CPP_FLAGS) -c $(ODB_SRC_PATH)/all.cpp $ -o $@

typed_python/_types.cpython-36m-x86_64-linux-gnu.so: $(TP_LIB_PATH)/_types.cpython-36m-x86_64-linux-gnu.so
	cp $(TP_LIB_PATH)/_types.cpython-36m-x86_64-linux-gnu.so  typed_python

object_database/_types.cpython-36m-x86_64-linux-gnu.so: $(ODB_LIB_PATH)/_types.cpython-36m-x86_64-linux-gnu.so
	cp $(ODB_LIB_PATH)/_types.cpython-36m-x86_64-linux-gnu.so  object_database

$(TP_LIB_PATH)/_types.cpython-36m-x86_64-linux-gnu.so: $(TP_LIB_PATH) $(TP_BUILD_PATH) $(TP_O_FILES)
	$(CXX) -pthread -shared -Wl,-O1 -Wl,-Bsymbolic-functions -Wl,-Bsymbolic-functions -Wl,-z,relro -Wl,-Bsymbolic-functions -Wl,-z,relro -g -fstack-protector-strong -Wformat -Werror=format-security -Wdate-time -D_FORTIFY_SOURCE=2 \
		$(TP_O_FILES) \
		-o $(TP_LIB_PATH)/_types.cpython-36m-x86_64-linux-gnu.so

$(ODB_LIB_PATH)/_types.cpython-36m-x86_64-linux-gnu.so: $(ODB_LIB_PATH) $(ODB_BUILD_PATH) $(ODB_O_FILES)
	$(CXX) -pthread -shared -Wl,-O1 -Wl,-Bsymbolic-functions -Wl,-Bsymbolic-functions -Wl,-z,relro -Wl,-Bsymbolic-functions -Wl,-z,relro -g -fstack-protector-strong -Wformat -Werror=format-security -Wdate-time -D_FORTIFY_SOURCE=2 \
		$(ODB_O_FILES) \
		-o $(ODB_LIB_PATH)/_types.cpython-36m-x86_64-linux-gnu.so

$(TP_BUILD_PATH):
	mkdir -p $(TP_BUILD_PATH)

$(ODB_BUILD_PATH):
	mkdir -p $(ODB_BUILD_PATH)

$(TP_LIB_PATH):
	mkdir -p $(TP_LIB_PATH)

$(ODB_LIB_PATH):
	mkdir -p $(ODB_LIB_PATH)

testcert.cert testcert.key:
	openssl req -x509 -newkey rsa:2048 -keyout testcert.key -nodes \
		-out testcert.cert -sha256 -days 1000 \
		-subj '/C=US/ST=New York/L=New York/CN=localhost'




