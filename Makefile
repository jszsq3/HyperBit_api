LIB_NAME = tripleBit
CFLAGS = -fPIC -Wall -w -g -fpermissive -O3 -std=c++11
# LFLAGS = -Llib -l$(LIB_NAME) -Wl,-rpath,\$$ORIGIN/../lib
LIBS := -lpthread -lboost_system -lboost_thread -lstdc++ -lsnappy -lleveldb -ltcmalloc -L/usr/local/lib -L/usr/local/include/cpprest -L/usr/lib/x86_64-linux-gnu/libboost_system.so 


SRC_FILES = $(wildcard TripleBit/*.cpp) $(wildcard TripleBit/util/*.cpp) $(wildcard TripleBit/comm/*.cpp) $(wildcard TripleBit/tools/*.cpp)
# $(wildcard TripleBitQuery/*.cpp)
O_FILES = $(patsubst %.cpp,build/%.o,$(SRC_FILES))

all: lib/lib$(LIB_NAME).so

# bin/%: build/%.o | lib/lib$(LIB_NAME).so
# 	mkdir -p $(@D)
# 	g++ $< $(LFLAGS) -o $@

build/%.o: %.cpp
	mkdir -p $(@D)
	g++  -c $(CFLAGS) $< -o $@ $(LIBS)

lib/lib$(LIB_NAME).so: $(O_FILES)
	mkdir -p $(@D)
	g++ -shared  $(CFLAGS) $^ -o $@ -lpthread $(LIBS)



clean:
	rm -rf  build lib
