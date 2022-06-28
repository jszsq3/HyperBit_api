#!/bin/bash
g++ -std=c++11 -g -O3 -fpermissive -lpthread -lboost_system -lboost_thread -L/usr/local/lib -ltcmalloc -lleveldb -lsnappy -shared -fPIC TripleQuery.cpp -o libtriplebitApi.so -L../lib -ltripleBit -Wl,-rpath ../lib
