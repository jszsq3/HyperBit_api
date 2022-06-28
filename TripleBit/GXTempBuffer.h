/*
 * GXTempBuffer.h
 *
 *  Created on: 2013-7-10
 *      Author: root
 */

#ifndef TEMPBUFFER_H_
#define TEMPBUFFER_H_

#include "TripleBit.h"
#include "ThreadPool.h"
#include "util/atomic.hpp"

 //�����洢��������ݣ���ʱ�ڴ�
class GXTempBuffer {
public:
	GXChunkTriple* buffer;
	int pos; //buffer's index

	size_t usedSize;
	size_t totalSize;
	GXTempBuffer * next;

	//static triplebit::atomic<size_t> count;
private:
	ID predicateID;
	ID chunkID;
	bool soType;
public:
	ID getPredicateID(){
		return predicateID;
	}
	ID getChunkID(){
		return chunkID;
	}

	bool getSOType(){
		return soType;
	}
	Status insertTriple(ID subjectID, ID objectID,ID timeID);
	void Print();
	Status sort(bool soType);
	void uniqe();
	Status clear();
	GXChunkTriple& operator[](const size_t index);
	bool isEquals(GXChunkTriple* lTriple, GXChunkTriple* rTriple);
	bool isFull() {
		return usedSize >= totalSize;
	}
	bool isEmpty() {
		return usedSize == 0;
	}
	size_t getSize() const {
		return usedSize;
	}
	GXChunkTriple* getBuffer() const {
		return buffer;
	}

	GXChunkTriple* getEnd() {
		return getBuffer() + usedSize;
	}

	GXTempBuffer();
	GXTempBuffer(ID predicateID, ID chunkID, bool soType);
	~GXTempBuffer();

};


//�����洢��ѯ���м���
class CXTempBuffer {
public:

	int IDCount;
	ID* buffer;
	int pos;
	size_t usedSize;
	size_t totalSize;

	//used to sort
	int sortKey;

public:
	Status insertID(ID idX, ID idY);
	void Print();
	Status sort();
	void uniqe();
	ID& operator[](const size_t index);
	Status clear();
	bool isFull() { return usedSize > totalSize - 2; }
	bool isEmpty() { return usedSize == 0; }
	size_t getSize() const {
		return usedSize / IDCount;
	}
	ID* getBuffer() const {
		return buffer;
	}

	ID* getEnd() {
		return getBuffer() + usedSize;
	}

	CXTempBuffer();
	~CXTempBuffer();

};
#endif /* TEMPBUFFER_H_ */
