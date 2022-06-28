/*
 * GXTempBuffer.cpp
 *
 *  Created on: 2013-7-10
 *      Author: root
 */

#include "GXTempBuffer.h"
#include "MemoryBuffer.h"
#include <math.h>
#include <pthread.h>

//triplebit::atomic<size_t> GXTempBuffer::count(0);

GXTempBuffer::GXTempBuffer() {
	// TODO Auto-generated constructor stub
	buffer = (GXChunkTriple*) malloc(TEMPBUFFER_INIT_PAGE_COUNT * getpagesize() * 1000);
	usedSize = 0;
	totalSize = TEMPBUFFER_INIT_PAGE_COUNT * getpagesize() * 1000 / sizeof(GXChunkTriple);
	pos = 0;
	next = NULL;
}

GXTempBuffer::GXTempBuffer(ID predicateID, ID chunkID, bool soType):predicateID(predicateID), chunkID(chunkID), soType(soType) {
	// TODO Auto-generated constructor stub
	buffer = (GXChunkTriple*) malloc(TEMPBUFFER_INIT_PAGE_COUNT * getpagesize() * 1000);
	usedSize = 0;
	totalSize = TEMPBUFFER_INIT_PAGE_COUNT * getpagesize() * 1000 / sizeof(GXChunkTriple);
	pos = 0;
	next = NULL;
}

GXTempBuffer::~GXTempBuffer() {
	// TODO Auto-generated destructor stub
	if (buffer != NULL)
		free(buffer);
	buffer = NULL;
}

Status GXTempBuffer::insertTriple(ID subjectID, ID objectID,ID timeID) {
	buffer[pos].subjectID = subjectID;
	buffer[pos].objectID = objectID;
	buffer[pos].timeID = timeID;
	pos++;
	usedSize++;
	return OK;
}

Status GXTempBuffer::clear() {
	pos = 0;
	usedSize = 0;
	return OK;
}

GXChunkTriple& GXTempBuffer::operator[](const size_t index) {
	if (index >= usedSize) {
		return buffer[0];
	}
	return buffer[index];
}

void GXTempBuffer::Print() {
	for (int i = 0; i < usedSize; ++i) {
		cout << "subjectID:" << buffer[i].subjectID;
		cout << " objectID:" << buffer[i].objectID << " ";
	}
	cout << endl;
}

int cmpByS(const void* lhs, const void* rhs) {
	GXChunkTriple* lTriple = (GXChunkTriple*)lhs;
	GXChunkTriple* rTriple = (GXChunkTriple*)rhs;
	if (lTriple->subjectID != rTriple->subjectID) {
		return lTriple->subjectID - rTriple->subjectID;
	}
	else {
		return lTriple->objectID - rTriple->objectID;
	}
}

int cmpByO(const void* lhs, const void* rhs) {
	GXChunkTriple* lTriple = (GXChunkTriple*)lhs;
	GXChunkTriple* rTriple = (GXChunkTriple*)rhs;
	if (lTriple->objectID != rTriple->objectID) {
		return lTriple->objectID - rTriple->objectID;
	}
	else {
		return lTriple->subjectID - rTriple->subjectID;
	}
}

Status GXTempBuffer::sort(bool soType) {
	if (soType == ORDERBYS) {
		qsort(buffer, usedSize, sizeof(GXChunkTriple), cmpByS);
	} else if (soType == ORDERBYO) {
		qsort(buffer, usedSize, sizeof(GXChunkTriple), cmpByO);
	}

	return OK;
}

bool GXTempBuffer::isEquals(GXChunkTriple* lTriple, GXChunkTriple* rTriple) {
	if (lTriple->subjectID == rTriple->subjectID && lTriple->objectID == rTriple->objectID ) {
		return true;
	}
	return false;
}

void GXTempBuffer::uniqe() {
	if (usedSize <= 1)
		return;
	GXChunkTriple *lastPtr, *currentPtr, *endPtr;
	lastPtr = currentPtr = buffer;
	endPtr = getEnd();
	currentPtr++;
	while (currentPtr < endPtr) {
		if (isEquals(lastPtr, currentPtr)) {
			currentPtr++;
		} else {
			lastPtr++;
			*lastPtr = *currentPtr;
			currentPtr++;
		}
	}
	usedSize = lastPtr - buffer + 1;
}



CXTempBuffer::CXTempBuffer() {
	// TODO Auto-generated constructor stub
	buffer = (ID*)malloc(TEMPBUFFER_INIT_PAGE_COUNT * getpagesize());
	usedSize = 0;
	totalSize = TEMPBUFFER_INIT_PAGE_COUNT * getpagesize() / sizeof(ID);
	pos = 0;

	IDCount = 2;
	sortKey = 0;
}

CXTempBuffer::~CXTempBuffer() {
	// TODO Auto-generated destructor stub
	if (buffer != NULL)
		free(buffer);
	buffer = NULL;
}

ID& CXTempBuffer::operator [](const size_t index)
{
	if (index > (usedSize / IDCount))
		return buffer[0];
	return buffer[index * IDCount + sortKey];
}

Status CXTempBuffer::insertID(ID idX, ID idY)
{
	buffer[pos++] = idX;
	buffer[pos++] = idY;
	usedSize++;
	usedSize++;
	return OK;
}

Status CXTempBuffer::clear()
{
	pos = 0;
	usedSize = 0;
	return OK;
}

void CXTempBuffer::Print()
{
	for (size_t i = 0; i < usedSize; ++++i)
	{
		cout << "x:" << buffer[i];
		cout << " y:" << buffer[i + 1] << " ";
	}
	cout << endl;
}

int cmp(const void* lhs, const void* rhs)
{
	ID xlhs = *(ID*)lhs;
	ID ylhs = *(ID*)(lhs + sizeof(ID));
	ID xrhs = *(ID*)rhs;
	ID yrhs = *(ID*)(rhs + sizeof(ID));
	if (xlhs != xrhs) return xlhs - xrhs;
	else return ylhs - yrhs;
}

Status CXTempBuffer::sort()
{
	qsort(buffer, getSize(), sizeof(ID) * IDCount, cmp);
	return OK;
}

void CXTempBuffer::uniqe()
{
	if (usedSize <= 2) return;
	ID* lastPtr, * currentPtr, * endPtr;
	lastPtr = currentPtr = buffer;
	endPtr = getEnd();
	currentPtr += IDCount;
	while (currentPtr < endPtr)
	{
		if (*lastPtr == *currentPtr && *(lastPtr + 1) == *(currentPtr + 1))
		{
			currentPtr += IDCount;
		}
		else
		{
			lastPtr += IDCount;
			*lastPtr = *currentPtr;
			*(lastPtr + 1) = *(currentPtr + 1);
			currentPtr += 2;
		}
	}
	usedSize = lastPtr + IDCount - buffer;
}