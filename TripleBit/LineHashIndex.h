/*
 * LineHashIndex.h
 *
 *  Created on: Nov 15, 2010
 *      Author: root
 */
#ifndef LINEHASHINDEX_H_
#define LINEHASHINDEX_H_

class MemoryBuffer;
class ChunkManager;
class MMapBuffer;

#include "TripleBit.h"

class LineHashIndex {
public:
	struct chunkMetaData{
		ID minx;    //The minIDx of a chunk
		ID miny;		//The minIDy of a chunk
		chunkMetaData(){}
		chunkMetaData(ID minx, ID miny): minx(minx), miny(miny){}
	};

	enum IndexType {
		SUBJECT_INDEX, OBJECT_INDEX
	};
private:
	MemoryBuffer* idTable;
	ChunkManager& chunkManager;
	IndexType indexType;
	uchar* lineHashIndexBase; //used to do update

public:
	//some useful thing about the chunkManager
	const uchar *startPtr, *endPtr;
	vector<pair<ID, ID>> chunkMeta;

public:
	LineHashIndex(ChunkManager& _chunkManager, IndexType index_type);
	virtual ~LineHashIndex();
	int lowerfind(const pair<ID, ID>& target);
	int upperfind(const pair<ID, ID>& target);
	static LineHashIndex* load(ChunkManager& manager, IndexType index_type);
};

#endif
