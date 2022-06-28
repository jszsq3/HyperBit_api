/*
 * LineHashIndex.cpp
 *
 *  Created on: Nov 15, 2010
 *      Author: root
 */

#include "LineHashIndex.h"
#include "MMapBuffer.h"
#include "MemoryBuffer.h"
#include "BitmapBuffer.h"

#include <math.h>

LineHashIndex::LineHashIndex(ChunkManager& _chunkManager, IndexType index_type) : chunkManager(_chunkManager), indexType(index_type) {
	idTable = NULL; 
}

LineHashIndex::~LineHashIndex() {
	if (idTable != NULL) {
		delete idTable;
		idTable = NULL;
	}
	startPtr = NULL;
	endPtr = NULL;
	chunkMeta.clear();
}

//返回target所在的第一个块
int LineHashIndex::lowerfind(const pair<ID, ID>& target) {
	if (chunkMeta.size() == 0) {
		cout << "LineHashIndex::lowerfind error" << endl;
	}
	int l = 0;
	int r = chunkMeta.size() - 1;
	int mid = 0;
	while (l < r) {
		mid = (l + r) / 2;
		if (target > chunkMeta[mid]) {
			if (l + 1 == r) {
				if (target <= chunkMeta[r]) {
					return l;
				}
				else {
					return r;
				}
			}
			else {
				l = mid;
			}
		}
		else if (target == chunkMeta[mid]) {
			r = mid;
		}
		else {
			r = mid - 1;
		}
	}
	return l;
}

//返回target所在的最后一个块
int LineHashIndex::upperfind(const pair<ID, ID>& target) {
	int start = lowerfind(target);
	int end = chunkMeta.size() - 1;
	while (start <= end) {
		if (chunkMeta[start] > target) {
			return start == 0 ? 0 : (start - 1);
		}
		else {
			++start;
		}
	}
	if (start > end) {
		return end;
	}
}

//重新创建二分查找的索引
LineHashIndex* LineHashIndex::load(ChunkManager& manager, IndexType index_type) {
	LineHashIndex* index = new LineHashIndex(manager, index_type);
	const uchar* chunkBegin;
	const uchar* reader;
	register ID s;
	register ID o;
	index->startPtr = index->chunkManager.getStartPtr();
	index->endPtr = index->chunkManager.getEndPtr();


	if (index_type == SUBJECT_INDEX) {
		if (index->startPtr == index->endPtr) {
			index->chunkMeta.push_back({ 0, 0 });
			cout << "LineHashIndex::load error" << endl;
			return index;
		}
		reader = index->startPtr + sizeof(MetaData);
		Chunk::readID(reader, s);
		Chunk::readID(reader, o);
		index->chunkMeta.push_back({ s, o });
		chunkBegin = index->startPtr - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
		while (chunkBegin < index->endPtr) {
			reader = chunkBegin + sizeof(MetaData);
			Chunk::readID(reader, s);
			Chunk::readID(reader, o);
			index->chunkMeta.push_back({ s, o });
			chunkBegin = chunkBegin + MemoryBuffer::pagesize;
		}

	}
	else if (index_type == OBJECT_INDEX) {
		if (index->startPtr == index->endPtr) {
			index->chunkMeta.push_back({ 0, 0 });
			cout << "LineHashIndex::load error" << endl;
			return index;
		}
		reader = index->startPtr + sizeof(MetaData);
		Chunk::readID(reader, s);
		Chunk::readID(reader, o);
		index->chunkMeta.push_back({ o, s });
		chunkBegin = index->startPtr - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
		while (chunkBegin < index->endPtr) {
			reader = chunkBegin + sizeof(MetaData);
			Chunk::readID(reader, s);
			Chunk::readID(reader, o);
			index->chunkMeta.push_back({ o, s });
			chunkBegin = chunkBegin + MemoryBuffer::pagesize;
		}
	}
	else {
		cout << "LineHashIndex::load error" << endl;
	}
	return index;
}
