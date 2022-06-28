/*
 * PartitionMaster.h
 *
 *  Created on: 2013-8-19
 *      Author: root
 */

#ifndef PARTITIONMASTER_H_
#define PARTITIONMASTER_H_

class BitmapBuffer;
class GXTripleBitRepository;
class CXTripleBitRepository;
class EntityIDBuffer;
class Chunk;
class TasksQueueWP;
class CXTasksQueueWP;
class GXSubTrans;
class CXSubTrans;
class ChunkManager;
class ResultBuffer;
class GXTempBuffer;
class CXTempBuffer;
class PartitionBufferManager;
class GXTasksQueueChunk;
class CXTasksQueueChunk;
class subTaskPackage;
class IndexForTT;
class GXChunkTask;
class CXChunkTask;

#include "TripleBit.h"
#include "TripleBitQueryGraph.h"
#include <boost/thread/thread.hpp>
using namespace boost;

//管理一个谓词，每个谓词都有一个PartitionMaster
class PartitionMaster {
private:
	CXTripleBitRepository* tripleBitRepo;
	BitmapBuffer* bitmap;

	int workerNum;
	int partitionNum;

	CXTasksQueueWP* tasksQueue;
	map<ID, ResultBuffer*> resultBuffer;

	ID partitionID;
	ChunkManager* partitionChunkManager[2];

	//每个数据块下的块级别查询任务队列
	map<ID, CXTasksQueueChunk*> ChunkQueue[2];

	unsigned ChunkNumber[2];
	PartitionBufferManager* partitionBufferManager;

public:
	PartitionMaster(CXTripleBitRepository*& repo, const ID parID);
	virtual ~PartitionMaster();
	void Work();
	uchar getLen(ID id) {
		uchar len = 0;
		while (id >= 128) {
			++len;
			id >>= 7;
		}
		return len + 1;
	}

private:
	void doChunkQuery(pair<CXChunkTask*, CXTasksQueueChunk*>& task);
	void doChunkGroupQuery(vector<pair<CXChunkTask*, CXTasksQueueChunk*> >& tasks, int begin, int end);

	void executeQuery(CXSubTrans* subTransaction);

	void executeChunkTaskQuery(CXChunkTask* chunkTask, const ID chunkID, const uchar* chunkBegin, const int xyType);



	void findSubjectIDByPredicate(EntityIDBuffer* retBuffer, const ID minID, const ID maxID, const uchar* startPtr, const int xyType);
	void findSubjectIDByPredicate(EntityIDBuffer* retBuffer, const uchar* startPtr, const int xyType);

	void findObjectIDByPredicate(EntityIDBuffer* retBuffer, const ID minID, const ID maxID, const uchar* startPtr, const int xyType);
	void findObjectIDByPredicate(EntityIDBuffer* retBuffer, const uchar* startPtr, const int xyType);


	//查询SO
	void findSubjectIDAndObjectIDByPredicate(EntityIDBuffer* retBuffer, const ID minID, const ID maxID, const uchar* startPtr, const int xyType);
	void findSubjectIDAndObjectIDByPredicate(EntityIDBuffer* retBuffer, const uchar* startPtr, const int xyType);

	//查询OS
	void findObjectIDAndSubjectIDByPredicate(EntityIDBuffer* retBuffer, const ID minID, const ID maxID, const uchar* startPtr, const int xyType);
	void findObjectIDAndSubjectIDByPredicate(EntityIDBuffer* retBuffer, const uchar* startPtr, const int xyType);


	//查询 O  
	void findObjectIDByPredicateAndSubject(const ID subject, EntityIDBuffer* retBuffer, const ID minID, const ID maxID, const uchar* startPtr, const int xyType);
	void findObjectIDByPredicateAndSubject(const ID subject, EntityIDBuffer* retBuffer, const uchar* startPtr, const int xyType);


	//查询S
	void findSubjectIDByPredicateAndObject(const ID object, EntityIDBuffer* retBuffer, const ID minID, const ID maxID, const uchar* startPtr, const int xyType);
	void findSubjectIDByPredicateAndObject(const ID object, EntityIDBuffer* retBuffer, const uchar* startPtr, const int xyType);

};

#endif /* PARTITIONMASTER_H_ */
