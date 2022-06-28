/*
 * GXTripleBitWorkerQuery.h
 *
 *  Created on: 2013-7-1
 *      Author: root
 */

#ifndef TRIPLEBITWORKERQUERY_H_
#define TRIPLEBITWORKERQUERY_H_

class BitmapBuffer;
class URITable;
class PredicateTable;
class GXTripleBitRepository;
class CXTripleBitRepository;
class TripleBitQueryGraph;
class EntityIDBuffer;
class HashJoin;
class TasksQueueWP;
class CXTasksQueueWP;
class ResultBuffer;
class PartitionThreadPool;
class IndexForTT;
class GXSubTrans;
class GXChunkTask;
class CXSubTrans;
class CXChunkTask;

#include "TripleBitQueryGraph.h"
#include "TripleBit.h"
#include "util/HashJoin.h"
#include "util/SortMergeJoin.h"
#include "util/atomic.hpp"
#include "GXTempBuffer.h"
#include "TempMMapBuffer.h"
#include "comm/GXTasksQueueChunk.h"
#include "comm/TempBufferQueue.h"
#include "comm/SortChunkTaskQueue.h"
#include "TempFile.h"
#include "Sorter.h"
#include <boost/thread/mutex.hpp>

typedef map<ID, EntityIDBuffer*> EntityIDListType;
typedef map<ID, EntityIDBuffer*>::iterator EntityIDListIterType;

class GXTripleBitWorkerQuery {
public:
	map<ID, ChunkManager*> chunkManager[2];
private:
	GXTripleBitRepository* tripleBitRepo;
	BitmapBuffer* bitmap;
	URITable* uriTable;
	PredicateTable* preTable;
	

	TripleBitQueryGraph* _queryGraph;
	TripleBitQueryGraph::SubQuery* _query;

	EntityIDListType EntityIDList;
	vector<TripleBitQueryGraph::JoinVariableNodeID> idTreeBFS;
	vector<TripleBitQueryGraph::JoinVariableNodeID> leafNode;
	vector<TripleBitQueryGraph::JoinVariableNodeID> varVec;

	ID workerID;

	//something about shared memory
	int partitionNum;
	vector<TasksQueueWP*> tasksQueueWP;
	vector<ResultBuffer*> resultWP;
	vector<boost::mutex*> tasksQueueWPMutex;
	map<ID, map<ID, MidResultBuffer*>> resultSet;

//	TasksQueueWP* tasksQueue;
	map<ID, TasksQueueWP*> tasksQueue;
	map<ID, ResultBuffer*> resultBuffer;

	//ÿ�����ݿ���������
	map<ID, vector<GXTasksQueueChunk*>> chunkQueue[2];

	//ÿ�����ݿ�Ļ���
	map<ID, vector<GXTempBuffer*>> chunkTempBuffer[2];

	//ÿ�����ݿ�Ŀ�ʼ��ַ
	map<ID, vector<const uchar*>> chunkStartPtr[2];

	//used to classity the InsertData or DeleteData
	map<ID, vector<GXTripleNode*> > tripleNodeMap;
	vector<GXChunkTask*> chunkTasks;
	boost::mutex chunkTasksMutex;

	//used to get the results
	vector<int> varPos;
	vector<int> keyPos;
	vector<int> resultPos;
	vector<int> verifyPos;
	vector<ID> resultVec;
	vector<size_t> bufPreIndexs;
	bool needselect;

	timeval *transactionTime;

	vector<string>* resultPtr;
	vector<SortChunkTask *> sortTask;
	vector<GXTempBuffer*> tempBuffers;

public:
	static triplebit::atomic<size_t> timeCount;
	static triplebit::atomic<size_t> tripleCount;
public:
	GXTripleBitWorkerQuery(GXTripleBitRepository*& repo, ID workID);
	virtual ~GXTripleBitWorkerQuery();

	void releaseBuffer();
	Status query(TripleBitQueryGraph* queryGraph, vector<string>& resultSet, timeval& transTime);
	void executeSortChunkTask(SortChunkTask *sortChunkTask);
	void executeSortChunkTaskPartition(const vector<SortChunkTask *>&sortTask, int start, int end);

private:

	void taskEnQueue(GXChunkTask*chunkTask, GXTasksQueueChunk *tasksQueue);
	void handleTasksQueueChunk(GXTasksQueueChunk* tasksQueue);
	void executeEndForWorkTaskPartition(vector<GXTempBuffer*> &tempBuffers, int start, int end);

	void endForWork();
	void sortDataBase();

	Status excuteInsertData();
	void executeInsertData(const vector<GXTripleNode*> &tripleNodes);
	void executeInsertChunkData(GXChunkTask*chunkTask);
	void combineTempBufferToSource(GXTempBuffer *buffer);

	void classifyTripleNode();


};

//---------------------------------------//
class CXTripleBitWorkerQuery
{
private:
	CXTripleBitRepository* tripleBitRepo;
	BitmapBuffer* bitmap;
	URITable* uriTable;
	PredicateTable* preTable;

	TripleBitQueryGraph* _queryGraph;
	TripleBitQueryGraph::SubQuery* _query;

	EntityIDListType EntityIDList;
	vector<TripleBitQueryGraph::JoinVariableNodeID> idTreeBFS;
	vector<TripleBitQueryGraph::JoinVariableNodeID> leafNode;
	vector<TripleBitQueryGraph::JoinVariableNodeID> varVec;

	ID workerID;
	ID max_id;

	//something about shared memory
	int partitionNum;
	vector<CXTasksQueueWP*> tasksQueueWP;
	vector<ResultBuffer*> resultWP;
	vector<boost::mutex*> tasksQueueWPMutex;

	map<ID, CXTasksQueueWP*> tasksQueue;
	map<ID, ResultBuffer*> resultBuffer;

	//used to classity the InsertData or DeleteData
	map<ID, set<CXTripleNode*> > tripleNodeMap;

	//used to get the results
	vector<int> varPos;
	vector<int> keyPos;
	vector<int> resultPos;
	vector<int> verifyPos;
	vector<ID> resultVec;
	vector<size_t> bufPreIndexs;
	bool needselect;

	HashJoin hashJoin;
	SortMergeJoin mergeJoin;

	timeval* transactionTime;

public:
	vector<string>* resultPtr;

public:
	CXTripleBitWorkerQuery(CXTripleBitRepository*& repo, ID workID);
	~CXTripleBitWorkerQuery();

	void releaseBuffer();
	Status query(int mod, TripleBitQueryGraph* queryGraph, vector<string>& resultSet, timeval& transTime);
	Status query1(int mod, TripleBitQueryGraph* queryGraph, vector<string>& resultSet, timeval& transTime);

private:
	void tasksEnQueue(ID partitionID, CXSubTrans* subTrans);
	Status excuteQuery();
	Status excuteQuery1();
	Status centerJoin();
	Status centerJoin1();
	Status linerJoin();
	Status treeShapeJoin();

	ResultIDBuffer* findEntityIDByTriple(CXTripleNode* triple, ID minID, ID maxID);
	ResultIDBuffer* findEntityIDByTriple1(CXTripleNode* triple, ID minID, ID maxID);

	Status getTripleNodeByID(CXTripleNode*& triple, TripleBitQueryGraph::TripleNodeID nodeID);
	Status getVariableNodeByID(TripleBitQueryGraph::JoinVariableNode*& node, TripleBitQueryGraph::JoinVariableNodeID id);

	bool getResult(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buf_index);
	void getResult_join(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buf_index);

};
#endif /* TRIPLEBITWORKERQUERY_H_ */
