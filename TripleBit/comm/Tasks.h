/*
 * Tasks.h
 *
 *  Created on: 2014-3-6
 *      Author: root
 */

#ifndef TASKS_H_
#define TASKS_H_

#include "Tools.h"
#include "../TripleBit.h"
#include "../TripleBitQueryGraph.h"
#include "subTaskPackage.h"
#include "IndexForTT.h"

class GXSubTrans: private Uncopyable {
public:
	struct timeval transTime;
	ID sourceWorkerID;
	ID minID;
	ID maxID;
	TripleBitQueryGraph::OpType operationType;
	GXTripleNode triple;
	GXSubTrans *next;

	boost::shared_ptr<IndexForTT> indexForTT; //无用

	GXSubTrans(timeval &transtime, ID sWorkerID, ID miID, ID maID, TripleBitQueryGraph::OpType &opType, GXTripleNode &trip) :
			transTime(transtime), sourceWorkerID(sWorkerID), minID(miID), maxID(maID), operationType(opType), triple(
					trip) {
		next = NULL;
	}

	GXSubTrans(timeval &transtime, ID sWorkerID, ID miID, ID maID,
				TripleBitQueryGraph::OpType &opType,
				GXTripleNode &trip, boost::shared_ptr<IndexForTT> index_forTT) :
				transTime(transtime), sourceWorkerID(sWorkerID), minID(miID), maxID(
						maID), operationType(opType), triple(
						trip) , indexForTT(index_forTT){
		next = NULL;
		}

	GXSubTrans(){
		next = NULL;
	}

};


class CXSubTrans : private Uncopyable {
public:
	struct timeval transTime;
	ID sourceWorkerID;
	ID minID;
	ID maxID;
	TripleBitQueryGraph::OpType operationType;
	size_t tripleNumber;
	CXTripleNode triple;
	boost::shared_ptr<IndexForTT> indexForTT;

	CXSubTrans(timeval& transtime, ID sWorkerID, ID miID, ID maID,
		TripleBitQueryGraph::OpType& opType, size_t triNumber,
		CXTripleNode& trip, boost::shared_ptr<IndexForTT> index_forTT) :
		transTime(transtime), sourceWorkerID(sWorkerID), minID(miID), maxID(
			maID), operationType(opType), tripleNumber(triNumber), triple(
				trip), indexForTT(index_forTT) {
	}
};



//一个数据块插入任务
class GXChunkTask: private Uncopyable {
public:
	timeval chunkTaskTime;
	TripleBitQueryGraph::OpType operationType;
	GXChunkTriple Triple;
	boost::shared_ptr<SubTaskPackageForDelete> taskPackageForDelete;
	boost::shared_ptr<subTaskPackage> taskPackage;
	boost::shared_ptr<IndexForTT> indexForTT;
	ID updateSubjectID;
	ID updateObjectID;
	bool opSO;
	ID chunkID;

	GXChunkTask *next;

	GXChunkTask() {
		next = NULL;
	}

	//用于测试insert data任务完成计数
	int time;

	GXChunkTask(timeval &chunkTaskTime, TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID, ID timeID,
			GXTripleNode::Op operation, bool opSO, ID chunkID) :
			chunkTaskTime(chunkTaskTime), operationType(opType), Triple( { subjectID, predicateID, objectID,timeID,
					operation }), opSO(opSO), chunkID(chunkID) {
		next = NULL;
	}

	//用于测试insert data任务完成计数
	/*GXChunkTask(timeval &chunkTaskTime, TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID, GXTripleNode::Op operation, OrderByType opSO, ID chunkID, int& time) :
	 chunkTaskTime(chunkTaskTime), operationType(opType), Triple( { subjectID, predicateID, objectID, operation }), opSO(opSO), chunkID(chunkID), time(time) {
	 }*/

	GXChunkTask(timeval& chunkTaskTime, TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID,ID timeID,
		GXTripleNode::Op operation, boost::shared_ptr<SubTaskPackageForDelete> task_Package,bool opSO, ID chunkID) :
		chunkTaskTime(chunkTaskTime), operationType(opType), Triple({ subjectID, predicateID, objectID,timeID,operation }), 
		taskPackageForDelete(task_Package), opSO(opSO), chunkID(chunkID) {
	}

	GXChunkTask(timeval& chunkTaskTime, TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID, ID timeID,
		GXTripleNode::Op operation, boost::shared_ptr<subTaskPackage> task_Package, bool opSO, ID chunkID) :
		chunkTaskTime(chunkTaskTime), operationType(opType), Triple({ subjectID, predicateID, objectID,timeID,operation }),
		taskPackage(task_Package), opSO(opSO), chunkID(chunkID) {
	}

	GXChunkTask(timeval& chunkTaskTime, TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID, ID timeID,
		ID updateSubjectID, ID updateObjectID, GXTripleNode::Op operation,
		boost::shared_ptr<SubTaskPackageForDelete> task_Package, bool opSO, ID chunkID) :
		chunkTaskTime(chunkTaskTime), operationType(opType), Triple({ subjectID, predicateID, objectID,timeID,operation }),
		updateSubjectID(updateSubjectID), updateObjectID(updateObjectID),
		taskPackageForDelete(task_Package), opSO(opSO), chunkID(chunkID) {
	}

	GXChunkTask(TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID, ID timeID,
		GXTripleNode::Op operation, boost::shared_ptr<subTaskPackage> task_Package,
		boost::shared_ptr<IndexForTT> index_ForTT) :
		operationType(opType), Triple({ subjectID, predicateID, objectID,timeID, operation }),
		taskPackage(task_Package), indexForTT(index_ForTT) {
	} //partitionmaster

	GXChunkTask(TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID, ID timeID,
		GXTripleNode::Op operation, boost::shared_ptr<SubTaskPackageForDelete> task_Package,
		boost::shared_ptr<IndexForTT> index_ForTT) :
		operationType(opType), Triple({ subjectID, predicateID, objectID,timeID, operation }), 
		taskPackageForDelete(task_Package), indexForTT(index_ForTT) {
	} //partitionmaster

	GXChunkTask(TripleBitQueryGraph::OpType opType, ID subjectID, ID predicateID, ID objectID, ID timeID,
		ID updateSubjectID, ID updateObjectID, GXTripleNode::Op operation,
		boost::shared_ptr<SubTaskPackageForDelete> task_Package, boost::shared_ptr<IndexForTT> index_ForTT) :
		operationType(opType), Triple({ subjectID, predicateID, objectID,timeID, operation }), 
		updateSubjectID(updateSubjectID), updateObjectID(updateObjectID), taskPackageForDelete(task_Package) {
	} //partitionmaster

	~GXChunkTask() {
	}

	void setChunkTask(timeval &chunkTaskTime, TripleBitQueryGraph::OpType opType, GXTripleNode &tripleNode, bool opSO, ID chunkID) {
		this->chunkTaskTime = chunkTaskTime;
		this->operationType = opType;
		this->Triple.subjectID = tripleNode.subjectID;
		this->Triple.predicateID = tripleNode.predicateID;
		this->Triple.objectID = tripleNode.objectID;
		this->Triple.timeID = tripleNode.timeID;
		this->Triple.operation = tripleNode.scanOperation;
		this->opSO = opSO;
		this->chunkID = chunkID;
	}
};

//一个数据块查询任务
class CXChunkTask {
public:
	struct CXChunkTriple
	{
		ID subject, object;
		CXTripleNode::Op operation;
	};
	TripleBitQueryGraph::OpType operationType;
	CXChunkTriple Triple;
	boost::shared_ptr<subTaskPackage> taskPackage;
	boost::shared_ptr<IndexForTT> indexForTT;

	ID minID, maxID;
	CXChunkTask(TripleBitQueryGraph::OpType opType, ID subject, ID object, CXTripleNode::Op operation, boost::shared_ptr<subTaskPackage> task_Package, boost::shared_ptr<IndexForTT> index_ForTT) :
		operationType(opType), Triple({ subject, object, operation }), taskPackage(task_Package), indexForTT(index_ForTT) {
	}

	CXChunkTask(TripleBitQueryGraph::OpType opType, ID subject, ID object, CXTripleNode::Op operation, ID _minID, ID _maxID) :
		operationType(opType), Triple({ subject, object, operation }), minID(_minID), maxID(_maxID) {
	}

	~CXChunkTask() {}
};

#endif /* TASKS_H_ */
