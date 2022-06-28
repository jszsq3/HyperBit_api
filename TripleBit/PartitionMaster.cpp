/*
 * PartitionMaster.cpp
 *
 *  Created on: 2013-8-19
 *      Author: root
 */

#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "GXTripleBitRepository.h"
#include "EntityIDBuffer.h"
#include "TripleBitQueryGraph.h"
#include "util/BufferManager.h"
#include "util/PartitionBufferManager.h"
#include "comm/TasksQueueWP.h"
#include "comm/ResultBuffer.h"
#include "comm/GXTasksQueueChunk.h"
#include "comm/subTaskPackage.h"
#include "comm/Tasks.h"
#include "GXTempBuffer.h"
#include "MMapBuffer.h"
#include "PartitionMaster.h"
#include "ThreadPool.h"
#include "TempMMapBuffer.h"
#include "util/Timestamp.h"
#include "TripleBit.h"
#include "CBitMap.h"

#define QUERY_TIME

extern ID xMin;
extern ID xMax;
extern ID zMin;
extern ID zMax;

extern vector<vector<CBitMap*> > patternBitmap;
extern ID preVar;
extern ID postVar;
extern int preLevel;
extern int postLevel;
extern bool isSingle;

PartitionMaster::PartitionMaster(CXTripleBitRepository*& repo, const ID parID) {
	tripleBitRepo = repo;
	bitmap = repo->getBitmapBuffer();

	workerNum = repo->getWorkerNum();
	partitionNum = repo->getPartitionNum();
	vector<CXTasksQueueWP*> tasksQueueWP = repo->getTasksQueueWP();
	vector<ResultBuffer*> resultWP = repo->getResultWP();

	partitionID = parID;
	partitionChunkManager[0] = bitmap->getChunkManager(partitionID, ORDERBYS);
	partitionChunkManager[1] = bitmap->getChunkManager(partitionID, ORDERBYO);

	tasksQueue = tasksQueueWP[partitionID - 1];
	for (int workerID = 1; workerID <= workerNum; ++workerID) {
		resultBuffer[workerID] = resultWP[(workerID - 1) * partitionNum + partitionID - 1];
	}

	for (int type = 0; type < 2; type++) {
		const uchar* startPtr = partitionChunkManager[type]->getStartPtr();
		ChunkNumber[type] = partitionChunkManager[type]->getChunkNumber();
		ID chunkID = 0;
		ChunkQueue[type][0] = new CXTasksQueueChunk(startPtr, chunkID, 1, type);
		for (chunkID = 1; chunkID < ChunkNumber[type]; chunkID++) {
			ChunkQueue[type][chunkID] = new CXTasksQueueChunk(startPtr + chunkID * MemoryBuffer::pagesize - sizeof(ChunkManagerMeta), chunkID, 1,type);
		}
	}

	partitionBufferManager = NULL;
}

PartitionMaster::~PartitionMaster() {
	for (int type = 0; type < 2; type++) {
		for (unsigned chunkID = 0; chunkID < ChunkNumber[type]; chunkID++) {
			if (ChunkQueue[type][chunkID]) {
				delete ChunkQueue[type][chunkID];
				ChunkQueue[type][chunkID] = NULL;
			}
		}
	}

	if (partitionBufferManager) {
		delete partitionBufferManager;
		partitionBufferManager = NULL;
	}
}

void PartitionMaster::Work() {
#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID << endl;
#endif

	while (1) {
		CXSubTrans* subTransaction = tasksQueue->Dequeue();

		if (subTransaction == NULL)
			break;

		switch (subTransaction->operationType) {
		case TripleBitQueryGraph::QUERY:
			executeQuery(subTransaction);
			delete subTransaction;
			break;
		}
	}
}

void PartitionMaster::doChunkGroupQuery(vector<pair<CXChunkTask*, CXTasksQueueChunk*> >& tasks, int begin, int end) {
	for (int i = begin; i <= end; i++) {
		doChunkQuery(tasks[i]);
	}
}

void PartitionMaster::doChunkQuery(pair < CXChunkTask*, CXTasksQueueChunk* >& task) {
	CXChunkTask* chunkTask = task.first;
	CXTasksQueueChunk* tasksQueue = task.second;
	ID chunkID = tasksQueue->getChunkID();
	int xyType = 1;
	int soType = tasksQueue->getSOType();
	const uchar* chunkBegin = tasksQueue->getChunkBegin();
	executeChunkTaskQuery(chunkTask, chunkID, chunkBegin, xyType);
}

void PartitionMaster::executeQuery(CXSubTrans* subTransaction) {
#ifdef QUERY_TIME
	Timestamp t1;
#endif

	ID minID = subTransaction->minID;
	ID maxID = subTransaction->maxID;
	CXTripleNode* triple = &(subTransaction->triple);

	size_t chunkCount;
	size_t ChunkIDMin, ChunkIDMax;

	int soType;
	ChunkIDMin = ChunkIDMax = 0;
	chunkCount = 0;

	switch (triple->scanOperation) {
	case CXTripleNode::FINDOSBYP: {
		soType = 1;
		ChunkIDMin = partitionChunkManager[soType]->getChunkIndex()->lowerfind(make_pair(minID, 1));
		ChunkIDMax = partitionChunkManager[soType]->getChunkIndex()->upperfind(make_pair(maxID, UINT_MAX));
		cout << "chunkNumber:" << partitionChunkManager[soType]->getChunkNumber() << endl;

		cout << "chunkIDMin:" << ChunkIDMin << " chunkIDMax: " << ChunkIDMax << endl;
		assert(ChunkIDMin <= ChunkIDMax);
		chunkCount = ChunkIDMax - ChunkIDMin + 1;
		break;
	}
	case CXTripleNode::FINDSOBYP: {
		soType = 0;
		ChunkIDMin = partitionChunkManager[soType]->getChunkIndex()->lowerfind(make_pair(minID, 1));
		ChunkIDMax = partitionChunkManager[soType]->getChunkIndex()->upperfind(make_pair(maxID, UINT_MAX));
		cout << "chunkNumber:" << partitionChunkManager[soType]->getChunkNumber() << endl;
		cout << "chunkIDMin:" << ChunkIDMin << " chunkIDMax: " << ChunkIDMax << endl;
		assert(ChunkIDMin <= ChunkIDMax);
		chunkCount = ChunkIDMax - ChunkIDMin + 1;
		break;
	}
	case CXTripleNode::FINDSBYPO: {
		cout << "FINDSBYPO" << endl;
		soType = 1;//OS�������� 
		ChunkIDMin = partitionChunkManager[soType]->getChunkIndex()->lowerfind(make_pair(triple->object, minID));
		ChunkIDMax = partitionChunkManager[soType]->getChunkIndex()->upperfind(make_pair(triple->object, maxID));
		cout << "chunkNumber:" << partitionChunkManager[soType]->getChunkNumber() << endl;
		cout << "chunkIDMin:" << ChunkIDMin << " chunkIDMax: " << ChunkIDMax << endl;
		assert(ChunkIDMax >= ChunkIDMin);
		chunkCount = ChunkIDMax - ChunkIDMin + 1;
		break;
	}
	case CXTripleNode::FINDOBYSP: {
		soType = 0;//SO��������
		ChunkIDMin = partitionChunkManager[soType]->getChunkIndex()->lowerfind(make_pair(triple->subject, minID));
		ChunkIDMax = partitionChunkManager[soType]->getChunkIndex()->upperfind(make_pair(triple->subject, maxID));
		// cout << "chunkNumber:" << partitionChunkManager[soType]->getChunkNumber() << endl;
		// cout << "chunkIDMin:" << ChunkIDMin << " chunkIDMax: " << ChunkIDMax << endl;
		assert(ChunkIDMax >= ChunkIDMin);
		chunkCount = ChunkIDMax - ChunkIDMin + 1;
		break;
	}
	case CXTripleNode::FINDSBYP: {
		soType = 0;//SO��������  S����[minID,maxID]
		ChunkIDMin = partitionChunkManager[soType]->getChunkIndex()->lowerfind(make_pair(minID, 1));
		ChunkIDMax = partitionChunkManager[soType]->getChunkIndex()->upperfind(make_pair(maxID, UINT_MAX));
		cout << "chunkNumber:" << partitionChunkManager[soType]->getChunkNumber() << endl;
		cout << "chunkIDMin:" << ChunkIDMin << " chunkIDMax: " << ChunkIDMax << endl;
		assert(ChunkIDMax >= ChunkIDMin);
		chunkCount = ChunkIDMax - ChunkIDMin + 1;
		break;
	}
	case CXTripleNode::FINDOBYP: {
		soType = 1;//OS��������  O����[minID,maxID]
		ChunkIDMin = partitionChunkManager[soType]->getChunkIndex()->lowerfind(make_pair(minID, 1));
		ChunkIDMax = partitionChunkManager[soType]->getChunkIndex()->upperfind(make_pair(maxID, UINT_MAX));
		cout << "chunkNumber:" << partitionChunkManager[soType]->getChunkNumber() << endl;
		cout << "chunkIDMin:" << ChunkIDMin << " chunkIDMax: " << ChunkIDMax << endl;
		assert(ChunkIDMax >= ChunkIDMin);
		chunkCount = ChunkIDMax - ChunkIDMin + 1;
		break;
	}
	}

	if (chunkCount == 0) {
		cout << "Empty Result" << endl;
		return;
	}

	ID sourceWorkerID = subTransaction->sourceWorkerID;

	//���һ��pattern��ѯ����
	boost::shared_ptr<subTaskPackage> taskPackage(
		new subTaskPackage(chunkCount, subTransaction->operationType, sourceWorkerID, subTransaction->minID, subTransaction->maxID, 0, 0,
			partitionBufferManager));

	vector<pair<CXChunkTask*, CXTasksQueueChunk*> > tasks;

	//���һ���鼶��Ĳ�ѯ����
	CXChunkTask* chunkTask = new CXChunkTask(subTransaction->operationType, triple->subject, triple->object, triple->scanOperation, taskPackage, 
		subTransaction->indexForTT);

	if (chunkCount != 0) {
		for (size_t offsetID = ChunkIDMin; offsetID <= ChunkIDMax; offsetID++) {
			tasks.push_back(make_pair(chunkTask, ChunkQueue[soType][offsetID]));
		}
	}

	if (tasks.size() < SCAN_JOIN_THREAD_NUMBER) {
		for (int i = 0; i < tasks.size(); i++) {
			CXThreadPool::getScanjoinPool().addTask(boost::bind(&PartitionMaster::doChunkGroupQuery, this, tasks, i, i));
		}
	}
	else {
		size_t chunkSize = tasks.size() / SCAN_JOIN_THREAD_NUMBER;
		int i = 0;
		for (i = 0; i < SCAN_JOIN_THREAD_NUMBER; i++)
		{
			if (i == SCAN_JOIN_THREAD_NUMBER - 1) {
				CXThreadPool::getScanjoinPool().addTask(boost::bind(&PartitionMaster::doChunkGroupQuery, this, tasks, i * chunkSize, tasks.size() - 1));
			}
			else {
				CXThreadPool::getScanjoinPool().addTask(boost::bind(&PartitionMaster::doChunkGroupQuery, this, tasks, i * chunkSize, i * chunkSize + chunkSize - 1));
			}
		}
	}
	CXThreadPool::getScanjoinPool().wait();
	

#ifdef QUERY_TIME
	Timestamp t3;
#endif

#ifdef QUERY_TIME
	Timestamp t4;
	//cout << "ChunkCount:" << chunkCount << endl;
	//cout << " taskEnqueue time elapsed: " << (static_cast<double> (t4 - t3) / 1000.0) << " s" << endl;
#endif
}

void PrintChunkTaskPart(CXChunkTask* chunkTask) {
	cout << "opType:" << chunkTask->operationType << " subject:" << chunkTask->Triple.subject << " object:" << chunkTask->Triple.object
		<< " operation:" << chunkTask->Triple.operation << endl;
}

static void getInsertChars(char* temp, unsigned x, unsigned y) {
	char* ptr = temp;
	while (x >= 128) {
		uchar c = static_cast<uchar>(x & 127);
		*ptr = c;
		ptr++;
		x >>= 7;
	}
	*ptr = static_cast<uchar>(x & 127);
	ptr++;

	while (y >= 128) {
		uchar c = static_cast<uchar>(y | 128);
		*ptr = c;
		ptr++;
		y >>= 7;
	}
	*ptr = static_cast<uchar>(y | 128);
	ptr++;
}

//һ�����ݿ�Ĳ�������
void PartitionMaster::executeChunkTaskQuery(CXChunkTask* chunkTask, const ID chunkID, const uchar* chunkBegin, const int xyType) {

	EntityIDBuffer* retBuffer = new EntityIDBuffer;
	retBuffer->empty();

	switch (chunkTask->Triple.operation) {
	case CXTripleNode::FINDSBYPO:
		findSubjectIDByPredicateAndObject(chunkTask->Triple.object, retBuffer, chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID, chunkBegin, xyType);
		break;
	case CXTripleNode::FINDOBYSP:
		findObjectIDByPredicateAndSubject(chunkTask->Triple.subject, retBuffer, chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID, chunkBegin, xyType);
		break;
	case CXTripleNode::FINDSBYP:
		findSubjectIDByPredicate(retBuffer, chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID, chunkBegin, xyType);
		break;
	case CXTripleNode::FINDOBYP:
		findObjectIDByPredicate(retBuffer, chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID, chunkBegin, xyType);
		break;
	case CXTripleNode::FINDOSBYP:
		findObjectIDAndSubjectIDByPredicate(retBuffer, chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID, chunkBegin, xyType);
		break;
	case CXTripleNode::FINDSOBYP:
		findSubjectIDAndObjectIDByPredicate(retBuffer, chunkTask->taskPackage->minID, chunkTask->taskPackage->maxID, chunkBegin, xyType);
		break;
	default:
		cout << "unsupport now! executeChunkTaskQuery" << endl;
		break;
	}

	if (chunkTask->taskPackage->completeSubTask(chunkID, retBuffer, xyType)) {
		ResultIDBuffer* buffer = new ResultIDBuffer(chunkTask->taskPackage);
		//�����ѯ���
		resultBuffer[chunkTask->taskPackage->sourceWorkerID]->EnQueue(buffer);
	}
	retBuffer = NULL;
}

void PartitionMaster::findObjectIDByPredicateAndSubject(const ID subjectID, EntityIDBuffer* retBuffer, const ID minID, const ID maxID, const uchar* startPtr, const int xyType) {

	if (minID == 0 && maxID == UINT_MAX) {
		findObjectIDByPredicateAndSubject(subjectID, retBuffer, startPtr, xyType);
		return;
	}

	register ID s, o, t;
	const uchar* limit, * reader, * chunkBegin = startPtr;

	retBuffer->setIDCount(1);
	retBuffer->setSortKey(0);

	ID& a = retBuffer->range[0].first;
	ID& b = retBuffer->range[0].second;
	ID& c = retBuffer->range[1].first;
	ID& d = retBuffer->range[1].second;

	MetaData* metaData = (MetaData*)chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = chunkBegin + metaData->usedSpace;

	while (reader < limit) {
		Chunk::readID(reader, s);
		Chunk::readID(reader, o);
		Chunk::readID(reader, t);
		if (s < subjectID) {
			continue;
		}
		else if (s == subjectID) {
			if (o < minID) {
				continue;
			}
			else if (o <= maxID) {
				if (preLevel == 0) {
					patternBitmap[preVar][preLevel]->insert(o);
					a = min(a, o);
					b = max(b, o);
					if (isSingle) {
						 retBuffer->insertID(o); 
					}
				}
				else {
					if (patternBitmap[preVar][preLevel - 1]->contains(o)) {
						patternBitmap[preVar][preLevel]->insert(o);
						a = min(a, o);
						b = max(b, o);
						if (isSingle) {
							retBuffer->insertID(o);
						}
					}
				}
			}
			else {
				break;
			}
		}
		else {
			break;
		}
	}

	//���¿�
	if (metaData->pageNo != -1) {
		int lastPage = metaData->pageNo;//������Ŀ��
		while (metaData->NextPageNo != -1) {
			//��ȡ���ݿ����ʼ��ַ
			reader = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (metaData->NextPageNo) * MemoryBuffer::pagesize;
			//��ȡ����Ԫ��Ϣ
			metaData = (MetaData*)(const_cast<uchar*>(reader));
			//��ȡ���ݲ��ֵĽ�����ַ
			limit = reader + metaData->usedSpace;
			reader += sizeof(MetaData);
			while (reader < limit) {
				Chunk::readID(reader, s);
				Chunk::readID(reader, o);
				Chunk::readID(reader, t);
				if (s < subjectID) {
					continue;
				}
				else if (s == subjectID) {
					if (o < minID) {
						continue;
					}
					else if (o <= maxID) {
						if (preLevel == 0) {
							patternBitmap[preVar][preLevel]->insert(o);
							a = min(a, o);
							b = max(b, o);
							if (isSingle) {
								retBuffer->insertID(o); 
							}
						}
						else {
							if (patternBitmap[preVar][preLevel - 1]->contains(o)) {
								patternBitmap[preVar][preLevel]->insert(o);
								a = min(a, o);
								b = max(b, o);
								if (isSingle) {
									retBuffer->insertID(o);
								}
							}
						}
					}
					else {
						break;
					}
				}
				else {
					break;
				}
			}
			if (metaData->pageNo == lastPage) {
				break;
			}
		}
	}
}

void PartitionMaster::findObjectIDByPredicateAndSubject(const ID subjectID, EntityIDBuffer* retBuffer, const uchar* startPtr, const int xyType) {
	
	register ID s, o, t;
	const uchar* reader, * limit, * chunkBegin = startPtr;

	retBuffer->setIDCount(1);
	retBuffer->setSortKey(0);

	ID& a = retBuffer->range[0].first;
	ID& b = retBuffer->range[0].second;
	ID& c = retBuffer->range[1].first;
	ID& d = retBuffer->range[1].second;

	MetaData* metaData = (MetaData*)chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = chunkBegin + metaData->usedSpace;

	while (reader < limit) {
		Chunk::readID(reader, s);
		Chunk::readID(reader, o);
		Chunk::readID(reader, t);
		if (s < subjectID) {
			continue;
		}
		else if (s == subjectID) {
			if (preLevel == 0) {
				patternBitmap[preVar][preLevel]->insert(o);
				a = min(a, o);
				b = max(b, o);
				if (isSingle) { 
					retBuffer->insertID(o); 
				}
			}
			else {
				if (patternBitmap[preVar][preLevel - 1]->contains(o)) {
					patternBitmap[preVar][preLevel]->insert(o);
					a = min(a, o);
					b = max(b, o);
					if (isSingle) { retBuffer->insertID(o); }
				}
			}
		}
		else {
			break;
		}
	}

	if (metaData->pageNo != -1) {
		int lastPage = metaData->pageNo;//������Ŀ��
		while (metaData->NextPageNo != -1) {
			//��ȡ���ݿ����ʼ��ַ
			reader = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (metaData->NextPageNo) * MemoryBuffer::pagesize;
			//��ȡ����Ԫ��Ϣ
			metaData = (MetaData*)(const_cast<uchar*>(reader));
			//��ȡ���ݲ��ֵĽ�����ַ
			limit = reader + metaData->usedSpace;
			reader += sizeof(MetaData);
			while (reader < limit) {
				Chunk::readID(reader, s);
				Chunk::readID(reader, o);
				Chunk::readID(reader, t);
				if (s < subjectID) {
					continue;
				}
				else if (s == subjectID) {
					if (preLevel == 0) {
						patternBitmap[preVar][preLevel]->insert(o);
						a = min(a, o);
						b = max(b, o);
						if (isSingle) { retBuffer->insertID(o);  }
					}
					else {
						if (patternBitmap[preVar][preLevel - 1]->contains(o)) {
							patternBitmap[preVar][preLevel]->insert(o);
							a = min(a, o);
							b = max(b, o);
							if (isSingle) { retBuffer->insertID(o); }
						}
					}
				}
				else {
					break;
				}
			}
			if (metaData->pageNo == lastPage) {
				break;
			}
		}
	}


}

void PartitionMaster::findSubjectIDByPredicateAndObject(const ID objectID, EntityIDBuffer* retBuffer, const ID minID, const ID maxID, const uchar* startPtr, const int xyType) {

	if (minID == 0 && maxID == UINT_MAX) {
		findSubjectIDByPredicateAndObject(objectID, retBuffer, startPtr, xyType);
		return;
	}

	register ID s, o, t;
	const uchar* limit, * reader, * chunkBegin = startPtr;

	retBuffer->setIDCount(1);
	retBuffer->setSortKey(0);

	ID& a = retBuffer->range[0].first;
	ID& b = retBuffer->range[0].second;
	ID& c = retBuffer->range[1].first;
	ID& d = retBuffer->range[1].second;

	MetaData* metaData = (MetaData*)chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = chunkBegin + metaData->usedSpace;

	while (reader < limit) {
		Chunk::readID(reader, s);
		Chunk::readID(reader, o);
		Chunk::readID(reader, t);
		if (o < objectID) {
			continue;
		}
		else if (o == objectID) {
			if (s < minID) {
				continue;
			}
			else if (s <= maxID) {
				if (preLevel == 0) {
					patternBitmap[preVar][preLevel]->insert(s);
					a = min(a, s);
					b = max(b, s);
					if (isSingle) {
						retBuffer->insertID(s);
					}
				}
				else {
					if (patternBitmap[preVar][preLevel - 1]->contains(s)) {
						patternBitmap[preVar][preLevel]->insert(s);
						a = min(a, s);
						b = max(b, s);
						if (isSingle) {
							retBuffer->insertID(s);
							
						}
					}
				}
			}
			else {
				break;
			}
		}
		else {
			break;
		}
	}


	if (metaData->pageNo != -1) {
		int lastPage = metaData->pageNo;//������Ŀ��
		while (metaData->NextPageNo != -1) {
			//��ȡ���ݿ����ʼ��ַ
			reader = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (metaData->NextPageNo) * MemoryBuffer::pagesize;
			//��ȡ����Ԫ��Ϣ
			metaData = (MetaData*)(const_cast<uchar*>(reader));
			//��ȡ���ݲ��ֵĽ�����ַ
			limit = reader + metaData->usedSpace;
			reader += sizeof(MetaData);
			while (reader < limit) {
				Chunk::readID(reader, s);
				Chunk::readID(reader, o);
				Chunk::readID(reader, t);
				if (o < objectID) {
					continue;
				}
				else if (o == objectID) {
					if (s < minID) {
						continue;
					}
					else if (s <= maxID) {
						if (preLevel == 0) {
							patternBitmap[preVar][preLevel]->insert(s);
							a = min(a, s);
							b = max(b, s);
							if (isSingle) {
								retBuffer->insertID(s); 
							}
						}
						else {
							if (patternBitmap[preVar][preLevel - 1]->contains(s)) {
								patternBitmap[preVar][preLevel]->insert(s);
								a = min(a, s);
								b = max(b, s);
								if (isSingle) {
									retBuffer->insertID(s);
									
								}
							}
						}
					}
					else {
						break;
					}
				}
				else {
					break;
				}
			}
			if (metaData->pageNo == lastPage) {
				break;
			}
		}
	}

}

void PartitionMaster::findSubjectIDByPredicateAndObject(const ID objectID, EntityIDBuffer* retBuffer, const uchar* startPtr, const int xyType) {

	register ID s, o, t;
	const uchar* reader, * limit, * chunkBegin = startPtr;

	retBuffer->setIDCount(1);
	retBuffer->setSortKey(0);

	ID& a = retBuffer->range[0].first;
	ID& b = retBuffer->range[0].second;
	ID& c = retBuffer->range[1].first;
	ID& d = retBuffer->range[1].second;

	MetaData* metaData = (MetaData*)chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = chunkBegin + metaData->usedSpace;

	while (reader < limit) {
		Chunk::readID(reader, s);
		Chunk::readID(reader, o);
		Chunk::readID(reader, t);
		if (o < objectID) {
			continue;
		}
		else if (o == objectID) {
			if (preLevel == 0) {
				patternBitmap[preVar][preLevel]->insert(s);
				a = min(a, s);
				b = max(b, s);
				if (isSingle) { retBuffer->insertID(s); }
			}
			else {
				if (patternBitmap[preVar][preLevel - 1]->contains(s)) {
					patternBitmap[preVar][preLevel]->insert(s);
					a = min(a, s);
					b = max(b, s);
					if (isSingle) { retBuffer->insertID(s);  }
				}
			}
		}
		else {
			break;
		}
	}


	if (metaData->pageNo != -1) {
		int lastPage = metaData->pageNo;//������Ŀ��
		while (metaData->NextPageNo != -1) {
			//��ȡ���ݿ����ʼ��ַ
			reader = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (metaData->NextPageNo) * MemoryBuffer::pagesize;
			//��ȡ����Ԫ��Ϣ
			metaData = (MetaData*)(const_cast<uchar*>(reader));
			//��ȡ���ݲ��ֵĽ�����ַ
			limit = reader + metaData->usedSpace;
			reader += sizeof(MetaData);
			while (reader < limit) {
				Chunk::readID(reader, s);
				Chunk::readID(reader, o);
				Chunk::readID(reader, t);
				if (o < objectID) {
					continue;
				}
				else if (o == objectID) {
					if (preLevel == 0) {
						patternBitmap[preVar][preLevel]->insert(s);
						a = min(a, s);
						b = max(b, s);
						if (isSingle) { retBuffer->insertID(s); }
					}
					else {
						if (patternBitmap[preVar][preLevel - 1]->contains(s)) {
							patternBitmap[preVar][preLevel]->insert(s);
							a = min(a, s);
							b = max(b, s);
							if (isSingle) { retBuffer->insertID(s);  }
						}
					}
				}
				else {
					break;
				}
			}
			if (metaData->pageNo == lastPage) {
				break;
			}
		}
	}

}

void PartitionMaster::findObjectIDAndSubjectIDByPredicate(EntityIDBuffer* retBuffer, const ID minID, const ID maxID, const uchar* startPtr, const int xyType) {

	if (minID == 0 && maxID == UINT_MAX) {
		findObjectIDAndSubjectIDByPredicate(retBuffer, startPtr, xyType);
		return;
	}

	register ID s, o, t;
	const uchar* limit, * reader, * chunkBegin = startPtr;

	retBuffer->setIDCount(2);
	retBuffer->setSortKey(0);

	ID& a = retBuffer->range[0].first;
	ID& b = retBuffer->range[0].second;
	ID& c = retBuffer->range[1].first;
	ID& d = retBuffer->range[1].second;

	MetaData* metaData = (MetaData*)chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = chunkBegin + metaData->usedSpace;

	while (reader < limit) {
		Chunk::readID(reader, s);
		Chunk::readID(reader, o);
		Chunk::readID(reader, t);
		if (o < minID) {
			continue;
		}
		else if (o <= maxID) {
			if (preLevel == 0 || patternBitmap[preVar][preLevel - 1]->contains(o)) {
				if (postLevel == 0 || patternBitmap[postVar][postLevel - 1]->contains(s)) {
					patternBitmap[preVar][preLevel]->insert(o);
					patternBitmap[postVar][postLevel]->insert(s);
					retBuffer->insertID(o);
					retBuffer->insertID(s);
					a = min(a, o);
					b = max(b, o);
					c = min(c, s);
					d = max(d, s);
				}
			}
		}
		else {
			break;
		}
	}



	if (metaData->pageNo != -1) {
		int lastPage = metaData->pageNo;//������Ŀ��
		while (metaData->NextPageNo != -1) {
			//��ȡ���ݿ����ʼ��ַ
			reader = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (metaData->NextPageNo) * MemoryBuffer::pagesize;
			//��ȡ����Ԫ��Ϣ
			metaData = (MetaData*)(const_cast<uchar*>(reader));
			//��ȡ���ݲ��ֵĽ�����ַ
			limit = reader + metaData->usedSpace;
			reader += sizeof(MetaData);
			while (reader < limit) {
				Chunk::readID(reader, s);
				Chunk::readID(reader, o);
				Chunk::readID(reader, t);
				if (o < minID) {
					continue;
				}
				else if (o <= maxID) {
					if (preLevel == 0 || patternBitmap[preVar][preLevel - 1]->contains(o)) {
						if (postLevel == 0 || patternBitmap[postVar][postLevel - 1]->contains(s)) {
							patternBitmap[preVar][preLevel]->insert(o);
							patternBitmap[postVar][postLevel]->insert(s);
							retBuffer->insertID(o);
							retBuffer->insertID(s);
							a = min(a, o);
							b = max(b, o);
							c = min(c, s);
							d = max(d, s);
						}
					}
				}
				else {
					break;
				}
			}
			if (metaData->pageNo == lastPage) {
				break;
			}
		}
	}
}

void PartitionMaster::findObjectIDAndSubjectIDByPredicate(EntityIDBuffer* retBuffer, const uchar* startPtr, const int xyType) {

	register ID s, o, t;
	const uchar* reader, * limit, * chunkBegin = startPtr;

	retBuffer->setIDCount(2);
	retBuffer->setSortKey(0);

	ID& a = retBuffer->range[0].first;
	ID& b = retBuffer->range[0].second;
	ID& c = retBuffer->range[1].first;
	ID& d = retBuffer->range[1].second;

	MetaData* metaData = (MetaData*)chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = reader + metaData->usedSpace;
	while (reader < limit) {
		Chunk::readID(reader, s);
		Chunk::readID(reader, o);
		Chunk::readID(reader, t);
		if (preLevel == 0 || patternBitmap[preVar][preLevel - 1]->contains(o)) {
			if (postLevel == 0 || patternBitmap[postVar][postLevel - 1]->contains(s)) {
				patternBitmap[preVar][preLevel]->insert(o);
				patternBitmap[postVar][postLevel]->insert(s);
				retBuffer->insertID(o);
				retBuffer->insertID(s);
				a = min(a, o);
				b = max(b, o);
				c = min(c, s);
				d = max(d, s);
			}
		}
	}


	if (metaData->pageNo != -1) {
		int lastPage = metaData->pageNo;//������Ŀ��
		while (metaData->NextPageNo != -1) {
			//��ȡ���ݿ����ʼ��ַ
			reader = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (metaData->NextPageNo) * MemoryBuffer::pagesize;
			//��ȡ����Ԫ��Ϣ
			metaData = (MetaData*)(const_cast<uchar*>(reader));
			//��ȡ���ݲ��ֵĽ�����ַ
			limit = reader + metaData->usedSpace;
			reader += sizeof(MetaData);
			while (reader < limit) {
				Chunk::readID(reader, s);
				Chunk::readID(reader, o);
				Chunk::readID(reader, t);
				if (preLevel == 0 || patternBitmap[preVar][preLevel - 1]->contains(o)) {
					if (postLevel == 0 || patternBitmap[postVar][postLevel - 1]->contains(s)) {
						patternBitmap[preVar][preLevel]->insert(o);
						patternBitmap[postVar][postLevel]->insert(s);
						retBuffer->insertID(o);
						retBuffer->insertID(s);
						a = min(a, o);
						b = max(b, o);
						c = min(c, s);
						d = max(d, s);
					}
				}
			}
			if (metaData->pageNo == lastPage) {
				break;
			}
		}
	}
}

void PartitionMaster::findSubjectIDAndObjectIDByPredicate(EntityIDBuffer* retBuffer, const ID minID, const ID maxID, const uchar* startPtr, const int xyType) {

	if (minID == 0 && maxID == UINT_MAX) {
		findSubjectIDAndObjectIDByPredicate(retBuffer, startPtr, xyType);
		return;
	}

	register ID s, o, t;
	const uchar* limit, * reader, * chunkBegin = startPtr;

	retBuffer->setIDCount(2);
	retBuffer->setSortKey(0);

	ID& a = retBuffer->range[0].first;
	ID& b = retBuffer->range[0].second;
	ID& c = retBuffer->range[1].first;
	ID& d = retBuffer->range[1].second;

	MetaData* metaData = (MetaData*)chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = chunkBegin + metaData->usedSpace;

	while (reader < limit) {
		Chunk::readID(reader, s);
		Chunk::readID(reader, o);
		Chunk::readID(reader, t);
		if (s < minID) {
			continue;
		}
		else if (s <= maxID) {
			if (preLevel == 0 || patternBitmap[preVar][preLevel - 1]->contains(s)) {
				if (postLevel == 0 || patternBitmap[postVar][postLevel - 1]->contains(o)) {
					patternBitmap[preVar][preLevel]->insert(s);
					patternBitmap[postVar][postLevel]->insert(o);
					retBuffer->insertID(s);
					retBuffer->insertID(o);
					a = min(a, s);
					b = max(b, s);
					c = min(c, o);
					d = max(d, o);
				}
			}
		}
		else {
			break;
		}
	}


	if (metaData->pageNo != -1) {
		int lastPage = metaData->pageNo;//������Ŀ��
		while (metaData->NextPageNo != -1) {
			//��ȡ���ݿ����ʼ��ַ
			reader = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (metaData->NextPageNo) * MemoryBuffer::pagesize;
			//��ȡ����Ԫ��Ϣ
			metaData = (MetaData*)(const_cast<uchar*>(reader));
			//��ȡ���ݲ��ֵĽ�����ַ
			limit = reader + metaData->usedSpace;
			reader += sizeof(MetaData);
			while (reader < limit) {
				Chunk::readID(reader, s);
				Chunk::readID(reader, o);
				Chunk::readID(reader, t);
				if (s < minID) {
					continue;
				}
				else if (s <= maxID) {
					if (preLevel == 0 || patternBitmap[preVar][preLevel - 1]->contains(s)) {
						if (postLevel == 0 || patternBitmap[postVar][postLevel - 1]->contains(o)) {
							patternBitmap[preVar][preLevel]->insert(s);
							patternBitmap[postVar][postLevel]->insert(o);
							retBuffer->insertID(s);
							retBuffer->insertID(o);
							a = min(a, s);
							b = max(b, s);
							c = min(c, o);
							d = max(d, o);
						}
					}
				}
				else {
					break;
				}
			}
			if (metaData->pageNo == lastPage) {
				break;
			}
		}
	}

}

void PartitionMaster::findSubjectIDAndObjectIDByPredicate(EntityIDBuffer* retBuffer, const uchar* startPtr, const int xyType) {

	register ID s, o, t;
	const uchar* reader, * limit, * chunkBegin = startPtr;

	retBuffer->setIDCount(2);
	retBuffer->setSortKey(0);

	ID& a = retBuffer->range[0].first;
	ID& b = retBuffer->range[0].second;
	ID& c = retBuffer->range[1].first;
	ID& d = retBuffer->range[1].second;

	MetaData* metaData = (MetaData*)chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = reader + metaData->usedSpace;
	while (reader < limit) {
		Chunk::readID(reader, s);
		Chunk::readID(reader, o);
		Chunk::readID(reader, t);
		if (preLevel == 0 || patternBitmap[preVar][preLevel - 1]->contains(s)) {
			if (postLevel == 0 || patternBitmap[postVar][postLevel - 1]->contains(o)) {
				patternBitmap[preVar][preLevel]->insert(s);
				patternBitmap[postVar][postLevel]->insert(o);
				retBuffer->insertID(s);
				retBuffer->insertID(o);
				a = min(a, s);
				b = max(b, s);
				c = min(c, o);
				d = max(d, o);
			}
		}
	}

	if (metaData->pageNo != -1) {
		int lastPage = metaData->pageNo;//������Ŀ��
		while (metaData->NextPageNo != -1) {
			//��ȡ���ݿ����ʼ��ַ
			reader = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (metaData->NextPageNo) * MemoryBuffer::pagesize;
			//��ȡ����Ԫ��Ϣ
			metaData = (MetaData*)(const_cast<uchar*>(reader));
			//��ȡ���ݲ��ֵĽ�����ַ
			limit = reader + metaData->usedSpace;
			reader += sizeof(MetaData);
			while (reader < limit) {
				Chunk::readID(reader, s);
				Chunk::readID(reader, o);
				Chunk::readID(reader, t);
				if (preLevel == 0 || patternBitmap[preVar][preLevel - 1]->contains(s)) {
					if (postLevel == 0 || patternBitmap[postVar][postLevel - 1]->contains(o)) {
						patternBitmap[preVar][preLevel]->insert(s);
						patternBitmap[postVar][postLevel]->insert(o);
						retBuffer->insertID(s);
						retBuffer->insertID(o);
						a = min(a, s);
						b = max(b, s);
						c = min(c, o);
						d = max(d, o);
					}
				}
			}
			if (metaData->pageNo == lastPage) {
				break;
			}
		}
	}


}

void PartitionMaster::findObjectIDByPredicate(EntityIDBuffer* retBuffer, const ID minID, const ID maxID, const uchar* startPtr, const int xyType) {

	if (minID == 0 && maxID == UINT_MAX) {
		findObjectIDByPredicate(retBuffer, startPtr, xyType);
		return;
	}
	retBuffer->setIDCount(1);
	retBuffer->setSortKey(0);

	register ID s, o, t;
	const uchar* reader, * limit, * chunkBegin = startPtr;

	ID& a = retBuffer->range[0].first;
	ID& b = retBuffer->range[0].second;
	ID& c = retBuffer->range[1].first;
	ID& d = retBuffer->range[1].second;

	MetaData* metaData = (MetaData*)chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = chunkBegin + metaData->usedSpace;

	while (reader < limit) {
		Chunk::readID(reader, s);
		Chunk::readID(reader, o);
		Chunk::readID(reader, t);
		if (o < minID) {
			continue;
		}
		else if (o <= maxID) {
			if (preLevel == 0) {
				patternBitmap[preVar][preLevel]->insert(o);
				a = min(a, o);
				b = max(b, o);
			}
			else {
				if (patternBitmap[preVar][preLevel - 1]->contains(o)) {
					patternBitmap[preVar][preLevel]->insert(o);
					a = min(a, o);
					b = max(b, o);
					if (isSingle) retBuffer->insertID(o);
				}
			}
		}
		else {
			break;
		}
	}

	if (metaData->pageNo != -1) {
		int lastPage = metaData->pageNo;//������Ŀ��
		while (metaData->NextPageNo != -1) {
			//��ȡ���ݿ����ʼ��ַ
			reader = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (metaData->NextPageNo) * MemoryBuffer::pagesize;
			//��ȡ����Ԫ��Ϣ
			metaData = (MetaData*)(const_cast<uchar*>(reader));
			//��ȡ���ݲ��ֵĽ�����ַ
			limit = reader + metaData->usedSpace;
			reader += sizeof(MetaData);
			while (reader < limit) {
				Chunk::readID(reader, s);
				Chunk::readID(reader, o);
				Chunk::readID(reader, t);
				if (o < minID) {
					continue;
				}
				else if (o <= maxID) {
					if (preLevel == 0) {
						patternBitmap[preVar][preLevel]->insert(o);
						a = min(a, o);
						b = max(b, o);
					}
					else {
						if (patternBitmap[preVar][preLevel - 1]->contains(o)) {
							patternBitmap[preVar][preLevel]->insert(o);
							a = min(a, o);
							b = max(b, o);
							if (isSingle) retBuffer->insertID(o);
						}
					}
				}
				else {
					break;
				}
			}
			if (metaData->pageNo == lastPage) {
				break;
			}
		}
	}

}

void PartitionMaster::findObjectIDByPredicate(EntityIDBuffer* retBuffer, const uchar* startPtr, const int xyType) {

	retBuffer->setIDCount(1);
	retBuffer->setSortKey(0);

	register ID s, o, t;
	const uchar* reader, * limit, * chunkBegin = startPtr;

	ID& a = retBuffer->range[0].first;
	ID& b = retBuffer->range[0].second;
	ID& c = retBuffer->range[1].first;
	ID& d = retBuffer->range[1].second;

	MetaData* metaData = (MetaData*)chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = reader + metaData->usedSpace;
	while (reader < limit) {
		Chunk::readID(reader, s);
		Chunk::readID(reader, o);
		Chunk::readID(reader, t);
		if (preLevel == 0) {
			patternBitmap[preVar][preLevel]->insert(o);
			a = min(a, o);
			b = max(b, o);
		}
		else {
			if (patternBitmap[preVar][preLevel - 1]->contains(o)) {
				patternBitmap[preVar][preLevel]->insert(o);
				a = min(a, o);
				b = max(b, o);
				if (isSingle) retBuffer->insertID(o);
			}
		}
	}



	if (metaData->pageNo != -1) {
		int lastPage = metaData->pageNo;//������Ŀ��
		while (metaData->NextPageNo != -1) {
			//��ȡ���ݿ����ʼ��ַ
			reader = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (metaData->NextPageNo) * MemoryBuffer::pagesize;
			//��ȡ����Ԫ��Ϣ
			metaData = (MetaData*)(const_cast<uchar*>(reader));
			//��ȡ���ݲ��ֵĽ�����ַ
			limit = reader + metaData->usedSpace;
			reader += sizeof(MetaData);
			while (reader < limit) {
				Chunk::readID(reader, s);
				Chunk::readID(reader, o);
				Chunk::readID(reader, t);
				if (preLevel == 0) {
					patternBitmap[preVar][preLevel]->insert(o);
					a = min(a, o);
					b = max(b, o);
				}
				else {
					if (patternBitmap[preVar][preLevel - 1]->contains(o)) {
						patternBitmap[preVar][preLevel]->insert(o);
						a = min(a, o);
						b = max(b, o);
						if (isSingle) retBuffer->insertID(o);
					}
				}
			}
			if (metaData->pageNo == lastPage) {
				break;
			}
		}
	}
}

void PartitionMaster::findSubjectIDByPredicate(EntityIDBuffer* retBuffer, const ID minID, const ID maxID, const uchar* startPtr, const int xyType) {

	if (minID == 0 && maxID == UINT_MAX) {
		findSubjectIDByPredicate(retBuffer, startPtr, xyType);
		return;
	}
	retBuffer->setIDCount(1);
	retBuffer->setSortKey(0);

	register ID s, o, t;
	const uchar* reader, * limit, * chunkBegin = startPtr;

	ID& a = retBuffer->range[0].first;
	ID& b = retBuffer->range[0].second;
	ID& c = retBuffer->range[1].first;
	ID& d = retBuffer->range[1].second;

	MetaData* metaData = (MetaData*)chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = chunkBegin + metaData->usedSpace;

	while (reader < limit) {
		Chunk::readID(reader, s);
		Chunk::readID(reader, o);
		Chunk::readID(reader, t);
		if (s < minID) {
			continue;
		}
		else if (s <= maxID) {
			if (preLevel == 0) {
				patternBitmap[preVar][preLevel]->insert(s);
				a = min(a, s);
				b = max(b, s);
			}
			else {
				if (patternBitmap[preVar][preLevel - 1]->contains(s)) {
					patternBitmap[preVar][preLevel]->insert(s);
					a = min(a, s);
					b = max(b, s);
					if (isSingle) retBuffer->insertID(s);
				}
			}
		}
		else {
			break;
		}
	}

	if (metaData->pageNo != -1) {
		int lastPage = metaData->pageNo;//������Ŀ��
		while (metaData->NextPageNo != -1) {
			//��ȡ���ݿ����ʼ��ַ
			reader = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (metaData->NextPageNo) * MemoryBuffer::pagesize;
			//��ȡ����Ԫ��Ϣ
			metaData = (MetaData*)(const_cast<uchar*>(reader));
			//��ȡ���ݲ��ֵĽ�����ַ
			limit = reader + metaData->usedSpace;
			reader += sizeof(MetaData);
			while (reader < limit) {
				Chunk::readID(reader, s);
				Chunk::readID(reader, o);
				Chunk::readID(reader, t);
				if (s < minID) {
					continue;
				}
				else if (s <= maxID) {
					if (preLevel == 0) {
						patternBitmap[preVar][preLevel]->insert(s);
						a = min(a, s);
						b = max(b, s);
					}
					else {
						if (patternBitmap[preVar][preLevel - 1]->contains(s)) {
							patternBitmap[preVar][preLevel]->insert(s);
							a = min(a, s);
							b = max(b, s);
							if (isSingle) retBuffer->insertID(s);
						}
					}
				}
				else {
					break;
				}
			}
			if (metaData->pageNo == lastPage) {
				break;
			}
		}
	}
}

void PartitionMaster::findSubjectIDByPredicate(EntityIDBuffer* retBuffer, const uchar* startPtr, const int xyType) {

	retBuffer->setIDCount(1);
	retBuffer->setSortKey(0);

	register ID s, o, t;
	const uchar* reader, * limit, * chunkBegin = startPtr;

	ID& a = retBuffer->range[0].first;
	ID& b = retBuffer->range[0].second;
	ID& c = retBuffer->range[1].first;
	ID& d = retBuffer->range[1].second;

	MetaData* metaData = (MetaData*)chunkBegin;
	reader = chunkBegin + sizeof(MetaData);
	limit = reader + metaData->usedSpace;
	while (reader < limit) {
		Chunk::readID(reader, s);
		Chunk::readID(reader, o);
		Chunk::readID(reader, t);
		if (preLevel == 0) {
			patternBitmap[preVar][preLevel]->insert(s);
			a = min(a, s);
			b = max(b, s);
		}
		else {
			if (patternBitmap[preVar][preLevel - 1]->contains(s)) {
				patternBitmap[preVar][preLevel]->insert(s);
				a = min(a, s);
				b = max(b, s);
				if (isSingle) retBuffer->insertID(s);
			}
		}
	}

	if (metaData->pageNo != -1) {
		int lastPage = metaData->pageNo;//������Ŀ��
		while (metaData->NextPageNo != -1) {
			//��ȡ���ݿ����ʼ��ַ
			reader = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (metaData->NextPageNo) * MemoryBuffer::pagesize;
			//��ȡ����Ԫ��Ϣ
			metaData = (MetaData*)(const_cast<uchar*>(reader));
			//��ȡ���ݲ��ֵĽ�����ַ
			limit = reader + metaData->usedSpace;
			reader += sizeof(MetaData);
			while (reader < limit) {
				Chunk::readID(reader, s);
				Chunk::readID(reader, o);
				Chunk::readID(reader, t);
				if (preLevel == 0) {
					patternBitmap[preVar][preLevel]->insert(s);
					a = min(a, s);
					b = max(b, s);
				}
				else {
					if (patternBitmap[preVar][preLevel - 1]->contains(s)) {
						patternBitmap[preVar][preLevel]->insert(s);
						a = min(a, s);
						b = max(b, s);
						if (isSingle) retBuffer->insertID(s);
					}
				}
			}
			if (metaData->pageNo == lastPage) {
				break;
			}
		}
	}

}

