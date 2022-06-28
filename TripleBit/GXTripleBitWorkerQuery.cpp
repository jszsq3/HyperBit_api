/*
 * GXTripleBitWorkerQuery.cpp
 *
 *  Created on: 2013-7-1
 *      Author: root
 */

#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "GXTripleBitWorkerQuery.h"
#include "GXTripleBitRepository.h"
#include "URITable.h"
#include "PredicateTable.h"
#include "TripleBitQueryGraph.h"
#include "EntityIDBuffer.h"
#include "util/BufferManager.h"
#include "comm/TasksQueueWP.h"
#include "comm/ResultBuffer.h"
#include "comm/IndexForTT.h"
#include "comm/Tasks.h"
#include <algorithm>
#include <math.h>
#include <pthread.h>
#include "util/Timestamp.h"
#include "CBitMap.h"

#define BLOCK_SIZE 1024
//#define PRINT_RESULT
#define COLDCACHE
//#define MYDEBUG

triplebit::atomic<size_t> GXTripleBitWorkerQuery::timeCount(0);
triplebit::atomic<size_t> GXTripleBitWorkerQuery::tripleCount(0);


//初始化
GXTripleBitWorkerQuery::GXTripleBitWorkerQuery(GXTripleBitRepository*& repo, ID workID) {
	tripleBitRepo = repo;
	bitmap = repo->getBitmapBuffer();
	uriTable = repo->getURITable();
	preTable = repo->getPredicateTable();

	workerID = workID; // 1 ~ WORKERNUM

	tasksQueueWP = repo->getTasksQueueWP();
	resultWP = repo->getResultWP();
	tasksQueueWPMutex = repo->getTasksQueueWPMutex();
	partitionNum = repo->getPartitionNum();

	for (int partitionID = 1; partitionID <= partitionNum; ++partitionID) {
		resultBuffer[partitionID] = resultWP[(workerID - 1) * partitionNum + partitionID - 1];

		chunkManager[ORDERBYS][partitionID] = bitmap->getChunkManager(partitionID, ORDERBYS);
		chunkManager[ORDERBYO][partitionID] = bitmap->getChunkManager(partitionID, ORDERBYO);

		vector<GXTempBuffer*> tempBufferByS;
		GXTempBuffer* tempBuffer;

		vector<GXTasksQueueChunk*> tasksQueueChunkS; //
		GXTasksQueueChunk* tasksQueueChunk; //

		const uchar* startPtr = chunkManager[ORDERBYS][partitionID]->getStartPtr();
		startPtr -= sizeof(ChunkManagerMeta);
		tempBuffer = new GXTempBuffer(partitionID, 0, ORDERBYS);
		tempBufferByS.push_back(tempBuffer);
		chunkStartPtr[ORDERBYS][partitionID].push_back(startPtr + sizeof(ChunkManagerMeta));

		tasksQueueChunk = new GXTasksQueueChunk(startPtr + sizeof(ChunkManagerMeta), 0, ORDERBYS); //
		tasksQueueChunkS.push_back(tasksQueueChunk); //

		for (size_t chunkID = 1; chunkID < chunkManager[ORDERBYS][partitionID]->getChunkNumber(); chunkID++) {
			tempBuffer = new GXTempBuffer(partitionID, chunkID, ORDERBYS);
			tempBufferByS.push_back(tempBuffer);
			chunkStartPtr[ORDERBYS][partitionID].push_back(startPtr + chunkID * MemoryBuffer::pagesize);

			tasksQueueChunk = new GXTasksQueueChunk(startPtr + chunkID * MemoryBuffer::pagesize, chunkID, ORDERBYS); //
			tasksQueueChunkS.push_back(tasksQueueChunk); //
		}
		chunkTempBuffer[ORDERBYS][partitionID] = tempBufferByS;

		chunkQueue[ORDERBYS][partitionID] = tasksQueueChunkS; //

		vector<GXTempBuffer*> tempBufferByO;
		vector<GXTasksQueueChunk*> tasksQueueChunkO; //

		startPtr = chunkManager[ORDERBYO][partitionID]->getStartPtr();
		startPtr -= sizeof(ChunkManagerMeta);
		tempBuffer = new GXTempBuffer(partitionID, 0, ORDERBYO);
		tempBufferByO.push_back(tempBuffer);
		chunkStartPtr[ORDERBYO][partitionID].push_back(startPtr + sizeof(ChunkManagerMeta));

		tasksQueueChunk = new GXTasksQueueChunk(startPtr + sizeof(ChunkManagerMeta), 0, ORDERBYO); //
		tasksQueueChunkO.push_back(tasksQueueChunk); //

		for (size_t chunkID = 1; chunkID < chunkManager[ORDERBYO][partitionID]->getChunkNumber(); chunkID++) {
			tempBuffer = new GXTempBuffer(partitionID, chunkID, ORDERBYO);
			tempBufferByO.push_back(tempBuffer);
			chunkStartPtr[ORDERBYO][partitionID].push_back(startPtr + chunkID * MemoryBuffer::pagesize);

			tasksQueueChunk = new GXTasksQueueChunk(startPtr + chunkID * MemoryBuffer::pagesize, chunkID, ORDERBYO); //
			tasksQueueChunkO.push_back(tasksQueueChunk); //
		}
		chunkTempBuffer[ORDERBYO][partitionID] = tempBufferByO;

		chunkQueue[ORDERBYO][partitionID] = tasksQueueChunkO; //
	}
}

GXTripleBitWorkerQuery::~GXTripleBitWorkerQuery() {
	for (int soType = 0; soType < 2; soType++) {
		for (ID predicateID = 1; predicateID <= partitionNum; predicateID++) {
			for (ID chunkID = 0; chunkID < chunkManager[soType][predicateID]->getChunkNumber(); chunkID++) {
				delete chunkTempBuffer[soType][predicateID][chunkID];
				chunkTempBuffer[soType][predicateID][chunkID] = NULL;
				delete chunkQueue[soType][predicateID][chunkID];
				chunkQueue[soType][predicateID][chunkID] = NULL;
				chunkStartPtr[soType][predicateID][chunkID] = NULL;
			}
			chunkManager[soType][predicateID] = NULL;
		}
	}

	uriTable = NULL;
	preTable = NULL;
	bitmap = NULL;
	tripleBitRepo = NULL;
	EntityIDList.clear();
}

void GXTripleBitWorkerQuery::releaseBuffer() {
	idTreeBFS.clear();
	leafNode.clear();
	varVec.clear();

	EntityIDListIterType iter = EntityIDList.begin();

	for (; iter != EntityIDList.end(); iter++) {
		if (iter->second != NULL)
			BufferManager::getInstance()->freeBuffer(iter->second);
	}

	BufferManager::getInstance()->reserveBuffer();
	EntityIDList.clear();
}

//一个工作的执行函数
Status GXTripleBitWorkerQuery::query(TripleBitQueryGraph* queryGraph, vector<string>& resultSet, timeval& transTime) {
	this->_queryGraph = queryGraph;
	this->_query = &(queryGraph->getQuery());
	this->resultPtr = &resultSet;
	this->transactionTime = &transTime;

	switch (_queryGraph->getOpType()) {
	case TripleBitQueryGraph::INSERT_DATA:
		return excuteInsertData();
	}
	return OK;
}

//缓冲区数据持久化到磁盘
void GXTripleBitWorkerQuery::endForWork() {

	tempBuffers.clear();
	vector<GXTempBuffer*>().swap(tempBuffers);

	for (size_t predicateID = 1; predicateID <= partitionNum; ++predicateID) {
		GXTempBuffer* tempBuffer;
		for (size_t chunkID = 0; chunkID < chunkManager[ORDERBYS][predicateID]->getChunkNumber(); chunkID++) {
			tempBuffer = chunkTempBuffer[ORDERBYS][predicateID][chunkID];
			if (tempBuffer != NULL && tempBuffer->getSize()) {
				tempBuffers.push_back(tempBuffer);
			}
		}

		for (size_t chunkID = 0; chunkID < chunkManager[ORDERBYO][predicateID]->getChunkNumber(); chunkID++) {
			tempBuffer = chunkTempBuffer[ORDERBYO][predicateID][chunkID];
			if (tempBuffer != NULL && tempBuffer->getSize()) {
				tempBuffers.push_back(tempBuffer);
			}
		}
	}

	int start, end;
	int persize = (tempBuffers.size() % gthread == 0) ? (tempBuffers.size() / gthread) : (tempBuffers.size() / gthread + 1);

	for (int i = 0; i < gthread; i++) {
		start = persize * i;
		end = (start + persize > tempBuffers.size()) ? tempBuffers.size() : (start + persize);
		ThreadPool::getSubTranPool()[i]->addTask(boost::bind(&GXTripleBitWorkerQuery::executeEndForWorkTaskPartition, this, boost::ref(tempBuffers), start, end));
	}
}

void GXTripleBitWorkerQuery::executeEndForWorkTaskPartition(vector<GXTempBuffer*> &tempBuffers, int start, int end) {
	for (int i = start; i < end; i++) {
		//cout << "GXTripleBitWorkerQuery::executeEndForWorkTaskPartition" << endl;
		combineTempBufferToSource(tempBuffers[i]);
	}
}

//排序
void GXTripleBitWorkerQuery::sortDataBase() {
	SortChunkTask *sortChunkTask = NULL;

	sortTask.clear();
	vector<SortChunkTask *>().swap(sortTask);

	for (int soType = 0; soType < 2; soType++) {
		for (int predicateID = 1; predicateID <= partitionNum; ++predicateID) {
			for (size_t chunkID = 0; chunkID < chunkManager[soType][predicateID]->getChunkNumber(); chunkID++) {
				MetaData* metaData = (MetaData*)(chunkStartPtr[soType][predicateID][chunkID]);
				if (metaData->updateCount) {
					sortChunkTask = new SortChunkTask(chunkStartPtr[soType][predicateID][chunkID], predicateID, chunkID, soType);
					sortTask.push_back(sortChunkTask);
					metaData->updateCount = 0;
				}
			}

		}
	}

	int start, end;
	int persize = (sortTask.size() % gthread == 0) ? (sortTask.size() / gthread) : (sortTask.size() / gthread + 1);
	for (int i = 0; i < gthread; i++) {
		start = persize * i;
		end = (start + persize > sortTask.size()) ? sortTask.size() : (start + persize);
		ThreadPool::getSubTranPool()[i]->addTask(boost::bind(&GXTripleBitWorkerQuery::executeSortChunkTaskPartition, this, boost::ref(sortTask), start, end));
	}

}

void GXTripleBitWorkerQuery::executeSortChunkTaskPartition(const vector<SortChunkTask *>&sortTask, int start, int end) {
	for (int i = start; i < end; i++) {
		executeSortChunkTask(sortTask[i]);
	}
}

//块级别重排
void GXTripleBitWorkerQuery::executeSortChunkTask(SortChunkTask* sortChunkTask) {
	const uchar* startPtr = sortChunkTask->startPtr;//第一个数据块的开始指针(指向第一个数据块的元数据)
	ID chunkID = sortChunkTask->chunkID;
	ID predicateID = sortChunkTask->predicateID;
	bool soType = sortChunkTask->soType;
	ID subjectID;
	ID objectID;
	ID timeID;
	uint len;
	ID lastSubjectID;
	ID lastObjectID;
	ID lastTimeID;
	TempFile sorted("./sort");
	MetaData* metaData;
	MetaData* metaDataFirst;


	//读数据
	const uchar* readStartPtr = startPtr;//数据块的开始指针
	metaData = (MetaData*)readStartPtr;
	const uchar* read = readStartPtr + sizeof(MetaData);//块的数据区开始指针
	const uchar* readEnd = readStartPtr + metaData->usedSpace;//块的数据区结束指针
	//读取所有块的内容 
	//第一个块
	while (read < readEnd) {
		chunkManager[0][predicateID]->read(read, subjectID, objectID,timeID);
		if (subjectID == 0) {
			continue;
		}
		sorted.writeID(subjectID);
		sorted.writeID(objectID);
		sorted.writeID(timeID);
	}
	//其他块
	while (metaData->NextPageNo!=-1) {
		readStartPtr = TempMMapBuffer::getInstance().getAddress() + (metaData->NextPageNo) * MemoryBuffer::pagesize;
		metaData = (MetaData*)readStartPtr;
		read = readStartPtr + sizeof(MetaData);
		readEnd = readStartPtr + metaData->usedSpace;
		while (read < readEnd) {
			chunkManager[0][predicateID]->read(read, subjectID, objectID, timeID);
			if (subjectID == 0) {
				continue;
			}
			sorted.writeID(subjectID);
			sorted.writeID(objectID);
			sorted.writeID(timeID);
		}
	}

	//排序
	TempFile tempSorted("./tempSort");
	if (soType == ORDERBYS) {
		Sorter::sort(sorted, tempSorted, TempFile::skipIDID, TempFile::compare12);
	}
	else if (soType == ORDERBYO) {
		Sorter::sort(sorted, tempSorted, TempFile::skipIDID, TempFile::compare21);
	}
	else {
		cout << "GXTripleBitWorkerQuery::executeSortChunkTask error" << endl;
	}


	//写数据
	uchar* writeStartPtr = const_cast<uchar*>(startPtr);//数据块的开始指针
	//取metaData
	metaData = (MetaData*)writeStartPtr;
	//记录第一个数据块的元数据
	metaDataFirst = metaData;
	//重置metaData
	metaDataFirst->pageNo = -1;
	metaData->usedSpace = sizeof(MetaData);
	metaData->tripleCount = 0;
	metaData->updateCount = 0;
	uchar* write;//数据块的数据区的开始指针
	//设置write指针
	write = writeStartPtr + sizeof(MetaData);
	uchar* writeEnd;//数据块的结尾指针
	//设置writeEnd指针
	if (chunkID == 0) {
		writeEnd = writeStartPtr - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
	}
	else {
		writeEnd = writeStartPtr + MemoryBuffer::pagesize;
	}
	//设置文件指针
	MemoryMappedFile mappedIn;
	assert(mappedIn.open(tempSorted.getFile().c_str()));
	const uchar* fileRead = mappedIn.getBegin();
	const uchar* fileReadEnd = mappedIn.getEnd();
	//第一次插入
	while (fileRead < fileReadEnd) {
		TempFile::readIDIDID(fileRead, subjectID, objectID,timeID);//fileRead自动后移
		if (subjectID == 0) {
			continue;
		}
		lastSubjectID = subjectID;
		lastObjectID = objectID;
		lastTimeID = timeID;
		len = sizeof(ID) * 3;
		//插入数据
		chunkManager[soType][predicateID]->write(write, subjectID, objectID,timeID);
		//更新metaData
		metaData->usedSpace += len;
		metaData->tripleCount++;
		break;
	}
	//继续插入
	while (fileRead < fileReadEnd) {
		TempFile::readIDIDID(fileRead, subjectID, objectID, timeID);//fileRead自动后移
		if (subjectID == 0) {
			continue;
		}
		if (subjectID == lastSubjectID && objectID == lastObjectID) {
			continue;
		}
		lastSubjectID = subjectID;
		lastObjectID = objectID;
		lastTimeID = timeID;
		len = sizeof(ID) * 3;
		if (write + len > writeEnd) {
			assert(metaData->NextPageNo != -1);
			writeStartPtr = TempMMapBuffer::getInstance().getAddress() + (metaData->NextPageNo) * MemoryBuffer::pagesize;
			metaData = (MetaData*)writeStartPtr;
			//重置metaData
			metaData->usedSpace = sizeof(MetaData);
			metaData->tripleCount = 0;
			metaData->updateCount = 0;
			metaDataFirst->pageNo = metaData->pageNo;
			write = writeStartPtr + sizeof(MetaData);
			writeEnd = writeStartPtr + MemoryBuffer::pagesize;
		}
		//插入数据
		chunkManager[soType][predicateID]->write(write, subjectID, objectID,timeID);//write自动后移
		//更新metaData
		metaData->usedSpace += len;
		metaData->tripleCount++;
	}

	tempSorted.discard();
	sorted.discard();

	delete sortChunkTask;
	sortChunkTask = NULL;
}

//插入数据(整个数据)
//并行
Status GXTripleBitWorkerQuery::excuteInsertData() {
	//插入数据的三元组数目
	tripleSize = _query->GXtripleNodes.size();

	//cout << "insert number: " << tripleSize << endl;

	//把插入数据按谓词分区
	classifyTripleNode();



	//size_t needPage = 2 * (tripleSize / ((MemoryBuffer::pagesize - sizeof(MetaData)) / (3 * sizeof(ID))) + 1+bitmap->usedPageByS);
	//size_t needPage = 60000;

	size_t needPage = 2 * (tripleSize / ((MemoryBuffer::pagesize - sizeof(MetaData)) / (3 * sizeof(ID))) + 1) + TempMMapBuffer::getInstance().getBuildUsedPage();
	needPage = needPage - (TempMMapBuffer::getInstance().getLength() / MemoryBuffer::pagesize - TempMMapBuffer::getInstance().getUsedPage());
	TempMMapBuffer::getInstance().checkSize(needPage);

	//开始时间
	struct timeval start, end;
	gettimeofday(&start, NULL);


	//插入数据
	for (auto & pair_triple : tripleNodeMap) {
		ThreadPool::getSubTranPool()[pair_triple.first % gthread]->addTask( boost::bind(&GXTripleBitWorkerQuery::executeInsertData, this, boost::ref(pair_triple.second)));
	}
	ThreadPool::waitSubTranThread();

	//buffer缓冲区数据持久化
	endForWork();
	ThreadPool::waitSubTranThread();

	//结束时间
	gettimeofday(&end, NULL);
	//cout << "TempInsert Time: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << "s" << endl;

	//bitmap->print(2);

	//插入数据排序
	sortDataBase();
	ThreadPool::waitSubTranThread();

	return OK;
}

//插入一个谓词的数据
//GXtripleNodes = 同一个谓词的 vector<GXTripleNode*>
void GXTripleBitWorkerQuery::executeInsertData(const vector<GXTripleNode*> & GXtripleNodes) {
	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::INSERT_DATA;
	timeval start;//, end;

	size_t chunkID;
	GXChunkTask* chunkTask = new GXChunkTask[GXtripleNodes.size() * 2];
	int i = 0;
	gettimeofday(&start, NULL);
	for (auto & triplenode : GXtripleNodes) {
		chunkID = chunkManager[ORDERBYS][triplenode->predicateID]->getChunkIndex()->lowerfind(make_pair(triplenode->subjectID, triplenode->objectID));
		chunkTask[i].setChunkTask(start, operationType, *triplenode, ORDERBYS, chunkID);
		taskEnQueue(&chunkTask[i], chunkQueue[ORDERBYS][triplenode->predicateID][chunkID]);
		i++;

		chunkID = chunkManager[ORDERBYO][triplenode->predicateID]->getChunkIndex()->lowerfind(make_pair(triplenode->objectID, triplenode->subjectID));
		chunkTask[i].setChunkTask(start, operationType, *triplenode, ORDERBYO, chunkID);
		taskEnQueue(&chunkTask[i], chunkQueue[ORDERBYO][triplenode->predicateID][chunkID]);
		i++;
	}
	chunkTasksMutex.lock();
	chunkTasks.push_back(chunkTask);
	chunkTasksMutex.unlock();
}

//块级别并行
void GXTripleBitWorkerQuery::taskEnQueue(GXChunkTask *chunkTask, GXTasksQueueChunk *tasksQueue) {
	if (tasksQueue->isEmpty()) {
		tasksQueue->EnQueue(chunkTask);
		ThreadPool::getSubTranPool()[chunkTask->Triple.predicateID % gthread]->addTask( boost::bind(&GXTripleBitWorkerQuery::handleTasksQueueChunk, this, tasksQueue));
	} else {
		tasksQueue->EnQueue(chunkTask);
	}
}

void GXTripleBitWorkerQuery::handleTasksQueueChunk(GXTasksQueueChunk* tasksQueue) {
	GXChunkTask* chunkTask = NULL;
	while ((chunkTask = tasksQueue->Dequeue()) != NULL) {
		switch (chunkTask->operationType) {
		case TripleBitQueryGraph::INSERT_DATA:
			executeInsertChunkData(chunkTask);
			break;
		default:
			break;
		}
	}

}
//插入一条数据
void GXTripleBitWorkerQuery::executeInsertChunkData(GXChunkTask *chunkTask) {
	ID predicateID = chunkTask->Triple.predicateID;
	ID chunkID = chunkTask->chunkID;
	bool opSO = chunkTask->opSO;

	chunkTempBuffer[opSO][predicateID][chunkID]->insertTriple(chunkTask->Triple.subjectID, chunkTask->Triple.objectID,chunkTask->Triple.timeID);
	if (chunkTempBuffer[opSO][predicateID][chunkID]->isFull()) {
		//combine the data in tempbuffer into the source data
		combineTempBufferToSource(chunkTempBuffer[opSO][predicateID][chunkID]);
	}
}

//插入数据
void GXTripleBitWorkerQuery::combineTempBufferToSource(GXTempBuffer *buffer) {
	assert(buffer != NULL);
	ID predicateID = buffer->getPredicateID();
	ID chunkID = buffer->getChunkID();
	bool soType = buffer->getSOType();
	buffer->sort(soType);
	buffer->uniqe();
	uchar* currentPtrChunk, * endPtrChunk, * startPtrChunk;//数据块指针
	//初始化块指针
	startPtrChunk = const_cast<uchar*>(chunkStartPtr[soType][predicateID][chunkID]);
	if (chunkID == 0) {
		endPtrChunk = startPtrChunk - sizeof(ChunkManagerMeta) + MemoryBuffer::pagesize;
	} else {
		endPtrChunk = startPtrChunk + MemoryBuffer::pagesize;
	}
	MetaData *metaData = (MetaData *) startPtrChunk;
	MetaData * metaDataFirst = metaData;

	//更新原始块信息
	metaDataFirst->updateCount = 1;

	currentPtrChunk = startPtrChunk + metaData->usedSpace;

		
	//待插入数据指针
	GXChunkTriple * readBuffer = buffer->getBuffer(), *endBuffer = buffer->getEnd();
	//插入三元组长度
	uint len;
	//如果已经有下一块，直接定位到最后一块
	if (metaData->pageNo != -1) {
		startPtrChunk = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress())  + (metaData->pageNo) * MemoryBuffer::pagesize;
		endPtrChunk = startPtrChunk + MemoryBuffer::pagesize;
		metaData = (MetaData *) startPtrChunk;
		currentPtrChunk = startPtrChunk + metaData->usedSpace;
	}
	//在最后一块插入数据
	while (readBuffer < endBuffer) {
		len = sizeof(ID) * 3;
		//块满申请新块
		if (currentPtrChunk + len > endPtrChunk) {
			size_t pageNo;
			bool newchunk = false;
			if (metaData->NextPageNo != -1) {
				pageNo = metaData->NextPageNo;
				newchunk = false;
			}
			else {
				TempMMapBuffer::getInstance().getPage(pageNo);
				newchunk = true;
			}
			metaDataFirst->pageNo = pageNo;
			startPtrChunk = reinterpret_cast<uchar*>(TempMMapBuffer::getInstance().getAddress()) + (pageNo)*MemoryBuffer::pagesize;
			endPtrChunk = startPtrChunk + MemoryBuffer::pagesize;
			if (newchunk) {
				//更新上一个块的元信息
				metaData->NextPageNo = pageNo;
				//初始化新申请块的元信息
				metaData = (MetaData*)startPtrChunk;
				metaData->pageNo = pageNo;
				metaData->NextPageNo = -1;
				metaData->usedSpace = sizeof(MetaData);
				metaData->tripleCount = 0;
				metaData->updateCount = 0;
				currentPtrChunk = startPtrChunk + sizeof(MetaData);
			}
			else {
				//重置数据块的元信息
				metaData = (MetaData*)startPtrChunk;
				metaData->usedSpace = sizeof(MetaData);
				metaData->tripleCount = 0;
				metaData->updateCount = 0;
				currentPtrChunk = startPtrChunk + sizeof(MetaData);
			}

		}
		//插入数据
		chunkManager[soType][predicateID]->write(currentPtrChunk, readBuffer->subjectID, readBuffer->objectID,readBuffer->timeID);
		//更新metaData
		metaData->usedSpace += len;
		metaData->tripleCount++;
		//更新循环变量
		readBuffer++;
		
	}
	//清空buffer
	buffer->clear();
}

void GXTripleBitWorkerQuery::classifyTripleNode() {
	tripleNodeMap.clear();
	for (vector<GXTripleNode>::iterator iter = _query->GXtripleNodes.begin(); iter != _query->GXtripleNodes.end(); ++iter) {
		tripleNodeMap[iter->predicateID].push_back(&(*iter));
	}
}



//----------------------------------------//

#define PRINT_RESULT
#define MYDEBUG
//#define CUDATEST

//extern var define==================
ID xMin = UINT_MAX;
ID xMax = 0;
ID zMin = UINT_MAX;
ID zMax = 0;

vector<vector<CBitMap*> > patternBitmap;
map<ID, pair<ID, ID> > varRange;

ID preVar;
ID postVar;
int preLevel;
int postLevel;
bool isSingle = false;
//===================================

void to_reduce_p(EntityIDBuffer* idb) {
	size_t total = idb->getSize();
	size_t j = 0;
	size_t i;

	ID* p = idb->buffer;
	size_t IDCount = idb->getIDCount();

	int k;

	for (i = 0; i != total; i++)
	{
		// if(fountain[1][p[(i * IDCount + 0)]&xflag].find_free(p[(i * IDCount + 0)],pval) && *pval == 2){
		if (patternBitmap[preVar][patternBitmap[preVar].size() - 1]->contains(p[i * IDCount + 0]) &&
			patternBitmap[postVar][patternBitmap[postVar].size() - 1]->contains(p[i * IDCount + 1])) {
			for (k = 0; k < IDCount; k++)
			{
				p[j * IDCount + k] = p[i * IDCount + k];
			}
			j++;
		}
	}
	idb->setSize(j);
}

void doGroupReduce(vector<EntityIDBuffer*>& eds, int begin, int end) {
	/*
	for(int i = begin; i < end; i++){
		to_reduce_p(eds[i]);
	}
	*/
	for (int i = begin; i <= end; i++) {
		to_reduce_p(eds[i]);
	}
}

void reduce_p(ResultIDBuffer* res) {
	map<ID, EntityIDBuffer*>::iterator iter;
	vector<EntityIDBuffer*> eds;

	for (iter = res->taskPackage->xTempBuffer.begin(); iter != res->taskPackage->xTempBuffer.end(); iter++) {
		// totalSize += iter->second->getSize();
		eds.push_back(iter->second);
	}

	for (iter = res->taskPackage->xyTempBuffer.begin(); iter != res->taskPackage->xyTempBuffer.end(); iter++) {
		// totalSize += iter->second->getSize();
		eds.push_back(iter->second);
	}

	// #pragma omp parallel for num_threads(openMP_thread_num)
	int _chunkCount = 16;
	size_t chunkSize = eds.size() / _chunkCount;
	int i = 0;
	for (i = 0; i < _chunkCount; i++)
	{
		if (i == _chunkCount - 1) {
			CXThreadPool::getScanjoinPool().addTask(boost::bind(&doGroupReduce, eds, i * chunkSize, eds.size() - 1));
		}
		else {

			CXThreadPool::getScanjoinPool().addTask(boost::bind(&doGroupReduce, eds, i * chunkSize, i * chunkSize + chunkSize - 1));
		}
	}
	CXThreadPool::getScanjoinPool().wait();
}

CXTripleBitWorkerQuery::CXTripleBitWorkerQuery(CXTripleBitRepository*& repo, ID workID) {
	tripleBitRepo = repo;
	bitmap = repo->getBitmapBuffer();
	uriTable = repo->getURITable();
	preTable = repo->getPredicateTable();

	workerID = workID; // 1 ~ WORKERNUM
	max_id = repo->getURITable()->getMaxID();
	tasksQueueWP = repo->getTasksQueueWP();
	resultWP = repo->getResultWP();
	tasksQueueWPMutex = repo->getTasksQueueWPMutex();
	partitionNum = repo->getPartitionNum();

	for (int partitionID = 1; partitionID <= partitionNum; ++partitionID) {
		tasksQueue[partitionID] = tasksQueueWP[partitionID - 1];
	}

	for (int partitionID = 1; partitionID <= partitionNum; ++partitionID) {
		resultBuffer[partitionID] = resultWP[(workerID - 1) * partitionNum + partitionID - 1];
	}
}

CXTripleBitWorkerQuery::~CXTripleBitWorkerQuery() {
	EntityIDList.clear();
}

void CXTripleBitWorkerQuery::releaseBuffer() {
	idTreeBFS.clear();
	leafNode.clear();
	varVec.clear();

	EntityIDListIterType iter = EntityIDList.begin();

	for (; iter != EntityIDList.end(); iter++) {
		if (iter->second != NULL)
			BufferManager::getInstance()->freeBuffer(iter->second);
	}

	BufferManager::getInstance()->reserveBuffer();
	EntityIDList.clear();
}

Status CXTripleBitWorkerQuery::query(int mod, TripleBitQueryGraph* queryGraph, vector<string>& resultSet, timeval& transTime) {
	this->_queryGraph = queryGraph;
	this->_query = &(queryGraph->getQuery());
	this->resultPtr = &resultSet;
	this->transactionTime = &transTime;

	//普通图查询
	if (mod == 0) {
		switch (_queryGraph->getOpType()) {
		case TripleBitQueryGraph::QUERY:
			return excuteQuery();
		}
		return OK;
	}
	else if (mod == 1) {
	}
	
}

Status CXTripleBitWorkerQuery::query1(int mod, TripleBitQueryGraph* queryGraph, vector<string>& resultSet, timeval& transTime) {
	this->_queryGraph = queryGraph;
	this->_query = &(queryGraph->getQuery());
	this->resultPtr = &resultSet;
	this->transactionTime = &transTime;

	//普通图查询
	if (mod == 0) {
		switch (_queryGraph->getOpType()) {
		case TripleBitQueryGraph::QUERY:
			return excuteQuery1();
		}
		return OK;
	}
	else if (mod == 1) {
	}
	
}

//查询入口函数
Status CXTripleBitWorkerQuery::excuteQuery() {
	if (_query->joinVariables.size() == 1) {
#ifdef MYDEBUG
		// cout << "execute centerJoin" << endl;
#endif
		centerJoin();
	}
	else {
		if (_query->joinGraph == TripleBitQueryGraph::ACYCLIC) {
#ifdef MYDEBUG
			// cout << "execute linerJoin" << endl;
#endif
			linerJoin();
		}
		else if (_query->joinGraph == TripleBitQueryGraph::CYCLIC) {
#ifdef MYDEBUG
			// cout << "execute treeShapeJoin" << endl;
#endif
			treeShapeJoin();
		}
	}
	return OK;
}

Status CXTripleBitWorkerQuery::excuteQuery1() {
	if (_query->joinVariables.size() == 1) {
#ifdef MYDEBUG
		cout << "execute centerJoin" << endl;
#endif
		centerJoin1();
	}
// 	else {
// 		if (_query->joinGraph == TripleBitQueryGraph::ACYCLIC) {
// #ifdef MYDEBUG
// 			cout << "execute linerJoin" << endl;
// #endif
// 			linerJoin();
// 		}
// 		else if (_query->joinGraph == TripleBitQueryGraph::CYCLIC) {
// #ifdef MYDEBUG
// 			cout << "execute treeShapeJoin" << endl;
// #endif
// 			treeShapeJoin();
// 		}
// 	}
// 	return OK;
}

void CXTripleBitWorkerQuery::tasksEnQueue(ID partitionID, CXSubTrans* subTrans) {
	// cout << "pID: " << partitionID << endl;
	if (tasksQueue[partitionID]->Queue_Empty()) {
		tasksQueue[partitionID]->EnQueue(subTrans);
		CXThreadPool::getPartitionPool().addTask(boost::bind(&PartitionMaster::Work, tripleBitRepo->getPartitionMaster(partitionID)));
	}
	else {
		tasksQueue[partitionID]->EnQueue(subTrans);
	}
}

static void generateProjectionBitVector(uint& bitv, std::vector<ID>& project) {
	bitv = 0;
	for (size_t i = 0; i != project.size(); ++i) {
		bitv |= 1 << project[i];
	}
}

static void generateTripleNodeBitVector(uint& bitv, CXTripleNode& node) {
	bitv = 0;
	if (!node.constSubject)
		bitv |= (1 << node.subject);
	if (!node.constPredicate)
		bitv |= (1 << node.predicate);
	if (!node.constObject)
		bitv |= (1 << node.object);
}

static size_t countOneBits(uint bitv) {
	size_t count = 0;
	while (bitv) {
		bitv = bitv & (bitv - 1);
		count++;
	}
	return count;
}

static ID bitVtoID(uint bitv) {
	uint mask = 0x1;
	ID count = 0;
	while (true) {
		if ((mask & bitv) == mask)
			break;
		bitv = bitv >> 1;
		count++;
	}
	return count;
}

static int insertVarID(ID key, std::vector<ID>& idvec, CXTripleNode& node, ID& sortID) {
	int ret = 0;
	switch (node.scanOperation) {
	case CXTripleNode::FINDO:
	case CXTripleNode::FINDOBYP:
	case CXTripleNode::FINDOBYS:
	case CXTripleNode::FINDOBYSP:
		if (key != node.object)
			idvec.push_back(node.object);
		sortID = node.object;
		break;
	case CXTripleNode::FINDOPBYS:
		if (key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 0;
		}
		if (key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 1;
		}
		break;
	case CXTripleNode::FINDOSBYP:
		if (key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 0;
		}
		if (key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 1;
		}
		break;
	case CXTripleNode::FINDP:
	case CXTripleNode::FINDPBYO:
	case CXTripleNode::FINDPBYS:
	case CXTripleNode::FINDPBYSO:
		if (key != node.predicate)
			idvec.push_back(node.predicate);
		sortID = node.predicate;
		break;
	case CXTripleNode::FINDPOBYS:
		if (key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 0;
		}
		if (key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 1;
		}
		break;
	case CXTripleNode::FINDPSBYO:
		if (key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 0;
		}
		if (key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 1;
		}
		break;
	case CXTripleNode::FINDS:
	case CXTripleNode::FINDSBYO:
	case CXTripleNode::FINDSBYP:
	case CXTripleNode::FINDSBYPO:
		if (key != node.subject)
			idvec.push_back(node.subject);
		sortID = node.subject;
		break;
	case CXTripleNode::FINDSOBYP:
		if (key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 0;
		}
		if (key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 1;
		}
		break;
	case CXTripleNode::FINDSPBYO:
		if (key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 0;
		}
		if (key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 1;
		}
		break;
	case CXTripleNode::NOOP:
		break;
	}

	return ret;
}

static void generateResultPos(std::vector<ID>& idVec, std::vector<ID>& projection, std::vector<int>& resultPos) {
	resultPos.clear();

	std::vector<ID>::iterator iter;
	for (size_t i = 0; i != projection.size(); ++i) {
		iter = find(idVec.begin(), idVec.end(), projection[i]);
		resultPos.push_back(iter - idVec.begin());
	}
}

static void generateVerifyPos(std::vector<ID>& idVec, std::vector<int>& verifyPos) {
	verifyPos.clear();
	size_t i, j;
	size_t size = idVec.size();
	for (i = 0; i != size; ++i) {
		for (j = i + 1; j != size; j++) {
			if (idVec[i] == idVec[j]) {
				verifyPos.push_back(i);
				verifyPos.push_back(j);
			}
		}
	}
}

void PrintEntityBufferID(EntityIDBuffer* buffer) {
	size_t size = buffer->getSize();
	ID* resultBuffer = buffer->getBuffer();

	for (size_t i = 0; i < size; ++i) {
		cout << "ID:" << resultBuffer[i] << endl;
	}
	cout << "ResultBuffer Size: " << size << endl;
}

void printSomethingWorker(EntityIDBuffer* buffer) {
	size_t size = buffer->getSize();
	int count = 0;
	ID* resultBuffer = buffer->getBuffer();

	int IDCount = buffer->getIDCount();

	if (IDCount == 1) {
		for (size_t i = 1; i < size; ++i) {
			if (resultBuffer[i - 1] > resultBuffer[i])
				cout << "Buffer Error" << endl;
		}
	}
	else if (IDCount == 2) {
		for (size_t i = 3; i < size; i = i + 2) {
			if (resultBuffer[i - 3] > resultBuffer[i - 1])
				cout << "Buffer Error i-3,i-1" << endl;
			else if (resultBuffer[i - 3] == resultBuffer[i - 1]) {
				if (resultBuffer[i - 2] > resultBuffer[i])
					cout << "Buffer Error i-2,i" << endl;
			}
		}
	}

	for (size_t i = 0; i < size; ++i) {
		cout << "ID:" << resultBuffer[i] << ' ';
		count++;
		if (count % 20 == 0)
			cout << endl;
	}
	cout << "ResultBuffer Size: " << size << endl;
	cout << endl;
}

//查找实体ID
//输入：triple 表示一个查找模式  查找模式里面的第一个变量的范围属于[minID,maxID]
//输出：返回查找的中间结果 retBuffer
ResultIDBuffer* CXTripleBitWorkerQuery::findEntityIDByTriple(CXTripleNode* triple, ID minID, ID maxID) {
#ifdef PATTERN_TIME
	struct timeval start, end;
	gettimeofday(&start, NULL);
#endif

	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::QUERY;
	boost::shared_ptr<IndexForTT> indexForTT(new IndexForTT);
	//初始化一个任务(一个pattern)
	CXSubTrans* subTrans = new CXSubTrans(*transactionTime, workerID, minID, maxID, operationType, 1, *triple, indexForTT);
	//执行任务
	tasksEnQueue(triple->predicate, subTrans);
	//取回查询结果
	ResultIDBuffer* retBuffer = resultBuffer[triple->predicate]->DeQueue();

#ifdef PATTERN_TIME
	gettimeofday(&end, NULL);
	//cout << "find pattern " << triple->tripleNodeID << " time elapsed: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0
	//	<< endl;
#endif
	//返回查询结果
	return retBuffer;
}

ResultIDBuffer* CXTripleBitWorkerQuery::findEntityIDByTriple1(CXTripleNode* triple, ID minID, ID maxID) {
#ifdef PATTERN_TIME
	struct timeval start, end;
	gettimeofday(&start, NULL);
#endif

	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::QUERY;
	boost::shared_ptr<IndexForTT> indexForTT(new IndexForTT);
	//初始化一个任务(一个pattern)
	CXSubTrans* subTrans = new CXSubTrans(*transactionTime, workerID, minID, maxID, operationType, 1, *triple, indexForTT);
	//执行任务
	tasksEnQueue(triple->predicate, subTrans);
	//取回查询结果
	ResultIDBuffer* retBuffer = resultBuffer[triple->predicate]->DeQueue();

#ifdef PATTERN_TIME
	gettimeofday(&end, NULL);
	//cout << "find pattern " << triple->tripleNodeID << " time elapsed: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0
	//	<< endl;
#endif
	//返回查询结果
	return retBuffer;
}

Status CXTripleBitWorkerQuery::getTripleNodeByID(CXTripleNode*& triple, TripleBitQueryGraph::TripleNodeID nodeID) {
	vector<CXTripleNode>::iterator iter = _query->tripleNodes.begin();

	for (; iter != _query->tripleNodes.end(); iter++) {
		if (iter->tripleNodeID == nodeID) {
			triple = &(*iter);
			return OK;
		}
	}
	return NOT_FOUND;
}

Status CXTripleBitWorkerQuery::getVariableNodeByID(TripleBitQueryGraph::JoinVariableNode*& node, TripleBitQueryGraph::JoinVariableNodeID id) {
	int size = _query->joinVariableNodes.size();

	for (int i = 0; i < size; i++) {
		if (_query->joinVariableNodes[i].value == id) {
			node = &(_query->joinVariableNodes[i]);
			break;
		}
	}

	return OK;
}

bool CXTripleBitWorkerQuery::getResult(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buflist_index) {
	ofstream refile;
	refile.open("results",ios::out|ios::app);
	if (buflist_index == bufferlist.size())
		return true;

	EntityIDBuffer* entBuf = bufferlist[buflist_index];
	size_t currentIndex = bufPreIndexs[buflist_index];
	size_t bufsize = entBuf->getSize();
	while (currentIndex < bufsize && (*entBuf)[currentIndex] < key) {
		currentIndex++;
	}
	bufPreIndexs[buflist_index] = currentIndex;
	if (currentIndex >= bufsize || (*entBuf)[currentIndex] > key)
		return false;

	bool ret;
	ID* buf = entBuf->getBuffer();
	int IDCount = entBuf->getIDCount();
	int sortKey = entBuf->getSortKey();

	ret = true;
	size_t resultVecSize = resultVec.size();
	std::string URI;
	while (currentIndex < bufsize && buf[currentIndex * IDCount + sortKey] == key) {
		for (int i = 0; i < IDCount; ++i) {
			if (i != sortKey) {
				resultVec.push_back(buf[currentIndex * IDCount + i]);
			}
		}
		if (buflist_index == (bufferlist.size() - 1)) {
			if (needselect == true) {
				if (resultVec[verifyPos[0]] != resultVec[verifyPos[1]]) {
					++currentIndex;
					continue;
				}
			}
			for (size_t j = 0; j != resultPos.size(); j++) {
				uriTable->getURIById(URI, resultVec[resultPos[j]]);
#ifdef PRINT_RESULT
				std::cout << URI << " ";
				refile<<URI<<" ";
#else
				resultPtr->push_back(URI);
#endif
			}
#ifdef PRINT_RESULT
			std::cout << std::endl;
			refile<<endl;
#endif
		}
		else {
			ret = getResult(resultVec[keyPos[buflist_index]], bufferlist, buflist_index + 1);
			if (ret != true)
				break;
		}

		++currentIndex;
		resultVec.resize(resultVecSize);
	}

	return ret;
}

void CXTripleBitWorkerQuery::getResult_join(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buflist_index) {
	if (buflist_index == bufferlist.size())
		return;

	EntityIDBuffer* entBuf = bufferlist[buflist_index];
	size_t currentIndex = entBuf->getEntityIDPos(key); //bufPreIndexs[buflist_index]
	if (currentIndex == size_t(-1))
		return;
	size_t bufsize = entBuf->getSize();

	ID* buf = entBuf->getBuffer();
	int IDCount = entBuf->getIDCount();
	int sortKey = entBuf->getSortKey();

	size_t resultVecSize = resultVec.size();
	std::string URI;
	while (currentIndex < bufsize && buf[currentIndex * IDCount + sortKey] == key) {
		for (int i = 0; i < IDCount; ++i) {
			if (i != sortKey)
				resultVec.push_back(buf[currentIndex * IDCount + i]);
		}
		if (buflist_index == (bufferlist.size() - 1)) {
			if (needselect == true) {
				if (resultVec[verifyPos[0]] != resultVec[verifyPos[1]]) {
					++currentIndex;
					continue;
				}
			}
			for (size_t j = 0; j != resultPos.size(); ++j) {
				uriTable->getURIById(URI, resultVec[resultPos[j]]);
#ifdef PRINT_RESULT
				std::cout << URI << " ";
#else
				resultPtr->push_back(URI);
#endif
			}
#ifdef PRINT_RESULT
			std::cout << std::endl;
#endif
		}
		else {
			getResult_join(resultVec[keyPos[buflist_index]], bufferlist, buflist_index + 1);
		}

		++currentIndex;
		resultVec.resize(resultVecSize);
	}
}

//单个公共变量查询
extern int refile_mutex = 0;
Status CXTripleBitWorkerQuery::centerJoin() {
	patternBitmap.clear();
	patternBitmap.resize(this->_queryGraph->variableCount);
	isSingle = true;
#ifdef DEBUG
	cout << "variable count:  " << this->_queryGraph->variableCount << endl;
	cout << "patternBitmap size:" << patternBitmap.size() << endl;
#endif
	for (int i = 1; i < patternBitmap.size(); i++) {
		TripleBitQueryGraph::JoinVariableNode* node = NULL;
		getVariableNodeByID(node, i);
		if (!node) {
			CBitMap* temp = new CBitMap(1, max_id);
			patternBitmap[i].push_back(temp);
		}
		else {
			// cout << node->appear_tpnodes.size() << endl;
			for (int j = 0; j < node->appear_tpnodes.size(); j++) {
				CBitMap* temp = new CBitMap(1, max_id);
				patternBitmap[i].push_back(temp);
			}
		}
	}
#ifdef DEBUG
	cout << "========patternBitmap:========" << endl;
	for (int i = 1; i < patternBitmap.size(); i++) {
		cout << "variable i  : " << i << " total level " << patternBitmap[i].size() << endl;
	}
#endif
	//here is a problem of the key value of the map
	for (int i = 0; i < _query->joinVariables.size(); i++) {
		ID j = _query->joinVariables[i];
		varRange[j] = make_pair(0, UINT_MAX);  // 4294967295
		//varRange[i] = make_pair(0,INT_MAX); // 2147483647
	}

	EntityIDBuffer* buffer = NULL;
	CXTripleNode* triple = NULL;
	map<TripleNodeID, ResultIDBuffer*> res;

	//第一步，保存各个查询模式的中间结果
	for (int i = 0; i < _query->tripleNodes.size(); i++) {
#ifdef DEBUG
		cout << "pattern:" << i << endl;
#endif
		CXTripleNode& x = _query->tripleNodes[i];
		preVar = x.varLevel[0].first;
		preLevel = x.varLevel[0].second;
#ifdef DEBUG
		cout << "preVar:" << preVar << endl;
		cout << "preLevel:" << preLevel << endl;
#endif
		if (x.varLevel.size() > 1) {
			postVar = x.varLevel[1].first;
			postLevel = x.varLevel[1].second;
		}
#ifdef DEBUG
		cout << "range:" << varRange[preVar].first << " " << varRange[preVar].second << endl;
		cout << "zhixing chaxun" << endl;
#endif
		res[x.tripleNodeID] = findEntityIDByTriple(&x, varRange[preVar].first, varRange[preVar].second);
#ifdef DEBUG
		cout << "chaxun over" << endl;
#endif

		ID a_min = UINT_MAX;
		ID a_max = 1;
		ID b_min = UINT_MAX;
		ID b_max = 1;

		res[x.tripleNodeID]->printMinMax(a_min, a_max, b_min, b_max);
#ifdef DEBUG
		cout << "new a b:" << endl;
		cout << "a_min: " << a_min << " a_max: " << a_max << " b_min: " << b_min << " b_max: " << b_max << endl;
#endif
		if (!(a_min == UINT_MAX && a_max == 1 && b_min == UINT_MAX && b_max == 1)) {
			varRange[preVar].first = max(varRange[preVar].first, a_min);
			varRange[preVar].second = min(varRange[preVar].second, a_max);

			if (x.varLevel.size() > 1) {
				varRange[postVar].first = max(varRange[postVar].first, b_min);
				varRange[postVar].second = min(varRange[postVar].second, b_max);
			}
		}
#ifdef DEBUG
		for (int i = 1; i <= _query->joinVariables.size(); i++) {
			cout << "var " << i << " " << varRange[i].first << " " << varRange[i].second << endl;
		}

		cout << "tripleNodeID " << x.tripleNodeID << " SIZE: " << res[x.tripleNodeID]->getSize() << endl;
		cout << "i " << i << " _query->tripleNodes.size() " << _query->tripleNodes.size() << endl;
#endif
	}
#ifdef DEBUG
	cout << "Reduce intermediate result:" << endl;
#endif
	for (int i = 0; i < _query->tripleNodes.size(); i++) {
		CXTripleNode& x = _query->tripleNodes[i];
		if (res[x.tripleNodeID]->getSize() > 0) {
			preVar = x.varLevel[0].first;
			preLevel = x.varLevel[0].second;

			if (x.varLevel.size() > 1) {
				postVar = x.varLevel[1].first;
				postLevel = x.varLevel[1].second;
			}

			// is it really need the reduce part for centerjoin?
			#ifdef DEBUG
			if (preLevel < patternBitmap[preVar].size() - 1 || postLevel < patternBitmap[postVar].size() - 1) {
				cout << "triple " << x.tripleNodeID << " " << "before reduce: " << res[x.tripleNodeID]->getSize() << endl;
				//reduce_p(res[x.tripleNodeID]);
				cout << "after reduce " << res[x.tripleNodeID]->getSize() << endl;
			}
			#endif
			EntityIDList[x.tripleNodeID] = res[x.tripleNodeID]->getEntityIDBuffer();
			#ifdef DEBUG
			cout << "after transfer " << EntityIDList[x.tripleNodeID]->getSize() << endl;
			#endif
		}
	}
	#ifdef DEBUG
	cout << "End reduce." << endl;

	//对中间结果进行链接，得到最终查找结果
	cout << "Go to full join:" << endl;
	#endif
	cout<<"results:"<<endl;
	size_t i;
	std::string URI;
	size_t projectNo = _queryGraph->getProjection().size();

#ifdef PRINT_RESULT
	char temp[2];
	sprintf(temp, "%d", projectNo);
	resultPtr->push_back(temp);
#endif

	if (!EntityIDList.size())
	{

#ifdef PRINT_RESULT
		string subject , predicate;
		uriTable->getURIById(subject,_query->tripleNodes[0].subject);
		preTable->getPredicateByID(predicate,_query->tripleNodes[0].predicate);
		cout<<subject <<" "<<predicate<< " Empty Result" << endl;
#else
		resultPtr->push_back("Empty Result");
#endif

		return OK;
	}

	if (projectNo == 1) { 
		string subjuctstr,predicatestr,objectstr;
		// ofstream tempfile;
		// tempfile.open("tempfile",ios::out|ios::app);
		int index_t = _query->tripleNodes.size() - 1;
		size_t size = EntityIDList[_query->tripleNodes[index_t].tripleNodeID]->getSize();
		ID* p = EntityIDList[_query->tripleNodes[index_t].tripleNodeID]->getBuffer();
		preTable->getPredicateByID(predicatestr,_query->tripleNodes[index_t].predicate);
		uriTable->getURIById(subjuctstr,_query->tripleNodes[index_t].subject);
		uriTable->getURIById(objectstr,_query->tripleNodes[index_t].object);
		if(refile_mutex==0){
		ofstream refile;
		refile.open("results",ios::out);
		for (i = 0; i < size; ++i) {
			if (uriTable->getURIById(URI, p[i]) == OK) {
#ifdef PRINT_RESULT
				cout << URI << endl;
				if(_query->tripleNodes[index_t].constSubject){
					refile <<subjuctstr<<" "<<predicatestr<<" "<< URI << endl;
				}else if(_query->tripleNodes[index_t].constObject){
					refile <<URI<<" "<<predicatestr<<" "<< objectstr << endl;
				}
				
#else
				resultPtr->push_back(URI);
#endif
			}
			else {
#ifdef PRINT_RESULT
				cout << p[i] << " " << "not found" << endl;
#else
				resultPtr->push_back("Empty Result");
#endif
			}
		}
		refile_mutex = 1;
		}
	}
	else {
		std::vector<EntityIDBuffer*> bufferlist;
		std::vector<ID> resultVar;
		resultVar.resize(0);
		uint projBitV, nodeBitV, resultBitV, tempBitV;
		resultBitV = 0;
		ID sortID;
		int keyp;
		keyPos.clear();

		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
		for (i = 0; i != _query->tripleNodes.size(); i++) {
			//generate the bit vector of query pattern
			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
			//get the common bit
			tempBitV = projBitV & nodeBitV;
			if (tempBitV == nodeBitV) {
				//the query pattern which contains two or more variables is better
				if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0 || (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1))
					continue;
				bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);
				if (countOneBits(resultBitV) == 0) {
					//the first time, last joinKey should be set as UNIT_MAX
					keyp = insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
				}
				else {
					keyp = insertVarID(_query->joinVariables[0], resultVar, _query->tripleNodes[i], sortID);
					keyPos.push_back(keyp);
				}
				resultBitV |= nodeBitV;
				//the buffer of query pattern is enough
				if (countOneBits(resultBitV) == projectNo)
					break;
			}
		}

		if (projectNo == 2) {
			generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize();
			ID* ids = buf->getBuffer();
			int IDCount = buf->getIDCount();
			for (i = 0; i < bufsize; i++) {
				for (int j = 0; j < IDCount; j++) {
					if (uriTable->getURIById(URI, ids[i * IDCount + resultPos[j]]) == OK) {
#ifdef PRINT_RESULT
						cout << URI << " ";
#else
						resultPtr->push_back(URI);
#endif
					}
					else
						cout << "not found" << endl;
				}
				cout << endl;
			}
		}

		else {
			generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
			needselect = false;
			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize();
			bufPreIndexs.clear();
			bufPreIndexs.resize(bufferlist.size(), 0);
			ID key;
			int IDCount = buf->getIDCount();
			ID* ids = buf->getBuffer();
			int sortKey = buf->getSortKey();
			for (i = 0; i != bufsize; i++) {
				resultVec.resize(0);
				key = ids[i * IDCount + sortKey];
				for (int j = 0; j < IDCount; j++) {
					resultVec.push_back(ids[i * IDCount + j]);
				}

				bool ret = getResult(key, bufferlist, 1);
				if (ret == false) {
					while (i < bufsize && ids[i * IDCount + sortKey] == key) {
						i++;
					}
					i--;
				}
			}
			
		}
	}
	refile_mutex = 1;
	return OK;
}

Status CXTripleBitWorkerQuery::centerJoin1() {
	patternBitmap.clear();
	patternBitmap.resize(this->_queryGraph->variableCount);
	isSingle = true;
	cout << "variable count:  " << this->_queryGraph->variableCount << endl;
	cout << "patternBitmap size:" << patternBitmap.size() << endl;

	for (int i = 1; i < patternBitmap.size(); i++) {
		TripleBitQueryGraph::JoinVariableNode* node = NULL;
		getVariableNodeByID(node, i);
		if (!node) {
			CBitMap* temp = new CBitMap(1, max_id);
			patternBitmap[i].push_back(temp);
		}
		else {
			// cout << node->appear_tpnodes.size() << endl;
			for (int j = 0; j < node->appear_tpnodes.size(); j++) {
				CBitMap* temp = new CBitMap(1, max_id);
				patternBitmap[i].push_back(temp);
			}
		}
	}
	cout << "========patternBitmap:========" << endl;
	for (int i = 1; i < patternBitmap.size(); i++) {
		cout << "variable i  : " << i << " total level " << patternBitmap[i].size() << endl;
	}

	//here is a problem of the key value of the map
	for (int i = 0; i < _query->joinVariables.size(); i++) {
		ID j = _query->joinVariables[i];
		varRange[j] = make_pair(0, UINT_MAX);  // 4294967295
		//varRange[i] = make_pair(0,INT_MAX); // 2147483647
	}

	EntityIDBuffer* buffer = NULL;
	CXTripleNode* triple = NULL;
	map<TripleNodeID, ResultIDBuffer*> res;

	//第一步，保存各个查询模式的中间结果
	for (int i = 0; i < _query->tripleNodes.size(); i++) {
		cout << "pattern:" << i << endl;
		CXTripleNode& x = _query->tripleNodes[i];
		preVar = x.varLevel[0].first;
		preLevel = x.varLevel[0].second;
		cout << "preVar:" << preVar << endl;
		cout << "preLevel:" << preLevel << endl;
		if (x.varLevel.size() > 1) {
			postVar = x.varLevel[1].first;
			postLevel = x.varLevel[1].second;
		}
		cout << "range:" << varRange[preVar].first << " " << varRange[preVar].second << endl;
		cout << "zhixing chaxun" << endl;
		res[x.tripleNodeID] = findEntityIDByTriple(&x, varRange[preVar].first, varRange[preVar].second);
		cout << "chaxun over" << endl;

		ID a_min = UINT_MAX;
		ID a_max = 1;
		ID b_min = UINT_MAX;
		ID b_max = 1;

		res[x.tripleNodeID]->printMinMax(a_min, a_max, b_min, b_max);

		cout << "new a b:" << endl;
		cout << "a_min: " << a_min << " a_max: " << a_max << " b_min: " << b_min << " b_max: " << b_max << endl;

		if (!(a_min == UINT_MAX && a_max == 1 && b_min == UINT_MAX && b_max == 1)) {
			varRange[preVar].first = max(varRange[preVar].first, a_min);
			varRange[preVar].second = min(varRange[preVar].second, a_max);

			if (x.varLevel.size() > 1) {
				varRange[postVar].first = max(varRange[postVar].first, b_min);
				varRange[postVar].second = min(varRange[postVar].second, b_max);
			}
		}

		for (int i = 1; i <= _query->joinVariables.size(); i++) {
			cout << "var " << i << " " << varRange[i].first << " " << varRange[i].second << endl;
		}

		cout << "tripleNodeID " << x.tripleNodeID << " SIZE: " << res[x.tripleNodeID]->getSize() << endl;
		cout << "i " << i << " _query->tripleNodes.size() " << _query->tripleNodes.size() << endl;
	}

	cout << "Reduce intermediate result:" << endl;
	for (int i = 0; i < _query->tripleNodes.size(); i++) {
		CXTripleNode& x = _query->tripleNodes[i];
		if (res[x.tripleNodeID]->getSize() > 0) {
			preVar = x.varLevel[0].first;
			preLevel = x.varLevel[0].second;

			if (x.varLevel.size() > 1) {
				postVar = x.varLevel[1].first;
				postLevel = x.varLevel[1].second;
			}

			// is it really need the reduce part for centerjoin?
			if (preLevel < patternBitmap[preVar].size() - 1 || postLevel < patternBitmap[postVar].size() - 1) {
				cout << "triple " << x.tripleNodeID << " " << "before reduce: " << res[x.tripleNodeID]->getSize() << endl;
				//reduce_p(res[x.tripleNodeID]);
				cout << "after reduce " << res[x.tripleNodeID]->getSize() << endl;
			}

			EntityIDList[x.tripleNodeID] = res[x.tripleNodeID]->getEntityIDBuffer();
			cout << "after transfer " << EntityIDList[x.tripleNodeID]->getSize() << endl;
		}
	}
	cout << "End reduce." << endl;

	//对中间结果进行链接，得到最终查找结果
	cout << "Go to full join:" << endl;

	size_t i;
	std::string URI;
	size_t projectNo = _queryGraph->getProjection().size();

#ifdef PRINT_RESULT
	char temp[2];
	sprintf(temp, "%d", projectNo);
	resultPtr->push_back(temp);
#endif

	if (!EntityIDList.size())
	{

#ifdef PRINT_RESULT
		cout << "Empty Result" << endl;
#else
		resultPtr->push_back("Empty Result");
#endif

		return OK;
	}

	if (projectNo == 1) {
		int index_t = _query->tripleNodes.size() - 1;
		size_t size = EntityIDList[_query->tripleNodes[index_t].tripleNodeID]->getSize();
		ID* p = EntityIDList[_query->tripleNodes[index_t].tripleNodeID]->getBuffer();
		for (i = 0; i < size; ++i) {
			if (uriTable->getURIById(URI, p[i]) == OK) {
#ifdef PRINT_RESULT
				cout << URI << endl;
#else
				resultPtr->push_back(URI);
#endif
			}
			else {
#ifdef PRINT_RESULT
				cout << p[i] << " " << "not found" << endl;
#else
				resultPtr->push_back("Empty Result");
#endif
			}
		}
	}
	else {
		std::vector<EntityIDBuffer*> bufferlist;
		std::vector<ID> resultVar;
		resultVar.resize(0);
		uint projBitV, nodeBitV, resultBitV, tempBitV;
		resultBitV = 0;
		ID sortID;
		int keyp;
		keyPos.clear();

		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
		for (i = 0; i != _query->tripleNodes.size(); i++) {
			//generate the bit vector of query pattern
			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
			//get the common bit
			tempBitV = projBitV & nodeBitV;
			if (tempBitV == nodeBitV) {
				//the query pattern which contains two or more variables is better
				if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0 || (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1))
					continue;
				bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);
				if (countOneBits(resultBitV) == 0) {
					//the first time, last joinKey should be set as UNIT_MAX
					keyp = insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
				}
				else {
					keyp = insertVarID(_query->joinVariables[0], resultVar, _query->tripleNodes[i], sortID);
					keyPos.push_back(keyp);
				}
				resultBitV |= nodeBitV;
				//the buffer of query pattern is enough
				if (countOneBits(resultBitV) == projectNo)
					break;
			}
		}

		if (projectNo == 2) {
			generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize();
			ID* ids = buf->getBuffer();
			int IDCount = buf->getIDCount();
			for (i = 0; i < bufsize; i++) {
				for (int j = 0; j < IDCount; j++) {
					if (uriTable->getURIById(URI, ids[i * IDCount + resultPos[j]]) == OK) {
#ifdef PRINT_RESULT
						cout << URI << " ";
#else
						resultPtr->push_back(URI);
#endif
					}
					else
						cout << "not found" << endl;
				}
				cout << endl;
			}
		}

		else {
			generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
			needselect = false;
			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize();
			bufPreIndexs.clear();
			bufPreIndexs.resize(bufferlist.size(), 0);
			ID key;
			int IDCount = buf->getIDCount();
			ID* ids = buf->getBuffer();
			int sortKey = buf->getSortKey();
			for (i = 0; i != bufsize; i++) {
				resultVec.resize(0);
				key = ids[i * IDCount + sortKey];
				for (int j = 0; j < IDCount; j++) {
					resultVec.push_back(ids[i * IDCount + j]);
				}

				bool ret = getResult(key, bufferlist, 1);
				if (ret == false) {
					while (i < bufsize && ids[i * IDCount + sortKey] == key) {
						i++;
					}
					i--;
				}
			}
		}
	}
	return OK;
}

//trace the F3.rq to clearify the range of the variable
Status CXTripleBitWorkerQuery::linerJoin() {
	isSingle = false;
	patternBitmap.clear();
	patternBitmap.resize(this->_queryGraph->variableCount);
	cout << "variable count:  " << this->_queryGraph->variableCount << endl;
	cout << "patternBitmap size:" << patternBitmap.size() << endl;

	for (int i = 1; i < patternBitmap.size(); i++) {
		TripleBitQueryGraph::JoinVariableNode* node = NULL;
		getVariableNodeByID(node, i);
		if (!node) {
			CBitMap* temp = new CBitMap(1, max_id);
			patternBitmap[i].push_back(temp);
		}
		else {
			// cout << node->appear_tpnodes.size() << endl;
			for (int j = 0; j < node->appear_tpnodes.size(); j++) {
				CBitMap* temp = new CBitMap(1, max_id);
				patternBitmap[i].push_back(temp);
			}
		}
	}
	cout << "========patternBitmap:========" << endl;
	for (int i = 0; i < patternBitmap.size(); i++) {
		cout << "variable i  : " << i << " total level " << patternBitmap[i].size() << endl;
	}

	//here is a problem of the key value of the map
	for (int i = 0; i < _query->joinVariables.size(); i++) {
		ID j = _query->joinVariables[i];
		varRange[j] = make_pair(0, UINT_MAX);  // 4294967295
		//varRange[i] = make_pair(0,INT_MAX); // 2147483647
	}

	/*
	cout<<"===========debug varRange：============"<<endl;
	for(int i = 1; i <= _query->joinVariables.size(); i++){
		cout <<"varRange[i].first:" << varRange[i].first << "varRange[i].second:" << varRange[i].second<<endl;
	}
	*/

	EntityIDBuffer* buffer = NULL;
	CXTripleNode* triple = NULL;
	map<TripleNodeID, ResultIDBuffer*> res;

	for (int i = 0; i < _query->tripleNodes.size(); i++) {
		CXTripleNode& x = _query->tripleNodes[i];
		preVar = x.varLevel[0].first;
		preLevel = x.varLevel[0].second;
		if (x.varLevel.size() > 1) {
			postVar = x.varLevel[1].first;
			postLevel = x.varLevel[1].second;
		}
		cout << "firstVar " << preVar << " " << varRange[preVar].first << " " << varRange[preVar].second;
		res[x.tripleNodeID] = findEntityIDByTriple(&x, varRange[preVar].first, varRange[preVar].second);

		ID a_min = UINT_MAX;
		ID a_max = 1;
		ID b_min = UINT_MAX;
		ID b_max = 1;
		res[x.tripleNodeID]->printMinMax(a_min, a_max, b_min, b_max);

		cout << "new a b:" << endl;
		cout << "a_min: " << a_min << " a_max: " << a_max << " b_min: " << b_min << " b_max: " << b_max << endl;

		if (!(a_min == UINT_MAX && a_max == 1 && b_min == UINT_MAX && b_max == 1)) {
			varRange[preVar].first = max(varRange[preVar].first, a_min);
			varRange[preVar].second = min(varRange[preVar].second, a_max);

			if (x.varLevel.size() > 1) {
				varRange[postVar].first = max(varRange[postVar].first, b_min);
				varRange[postVar].second = min(varRange[postVar].second, b_max);
			}
		}

		for (int i = 1; i <= _query->joinVariables.size(); i++) {
			cout << "var " << i << " " << varRange[i].first << " " << varRange[i].second << endl;
		}

		cout << "tripleNodeID " << x.tripleNodeID << " SIZE: " << res[x.tripleNodeID]->getSize() << endl;
		cout << "i " << i << " _query->tripleNodes.size() " << _query->tripleNodes.size() << endl;
	}

	cout << "Reduce intermediate result:" << endl;
	for (int i = 0; i < _query->tripleNodes.size(); i++) {
		CXTripleNode& x = _query->tripleNodes[i];
		if (res[x.tripleNodeID]->getSize() > 0) {
			preVar = x.varLevel[0].first;
			preLevel = x.varLevel[0].second;
			int calpreLevel = preLevel + 1;
			postVar = x.varLevel[1].first;
			postLevel = x.varLevel[1].second;
			int calpostLevel = postLevel + 1;

			if (preLevel < patternBitmap[preVar].size() - 1 || postLevel < patternBitmap[postVar].size() - 1) {
				cout << "triple " << x.tripleNodeID << " " << "before reduce " << res[x.tripleNodeID]->getSize() << endl;
				reduce_p(res[x.tripleNodeID]);
				cout << "after reduce " << res[x.tripleNodeID]->getSize() << endl;
			}

			EntityIDList[x.tripleNodeID] = res[x.tripleNodeID]->getEntityIDBuffer();
			cout << "after transfer " << EntityIDList[x.tripleNodeID]->getSize() << endl;
		}
	}
	cout << "End reduce." << endl;
	cout << "Go to full join:" << endl;

	size_t projectionSize = _queryGraph->getProjection().size();
	int i;
	if (projectionSize == 1) {
		uint projBitV, nodeBitV, tempBitV;
		std::vector<ID> resultVar;
		resultVar.resize(0);
		ID sortID;

		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
		TripleBitQueryGraph::TripleNodeID tid;
		size_t bufsize = UINT_MAX;
		for (i = 0; i != _query->tripleNodes.size(); ++i) {
			if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0)
				continue;
			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
			tempBitV = projBitV | nodeBitV;
			if (tempBitV == projBitV) {
				if (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1) {
					insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
					tid = _query->tripleNodes[i].tripleNodeID;
					break;
				}
				else {
					if (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getSize() < bufsize) {
						insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
						tid = _query->tripleNodes[i].tripleNodeID;
						bufsize = EntityIDList[tid]->getSize();
					}
				}
			}
		}

		std::vector<EntityIDBuffer*> bufferlist;
		bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);
		generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
		needselect = false;

		std::string URI;
		ID* p = EntityIDList[tid]->getBuffer();
		int IDCount = EntityIDList[tid]->getIDCount();

		for (i = 0; i != bufsize; ++i) {
			uriTable->getURIById(URI, p[i * IDCount + resultPos[0]]);
#ifdef PRINT_RESULT
			std::cout << URI << std::endl;
#else
			resultPtr->push_back(URI);
#endif
		}
	}

	else {
		std::vector<EntityIDBuffer*> bufferlist;
		std::vector<ID> resultVar;
		ID sortID;
		resultVar.resize(0);
		uint projBitV, nodeBitV, resultBitV, tempBitV;
		resultBitV = 0;
		vector<ID>::iterator iter;
		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
		i = 0;
		int sortKey;
		size_t tnodesize = _query->tripleNodes.size();
		while (true) {
			if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0 || (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1)) {
				++i;
				i = i % tnodesize;
				continue;
			}
			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
			tempBitV = projBitV & nodeBitV;
			if (tempBitV == nodeBitV) {
				if (countOneBits(resultBitV) == 0) {
					insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
					sortKey = EntityIDList[_query->tripleNodes[i].tripleNodeID]->getSortKey();
				}
				else {
					tempBitV = nodeBitV & resultBitV;
					if (countOneBits(tempBitV) == 1) {
						ID key = ID(log((double)tempBitV) / log(2.0));
						sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
						iter = find(resultVar.begin(), resultVar.end(), sortID);
						keyPos.push_back(iter - resultVar.begin());
					}
					else {
						++i;
						i = i % tnodesize;
						continue;
					}
				}

				resultBitV |= nodeBitV;
				EntityIDList[_query->tripleNodes[i].tripleNodeID]->setSortKey(sortKey);
				bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);

				if (countOneBits(resultBitV) == projectionSize)
					break;
			}

			++i;
			i = i % tnodesize;
		}

		for (i = 0; i < bufferlist.size(); ++i) {
			bufferlist[i]->sort();
		}

		generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
		needselect = false;

		std::string URI;
		if (projectionSize == 2) {
			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize();
			ID* ids = buf->getBuffer();
			int IDCount = buf->getIDCount();
			for (i = 0; i < bufsize; i++) {
				for (int j = 0; j < IDCount; ++j) {
					if (uriTable->getURIById(URI, ids[i * IDCount + resultPos[j]]) == OK) {
#ifdef PRINT_RESULT
						cout << URI << " ";
#else
						resultPtr->push_back(URI);
#endif
					}
					else {
						cout << "not found" << endl;
					}
				}
				cout << endl;
			}
		}
		else {
			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize();
			bufPreIndexs.resize(bufferlist.size(), 0);
			int IDCount = buf->getIDCount();
			ID* ids = buf->getBuffer();
			for (i = 0; i != bufsize; ++i) {
				resultVec.resize(0);
				for (int j = 0; j < IDCount; ++j) {
					resultVec.push_back(ids[i * IDCount + j]);
				}

				getResult_join(resultVec[keyPos[0]], bufferlist, 1);
			}
		}
	}

	return OK;
}

//trace the Q3 to clearify the full join operation
Status CXTripleBitWorkerQuery::treeShapeJoin() {
	isSingle = false;
	patternBitmap.clear();
	patternBitmap.resize(this->_queryGraph->variableCount);
	cout << "variable count:  " << this->_queryGraph->variableCount << endl;
	cout << "patternBitmap size:" << patternBitmap.size() << endl;

	for (int i = 1; i < patternBitmap.size(); i++) {
		TripleBitQueryGraph::JoinVariableNode* node = NULL;
		getVariableNodeByID(node, i);
		if (!node) {
			CBitMap* temp = new CBitMap(1, max_id);
			patternBitmap[i].push_back(temp);
		}
		else {
			// cout << node->appear_tpnodes.size() << endl;
			for (int j = 0; j < node->appear_tpnodes.size(); j++) {
				CBitMap* temp = new CBitMap(1, max_id);
				patternBitmap[i].push_back(temp);
			}
		}
	}
	cout << "========patternBitmap:========" << endl;
	for (int i = 0; i < patternBitmap.size(); i++) {
		cout << "variable i  : " << i << " total level " << patternBitmap[i].size() << endl;
	}

	//here is a problem of the key value of the map
	for (int i = 0; i < _query->joinVariables.size(); i++) {
		ID j = _query->joinVariables[i];
		varRange[j] = make_pair(0, UINT_MAX);  // 4294967295
		//varRange[i] = make_pair(0,INT_MAX); // 2147483647
	}

	/*
	cout<<"===========debug varRange：============"<<endl;
	for(int i = 1; i <= _query->joinVariables.size(); i++){
		cout <<"varRange[i].first:" << varRange[i].first << "varRange[i].second:" << varRange[i].second<<endl;
	}
	*/

	EntityIDBuffer* buffer = NULL;
	CXTripleNode* triple = NULL;
	map<TripleNodeID, ResultIDBuffer*> res;

	for (int i = 0; i < _query->tripleNodes.size(); i++) {
		CXTripleNode& x = _query->tripleNodes[i];
		preVar = x.varLevel[0].first;
		preLevel = x.varLevel[0].second;
		if (x.varLevel.size() > 1) {
			postVar = x.varLevel[1].first;
			postLevel = x.varLevel[1].second;
		}
		cout << "firstVar " << preVar << " " << varRange[preVar].first << " " << varRange[preVar].second;
		res[x.tripleNodeID] = findEntityIDByTriple(&x, varRange[preVar].first, varRange[preVar].second);

		ID a_min = UINT_MAX;
		ID a_max = 1;
		ID b_min = UINT_MAX;
		ID b_max = 1;
		res[x.tripleNodeID]->printMinMax(a_min, a_max, b_min, b_max);

		cout << "new a b:" << endl;
		cout << "a_min: " << a_min << " a_max: " << a_max << " b_min: " << b_min << " b_max: " << b_max << endl;

		if (!(a_min == UINT_MAX && a_max == 1 && b_min == UINT_MAX && b_max == 1)) {
			varRange[preVar].first = max(varRange[preVar].first, a_min);
			varRange[preVar].second = min(varRange[preVar].second, a_max);

			if (x.varLevel.size() > 1) {
				varRange[postVar].first = max(varRange[postVar].first, b_min);
				varRange[postVar].second = min(varRange[postVar].second, b_max);
			}
		}

		for (int i = 1; i <= _query->joinVariables.size(); i++) {
			cout << "var " << i << " " << varRange[i].first << " " << varRange[i].second << endl;
		}

		cout << "tripleNodeID " << x.tripleNodeID << " SIZE: " << res[x.tripleNodeID]->getSize() << endl;
		cout << "i " << i << " _query->tripleNodes.size() " << _query->tripleNodes.size() << endl;
	}

	cout << "Reduce intermediate result:" << endl;
	for (int i = 0; i < _query->tripleNodes.size(); i++) {
		CXTripleNode& x = _query->tripleNodes[i];
		if (res[x.tripleNodeID]->getSize() > 0) {
			preVar = x.varLevel[0].first;
			preLevel = x.varLevel[0].second;
			int calpreLevel = preLevel + 1;
			postVar = x.varLevel[1].first;
			postLevel = x.varLevel[1].second;
			int calpostLevel = postLevel + 1;

			if (preLevel < patternBitmap[preVar].size() - 1 || postLevel < patternBitmap[postVar].size() - 1) {
				cout << "triple " << x.tripleNodeID << " " << "before reduce " << res[x.tripleNodeID]->getSize() << endl;
				reduce_p(res[x.tripleNodeID]);
				cout << "after reduce " << res[x.tripleNodeID]->getSize() << endl;
			}

			EntityIDList[x.tripleNodeID] = res[x.tripleNodeID]->getEntityIDBuffer();
			cout << "after transfer " << EntityIDList[x.tripleNodeID]->getSize() << endl;
		}
	}
	cout << "End reduce." << endl;
	cout << "Go to full join:" << endl;

	std::vector<EntityIDBuffer*> bufferlist;
	std::vector<ID> resultVar;
	ID sortID;

	resultVar.resize(0);
	uint projBitV, nodeBitV, resultBitV, tempBitV;
	resultBitV = 0;
	//获取查询变量的位向量表示
	generateProjectionBitVector(projBitV, _queryGraph->getProjection());

	int sortKey;
	size_t tnodesize = _query->tripleNodes.size();
	std::set<ID> tids;
	ID tid;
	bool complete = true;
	int i = 0;
	vector<ID>::iterator iter;
	keyPos.clear();
	resultPos.clear();
	verifyPos.clear();

	while (true) {
		//if the pattern has no buffer, then skip it
		if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0) {
			++i;

			if (i == tnodesize) {
				if (complete == true)
					break;
				else {
					complete = true;
					i = i % tnodesize;
				}
			}

			i = i % tnodesize;
			continue;
		}
		generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
		//skip the singlevar pattern
		if (countOneBits(nodeBitV) == 1) {
			++i;
			if (i == tnodesize) {
				if (complete == true)
					break;
				else {
					complete = true;
					i = i % tnodesize;
				}
			}
			i = i % tnodesize;
			continue;
		}

		tid = _query->tripleNodes[i].tripleNodeID;
		if (tids.count(tid) == 0) {
			//resultBitV initialize as 0
			if (countOneBits(resultBitV) == 0) {
				//calculate the resultVar
				insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
				sortKey = EntityIDList[tid]->getSortKey();
			}
			else {
				tempBitV = nodeBitV & resultBitV;
				if (countOneBits(tempBitV) == 1) {
					//transfer the bitVar to varID
					ID key = bitVtoID(tempBitV); //ID(log((double)tempBitV)/log(2.0));
					sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
					iter = find(resultVar.begin(), resultVar.end(), sortID);
					keyPos.push_back(iter - resultVar.begin());
				}
				else if (countOneBits(tempBitV) == 2) {
					//verify buffers
					ID key = bitVtoID(tempBitV); //ID(log((double)tempBitV)/log(2.0));
					sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
					iter = find(resultVar.begin(), resultVar.end(), sortID);
					keyPos.push_back(iter - resultVar.begin());
				}
				else {
					complete = false;
					++i;
					i = i % tnodesize;
					continue;
				}
			}
			resultBitV = resultBitV | nodeBitV;
			EntityIDList[tid]->setSortKey(sortKey);
			bufferlist.push_back(EntityIDList[tid]);
			tids.insert(tid);
		}

		++i;
		if (i == tnodesize) {
			if (complete == true)
				break;
			else {
				complete = true;
				i = i % tnodesize;
			}
		}
	}

	// sotr buffer in bufferlist by sortkey
	for (i = 0; i < bufferlist.size(); ++i) {
		bufferlist[i]->sort();
	}
	//通过resultVar以及查询语句中的projection设置resultPos变量
	//resultPos中存放查询语句中projection的变量在resultVar中的偏移位置
	generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
	//generate verify pos vector
	//verifyPos中存放resultVar中相同变量的下标对,resultVar中之所以会有相同变量
	//是由insertVarID函数中,idvec.pushback()引起
	generateVerifyPos(resultVar, verifyPos);
	needselect = true;

#ifdef COLDCACHE
	Timestamp t1;
#endif

	EntityIDBuffer* buf = bufferlist[0];
	size_t bufsize = buf->getSize();

	int IDCount = buf->getIDCount();
	ID* ids = buf->getBuffer();
	sortKey = buf->getSortKey();
	//从bufferlist[0]作为开端,对其每一行进行全连接
	for (i = 0; i != bufsize; ++i) {
		resultVec.resize(0);
		//将bufferlist[0]模式中的变量首行写入resultVec中去
		for (int j = 0; j < IDCount; ++j) {
			resultVec.push_back(ids[i * IDCount + j]);
		}
		//resultVec存放当前模式的第i行变量,个数等于IDCount, 1表示当前待连接buffer在bufferlist中的次序
		getResult_join(resultVec[keyPos[0]], bufferlist, 1);
	}
	return OK;
}
