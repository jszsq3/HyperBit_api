/*
 * GXTasksQueueChunk.h
 *
 *  Created on: 2013-8-20
 *      Author: root
 */

#ifndef TASKSQUEUECHUNK_H_
#define TASKSQUEUECHUNK_H_

#include "../TripleBit.h"
#include "../TripleBitQueryGraph.h"
#include "subTaskPackage.h"
#include "IndexForTT.h"
#include "Tools.h"
#include "Tasks.h"


//插入任务队列
class GXTasksQueueChunk {
private:
	const uchar* chunkBegin;
	ID chunkID;
	int xyType;
	bool soType;
	size_t winSize;

private:
	GXChunkTask* head;
	GXChunkTask* tail;
	pthread_mutex_t headMutex, tailMutex;

private:
	GXTasksQueueChunk();
public:
	
	GXTasksQueueChunk(const uchar* chunk_Begin, ID chunk_ID, bool so_Type) :
		chunkBegin(chunk_Begin), chunkID(chunk_ID), soType(so_Type) {
		head = tail = new GXChunkTask();
		pthread_mutex_init(&headMutex, NULL);
		pthread_mutex_init(&tailMutex, NULL);
		winSize = windowSize;
	}

	~GXTasksQueueChunk() {
		if (head != NULL) {
			delete head;
			head = NULL;
			tail = NULL;
		}
		pthread_mutex_destroy(&headMutex);
		pthread_mutex_destroy(&tailMutex);
	}

	void setTasksQueueChunk(const uchar* chunk_Begin, ID chunk_ID, bool so_Type){
		chunkBegin = chunk_Begin;
		chunkID = chunk_ID;
		soType = so_Type;
	}

	bool isEmpty() {
		pthread_mutex_lock(&headMutex);
		if (head->next == NULL) {
			pthread_mutex_unlock(&headMutex);
			return true;
		} else {
			pthread_mutex_unlock(&headMutex);
			return false;
		}
	}
	const ID getChunkID() {
		return chunkID;
	}
	const uchar* getChunkBegin() {
		return chunkBegin;
	}
	const int getXYType() {
		return xyType;
	}
	const bool getSOType() {
		return soType;
	}

	const size_t getWinSize() {
		return winSize;
	}

	void EnQueue(GXChunkTask *chunkTask) {
		//NodeChunkQueue* nodeChunkQueue = new NodeChunkQueue(chunkTask);
		//pthread_mutex_lock(&tailMutex);
		tail->next = chunkTask;
		tail = chunkTask;
		//pthread_mutex_unlock(&tailMutex);
	}

	GXChunkTask* Dequeue() {
		GXChunkTask* chunkTask = NULL;
		if (head != tail) {
			chunkTask = head->next;
			head->next = chunkTask->next;
			if(chunkTask->next == NULL){
				tail = head;
			}
		}
		return chunkTask;
	}

	/*GXChunkTask * Dequeue() {
		NodeChunkQueue* node = NULL;
		GXChunkTask* chunkTask = NULL;

		//pthread_mutex_lock(&headMutex);
		node = head;
		NodeChunkQueue* newNode = node->next;
		if (newNode == NULL) {
			//pthread_mutex_unlock(&headMutex);
			return chunkTask;
		}
		chunkTask = newNode->chunkTask;
		head = newNode;
		//pthread_mutex_unlock(&headMutex);
		delete node;
		return chunkTask;
	}*/
};

class NodeChunkQueue : private Uncopyable {
public:
	CXChunkTask* chunkTask;
	NodeChunkQueue* next;
	NodeChunkQueue() :
		chunkTask(NULL), next(NULL) {
	}
	~NodeChunkQueue() {
	}
	NodeChunkQueue(CXChunkTask* chunk_Task) :
		chunkTask(chunk_Task), next(NULL) {
	}
};

//一个数据块的查询的任务队列
class CXTasksQueueChunk {
private:
	const uchar* chunkBegin;//数据块的起始地址
	ID chunkID;//数据块的块号
	int xyType;
	int soType;

private:
	NodeChunkQueue* head;
	NodeChunkQueue* tail;
	pthread_mutex_t headMutex, tailMutex;

private:
	CXTasksQueueChunk();
public:
	CXTasksQueueChunk(const uchar* chunk_Begin, ID& chunk_ID, int xy_Type, int so_Type) :
		chunkBegin(chunk_Begin), chunkID(chunk_ID), xyType(xy_Type), soType(so_Type) {
		NodeChunkQueue* nodeChunkQueue = new NodeChunkQueue();
		head = tail = nodeChunkQueue;
		pthread_mutex_init(&headMutex, NULL);
		pthread_mutex_init(&tailMutex, NULL);
	}

	~CXTasksQueueChunk() {
		cout << "index" << endl;
		if (head != NULL) {
			delete head;
			head = NULL;
		}
		if (tail != NULL) {
			delete tail;
			tail = NULL;
		}
		pthread_mutex_destroy(&headMutex);
		pthread_mutex_destroy(&tailMutex);
	}

	bool isEmpty() {
		pthread_mutex_lock(&headMutex);
		if (head->next == NULL) {
			pthread_mutex_unlock(&headMutex);
			return true;
		}
		else {
			pthread_mutex_unlock(&headMutex);
			return false;
		}
	}
	const ID getChunkID() {
		return chunkID;
	}
	const uchar* getChunkBegin() {
		return chunkBegin;
	}
	const int getXYType() {
		return xyType;
	}
	const int getSOType() {
		return soType;
	}

	void EnQueue(CXChunkTask* chunkTask) {
		NodeChunkQueue* nodeChunkQueue = new NodeChunkQueue(chunkTask);
		pthread_mutex_lock(&tailMutex);
		tail->next = nodeChunkQueue;
		tail = nodeChunkQueue;
		pthread_mutex_unlock(&tailMutex);
	}

	CXChunkTask* Dequeue() {
		NodeChunkQueue* node = NULL;
		CXChunkTask* chunkTask = NULL;

		pthread_mutex_lock(&headMutex);
		node = head;
		NodeChunkQueue* newNode = node->next;
		if (newNode == NULL) {
			pthread_mutex_unlock(&headMutex);
			return chunkTask;
		}
		chunkTask = newNode->chunkTask;
		head = newNode;
		pthread_mutex_unlock(&headMutex);
		delete node;
		return chunkTask;
	}
};

#endif /* TASKSQUEUECHUNK_H_ */
