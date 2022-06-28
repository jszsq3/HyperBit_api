/*
 * ThreadPool.cpp
 *
 *  Created on: 2010-6-18
 *      Author: liupu
 */

#include "ThreadPool.h"
#include "TripleBit.h"
#include <string>
#include <iostream>

using namespace std;

vector<ThreadPool*> ThreadPool::workPool;
vector<ThreadPool*> ThreadPool::chunkPool;
//ThreadPool *ThreadPool::partitionPool = NULL;
vector<ThreadPool*> ThreadPool::subTranThreadPool;

ThreadPool::ThreadPool() {
	gettimeofday(&start,NULL);
	shutDown = false;
	NodeTask *nodeTask = new NodeTask;
	head = tail = nodeTask;
	pthread_mutex_init(&headMutex, NULL);
	pthread_mutex_init(&tailMutex, NULL);
	pthread_cond_init(&pthreadCond, NULL);
	pthread_cond_init(&pthreadEmpty, NULL);
	pthread_create(&tid, NULL, threadFunc, this);
}

ThreadPool::~ThreadPool() {
	stopAll();
	if (head != NULL) {
		delete head;
		head = NULL;
		tail = NULL;
	}
	pthread_mutex_destroy(&headMutex);
	pthread_mutex_destroy(&tailMutex);
	pthread_cond_destroy(&pthreadCond);
	pthread_cond_destroy(&pthreadEmpty);
}

void* ThreadPool::threadFunc(void * threadData) {
	//pthread_t tid = pthread_self();
	int rnt;
	ThreadPool* pool = (ThreadPool*) threadData;
	while (1) {
		rnt = pthread_mutex_lock(&pool->headMutex);
		if (rnt != 0) {
			cout << "Get mutex error" << endl;
		}

		while (pool->isEmpty() && pool->shutDown == false) {
			pthread_cond_wait(&pool->pthreadCond, &pool->headMutex);
		}
		if (pool->shutDown == true) {
			pthread_mutex_unlock(&pool->headMutex);
			pthread_exit(NULL);
		}

		Task task = pool->Dequeue();

		task();
		//pool->decTaskNum();
		//cout << pool->getTaskNum() << endl;
		if (pool->isEmpty()) {
			//gettimeofday(&pool->end,NULL);
			//cerr << "thread " << tid << "\t"  << ((pool->end.tv_sec - pool->start.tv_sec) * 1000000 + pool->end.tv_usec - pool->start.tv_usec) << endl;
			pthread_cond_broadcast(&pool->pthreadEmpty);
		}
		pthread_mutex_unlock(&pool->headMutex);
	}
	return (void*) 0;
}

void ThreadPool::Enqueue(const Task &task) {
	NodeTask *nodeTask = new NodeTask(task);
	pthread_mutex_lock(&tailMutex);
	tail->next = nodeTask;
	tail = nodeTask;
	pthread_mutex_unlock(&tailMutex);
}

ThreadPool::Task ThreadPool::Dequeue() {
	NodeTask *node, *newNode;
	node = head;
	newNode = head->next;
	Task task = newNode->value;
	head = newNode;
//	pthread_mutex_unlock(&headMutex);
	delete node;
	return task;
}

int ThreadPool::addTask(const Task &task) {
	Enqueue(task);
	pthread_cond_broadcast(&pthreadCond);
	return 0;
}

int ThreadPool::stopAll() {
	shutDown = true;
	pthread_mutex_unlock(&headMutex);
	pthread_cond_broadcast(&pthreadCond);
	pthread_join(tid, NULL);

	return 0;
}

int ThreadPool::wait() {
	pthread_mutex_lock(&headMutex);
	while (!isEmpty()) {
		pthread_cond_wait(&pthreadEmpty, &headMutex);
	}
	pthread_mutex_unlock(&headMutex);
	return 0;
}

vector<ThreadPool*> & ThreadPool::getSubTranPool() {
	if (subTranThreadPool.empty()) {
		for (int i = 0; i < 16; i++) {
			subTranThreadPool.push_back(new ThreadPool());
			//subTranThreadPool[i]->setTaskNum((tripleSize*2+insert_pid_num)/gthread);
		}
	}
	return subTranThreadPool;
}

vector<ThreadPool*> &ThreadPool::getWorkPool() {
	if (workPool.empty()) {
		for (int i = 0; i < WORK_THREAD_NUMBER; i++) {
			workPool.push_back(new ThreadPool());
		}
	}
	return workPool;
}

vector<ThreadPool*> &ThreadPool::getChunkPool() {
	if (chunkPool.empty()) {
		for (int i = 0; i < 16/*CHUNK_THREAD_NUMBER*/; i++) {
			chunkPool.push_back(new ThreadPool());
		}
	}
	return chunkPool;
}

void ThreadPool::waitWorkThread() {
	for (int i = 0; i < WORK_THREAD_NUMBER; i++) {
		workPool[i]->wait();
	}
}

void ThreadPool::waitSubTranThread() {
	for (int i = 0; i < 16; i++) {
		subTranThreadPool[i]->wait();
	}
}

void ThreadPool::waitChunkThread() {
	for (int i = 0; i < 16/*CHUNK_THREAD_NUMBER*/; i++) {
		chunkPool[i]->wait();
	}
}

/*ThreadPool &ThreadPool::getPartitionPool() {
 if (partitionPool == NULL) {
 partitionPool = new ThreadPool(CHUNK_THREAD_NUMBER);
 }
 return *partitionPool;
 }*/

void ThreadPool::createAllPool() {
	//getWorkPool();
	//getChunkPool();
	//getPartitionPool();
	//getSubTranPool();
}

void ThreadPool::deleteAllPool() {
	for (vector<ThreadPool*>::iterator it = workPool.begin(); it != workPool.end(); it++) {
		if (*it != NULL) {
			delete *it;
			*it = NULL;
		}
	}

	for (vector<ThreadPool*>::iterator it = subTranThreadPool.begin(); it != subTranThreadPool.end(); it++) {
		if (*it != NULL) {
			delete *it;
			*it = NULL;
		}
	}

	for (vector<ThreadPool*>::iterator it = chunkPool.begin(); it != chunkPool.end(); it++) {
		if (*it != NULL) {
			delete *it;
			*it = NULL;
		}
	}
}

void ThreadPool::waitAllPoolComplete() {
	waitWorkThread();
	waitChunkThread();
	//getPartitionPool().wait();
	waitSubTranThread();
}


//---------------------------//
CXThreadPool* CXThreadPool::cxworkPool = NULL;
CXThreadPool* CXThreadPool::cxchunkPool = NULL;
CXThreadPool* CXThreadPool::cxpartitionPool = NULL;
CXThreadPool* CXThreadPool::cxscanjoinPool = NULL;



CXThreadPool::CXThreadPool(int threadNum) {
	this->threadNum = threadNum;
	shutDown = false;
	NodeTask* nodeTask = new NodeTask;
	head = tail = nodeTask;
	pthread_mutex_init(&headMutex, NULL);
	pthread_mutex_init(&tailMutex, NULL);
	pthread_mutex_init(&pthreadIdleMutex, NULL);
	pthread_mutex_init(&pthreadBusyMutex, NULL);
	pthread_cond_init(&pthreadCond, NULL);
	pthread_cond_init(&pthreadEmpty, NULL);
	pthread_cond_init(&pthreadBusyEmpty, NULL);
	create();
}

CXThreadPool::~CXThreadPool()
{
	stopAll();
	if (head != NULL) {
		delete head;
		head = NULL;
	}
	//	if(tail != NULL){
	//		delete tail;
	//		tail = NULL;
	//	}
	pthread_mutex_destroy(&headMutex);
	pthread_mutex_destroy(&tailMutex);
	pthread_mutex_destroy(&pthreadIdleMutex);
	pthread_mutex_destroy(&pthreadBusyMutex);
	pthread_cond_destroy(&pthreadCond);
	pthread_cond_destroy(&pthreadEmpty);
	pthread_cond_destroy(&pthreadBusyEmpty);
}

int CXThreadPool::moveToIdle(pthread_t tid) {
	pthread_mutex_lock(&pthreadBusyMutex);
	vector<pthread_t>::iterator busyIter = vecBusyThread.begin();
	while (busyIter != vecBusyThread.end()) {
		if (tid == *busyIter) {
			break;
		}
		busyIter++;
	}
	vecBusyThread.erase(busyIter);

	if (vecBusyThread.size() == 0) {
		pthread_cond_broadcast(&pthreadBusyEmpty);
	}

	pthread_mutex_unlock(&pthreadBusyMutex);

	pthread_mutex_lock(&pthreadIdleMutex);
	vecIdleThread.push_back(tid);
	pthread_mutex_unlock(&pthreadIdleMutex);
	return 0;
}

int CXThreadPool::moveToBusy(pthread_t tid) {
	pthread_mutex_lock(&pthreadIdleMutex);
	vector<pthread_t>::iterator idleIter = vecIdleThread.begin();
	while (idleIter != vecIdleThread.end()) {
		if (tid == *idleIter) {
			break;
		}
		idleIter++;
	}
	vecIdleThread.erase(idleIter);
	pthread_mutex_unlock(&pthreadIdleMutex);

	pthread_mutex_lock(&pthreadBusyMutex);
	vecBusyThread.push_back(tid);
	pthread_mutex_unlock(&pthreadBusyMutex);
	return 0;
}

void* CXThreadPool::threadFunc(void* threadData) {
	pthread_t tid = pthread_self();
	int rnt;
	ThreadPoolArg* arg = (ThreadPoolArg*)threadData;
	CXThreadPool* pool = arg->pool;
	while (1) {
		rnt = pthread_mutex_lock(&pool->headMutex);
		if (rnt != 0) {
			cout << "Get mutex error" << endl;
		}

		while (pool->isEmpty() && pool->shutDown == false) {
			pthread_cond_wait(&pool->pthreadCond, &pool->headMutex);
		}

		if (pool->shutDown == true) {
			pthread_mutex_unlock(&pool->headMutex);
			pthread_exit(NULL);
		}

		pool->moveToBusy(tid);
		Task task = pool->Dequeue();

		if (pool->isEmpty()) {
			pthread_cond_broadcast(&pool->pthreadEmpty);
		}
		pthread_mutex_unlock(&pool->headMutex);
		task();
		pool->moveToIdle(tid);
	}
	return (void*)0;
}

void CXThreadPool::Enqueue(const Task& task) {
	NodeTask* nodeTask = new NodeTask(task);
	pthread_mutex_lock(&tailMutex);
	tail->next = nodeTask;
	tail = nodeTask;
	pthread_mutex_unlock(&tailMutex);
}

CXThreadPool::Task CXThreadPool::Dequeue() {
	NodeTask* node, * newNode;
	//	pthread_mutex_lock(&headMutex);
	node = head;
	newNode = head->next;
	Task task = newNode->value;
	head = newNode;
	//	pthread_mutex_unlock(&headMutex);
	delete node;
	return task;
}

int CXThreadPool::addTask(const Task& task) {
	Enqueue(task);
	pthread_cond_broadcast(&pthreadCond);
	return 0;
}

int CXThreadPool::create() {
	struct ThreadPoolArg* arg = new ThreadPoolArg;
	pthread_mutex_lock(&pthreadIdleMutex);
	for (int i = 0; i < threadNum; i++) {
		pthread_t tid = 0;
		arg->pool = this;
		pthread_create(&tid, NULL, threadFunc, arg);
		vecIdleThread.push_back(tid);
	}
	pthread_mutex_unlock(&pthreadIdleMutex);
	return 0;
}

int CXThreadPool::stopAll() {
	shutDown = true;
	pthread_mutex_unlock(&headMutex);
	pthread_cond_broadcast(&pthreadCond);
	vector<pthread_t>::iterator iter = vecIdleThread.begin();
	while (iter != vecIdleThread.end()) {
		pthread_join(*iter, NULL);
		iter++;
	}

	iter = vecBusyThread.begin();
	while (iter != vecBusyThread.end()) {
		pthread_join(*iter, NULL);
		iter++;
	}

	return 0;
}

int CXThreadPool::wait()
{
	pthread_mutex_lock(&headMutex);
	while (!isEmpty()) {
		pthread_cond_wait(&pthreadEmpty, &headMutex);
	}
	pthread_mutex_unlock(&headMutex);
	pthread_mutex_lock(&pthreadBusyMutex);
	while (vecBusyThread.size() != 0) {
		pthread_cond_wait(&pthreadBusyEmpty, &pthreadBusyMutex);
	}
	pthread_mutex_unlock(&pthreadBusyMutex);
	return 0;
}

CXThreadPool& CXThreadPool::getWorkPool() {
	if (cxworkPool == NULL) {
		cxworkPool = new CXThreadPool(WORK_THREAD_NUMBER);
	}
	return *cxworkPool;
}

CXThreadPool& CXThreadPool::getChunkPool() {
	if (cxchunkPool == NULL) {
		cxchunkPool = new CXThreadPool(CHUNK_THREAD_NUMBER);
	}
	return *cxchunkPool;
}

CXThreadPool& CXThreadPool::getPartitionPool() {
	if (cxpartitionPool == NULL) {
		cxpartitionPool = new CXThreadPool(PARTITION_THREAD_NUMBER);
	}
	return *cxpartitionPool;
}

CXThreadPool& CXThreadPool::getScanjoinPool() {
	if (cxscanjoinPool == NULL) {
		cxscanjoinPool = new CXThreadPool(SCAN_JOIN_THREAD_NUMBER);
	}
	return *cxscanjoinPool;
}


void CXThreadPool::createAllPool() {
	getWorkPool();
	getChunkPool();
	getPartitionPool();
}

void CXThreadPool::deleteAllPool() {
	if (cxworkPool != NULL) {
		delete cxworkPool;
		cxworkPool = NULL;
	}
	if (cxchunkPool != NULL) {
		delete cxchunkPool;
		cxchunkPool = NULL;
	}
	if (cxpartitionPool != NULL) {
		delete cxpartitionPool;
		cxpartitionPool = NULL;
	}
	if (cxscanjoinPool != NULL) {
		delete cxscanjoinPool;
		cxscanjoinPool = NULL;
	}
}

void CXThreadPool::waitAllPoolComplete() {
	getWorkPool().wait();
	getChunkPool().wait();
	getPartitionPool().wait();
	getScanjoinPool().wait();
}
