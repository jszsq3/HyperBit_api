/*
 * TripleBitRespository.cpp
 *
 *  Created on: May 13, 2010
 *      Author: root
 */

#include "MemoryBuffer.h"
#include "GXTripleBitRepository.h"
#include "PredicateTable.h"
#include "URITable.h"
#include "TripleBitBuilder.h"
#include "BitmapBuffer.h"
#include "StatisticsBuffer.h"
#include "EntityIDBuffer.h"
#include "MMapBuffer.h"
#include "ThreadPool.h"
#include "TempMMapBuffer.h"
#include "OSFile.h"
#include <sys/time.h>
#include "comm/TransQueueSW.h"
#include "comm/TasksQueueWP.h"
#include "comm/ResultBuffer.h"
#include "comm/IndexForTT.h"
#include "util/Properties.h"
#include <boost/thread/thread.hpp>

//#define MYDEBUG
int GXTripleBitRepository::colNo = INT_MAX - 1;

GXTripleBitRepository::GXTripleBitRepository() {
	this->UriTable = NULL;
	this->preTable = NULL;
	this->bitmapBuffer = NULL;
	buffer = NULL;
	this->transQueSW = NULL;

//	spStatisBuffer = NULL;
//	opStatisBuffer = NULL;

	subjectStat = NULL;
	subPredicateStat = NULL;
	objectStat = NULL;
	objPredicateStat = NULL;

	bitmapImage = NULL;
	bitmapPredicateImage = NULL;
}

GXTripleBitRepository::~GXTripleBitRepository() {
	TempMMapBuffer::deleteInstance();
	if (tripleBitWorker.size() == workerNum) {
		for (size_t i = 1; i <= workerNum; ++i) {
			if (tripleBitWorker[i] != NULL)
				delete tripleBitWorker[i];
			tripleBitWorker[i] = NULL;
		}
	}
	if (buffer != NULL)
		delete buffer;
	buffer = NULL;

	if (UriTable != NULL)
		delete UriTable;
	UriTable = NULL;

	if (preTable != NULL)
		delete preTable;
	preTable = NULL;

	sharedMemoryDestroy();

	if (bitmapBuffer != NULL)
		delete bitmapBuffer;
	bitmapBuffer = NULL;


	if (bitmapImage != NULL) {
		bitmapImage->close();
		delete bitmapImage;
		bitmapImage = NULL;
	}

	if (bitmapPredicateImage != NULL) {
		bitmapPredicateImage->close();
		delete bitmapPredicateImage;
		bitmapPredicateImage = NULL;
	}

	if (indexForTT != NULL)
		delete indexForTT;
	indexForTT = NULL;

	if (uriMutex != NULL) {
		delete uriMutex;
		uriMutex = NULL;
	}
}

bool GXTripleBitRepository::find_pid_by_string(PID& pid, const string& str) {
	if (preTable->getIDByPredicate(str.c_str(), pid) != OK)
		return false;
	return true;
}

bool GXTripleBitRepository::find_pid_by_string_update(PID& pid, const string& str) {
	if (preTable->getIDByPredicate(str.c_str(), pid) != OK) {
		if (preTable->insertTable(str.c_str(), pid) != OK)
			return false;
	}
	return true;
}

bool GXTripleBitRepository::find_soid_by_string(SOID& soid, const string& str) {
	if (UriTable->getIdByURI(str.c_str(), soid) != URI_FOUND)
		return false;
	return true;
}

bool GXTripleBitRepository::find_soid_by_string_update(SOID& soid, const string& str) {
	if (UriTable->getIdByURI(str.c_str(), soid) != URI_FOUND) {
		if (UriTable->insertTable(str.c_str(), soid) != OK)
			return false;
	}
	return true;
}

bool GXTripleBitRepository::find_string_by_pid(string& str, PID& pid) {
	if (preTable->getPredicateByID(str, pid) == OK)
		return true;
	return false;
}

bool GXTripleBitRepository::find_string_by_soid(string& str, SOID& soid) {
	if (UriTable->getURIById(str, soid) == URI_NOT_FOUND)
		return false;

	return true;
}

int GXTripleBitRepository::get_predicate_count(PID pid) {
	int count1 = bitmapBuffer->getChunkManager(pid, ORDERBYS)->getTripleCount();
	int count2 = bitmapBuffer->getChunkManager(pid, ORDERBYO)->getTripleCount();

	return count1 + count2;
}

bool GXTripleBitRepository::lookup(const string& str, ID& id) {
	if (preTable->getIDByPredicate(str.c_str(), id) != OK && UriTable->getIdByURI(str.c_str(), id) != URI_FOUND)
		return false;

	return true;
}
int GXTripleBitRepository::get_object_count(double object, char objType) {
	//longlong count=0;
	//	spStatisBuffer->getStatisBySO(object, count, objType);
	//stInfoByOP->get(object, objType, count);
	return 0;
}
int GXTripleBitRepository::get_subject_count(ID subjectID) {
	//longlong count=0;
	//	opStatisBuffer->getStatisBySO(subjectID, count);
	//stInfoBySP->get(subjectID, STRING, count);
	return 0;
}
int GXTripleBitRepository::get_subject_predicate_count(ID subjectID, ID predicateID) {
	longlong count;
	//stInfoBySP->get(subjectID, STRING, predicateID, count);
	//	spStatisBuffer->getStatis(subjectID, predicateID, count);
	return 0;
}
int GXTripleBitRepository::get_object_predicate_count(double object, ID predicateID, char objType) {
	longlong count;
	//stInfoByOP->get(object, objType, predicateID, count);
	//	opStatisBuffer->getStatis(object, predicateID, count, objType);
	return 0;
}
int GXTripleBitRepository::get_subject_object_count(ID subjectID, double object, char objType) {
	return 1;
}
Status GXTripleBitRepository::getSubjectByObjectPredicate(double object, ID pod, char objType) {
	pos = 0;
	return OK;
}

void GXTripleBitRepository::save_cast(const string dir){
	ofstream idtostring;
	idtostring.open(dir+"/predicate_idtostring",ios::out);
	ofstream stringtoid;
	stringtoid.open(dir+"/predicate_stringtoid",ios::out);
	ofstream hidfile;
	hidfile.open(dir+"/hid",ios::out);
	for(auto iter = preTable->idtostring.begin();iter != preTable->idtostring.end();iter++){
		idtostring<<iter->first<<" "<<iter->second<<" "<<preTable->isused[iter->first]<<endl;
	}
	for(auto iter = preTable->stringtoid.begin();iter != preTable->stringtoid.end();iter++){
		stringtoid<<iter->first<<" "<<iter->second<<endl;
		
	}
	hidfile<<hid<<endl;
}
ID GXTripleBitRepository::next() {
	ID id;
	Status s = buffer->getID(id, pos);
	if (s != OK)
		return 0;

	pos++;
	return id;
}

//�������ݿ�
//path = database/
GXTripleBitRepository* GXTripleBitRepository::create(const string& path) {

	GXTripleBitRepository* repo = new GXTripleBitRepository();

	repo->dataBasePath = path;

	repo->UriTable = URITable::load(path);
	repo->preTable = PredicateTable::load(path);

	string filename = path + "BitmapBuffer";// filename=database/BitmapBuffer
	repo->bitmapImage = new MMapBuffer(filename.c_str(), 0);// filename=database/BitmapBuffer

	string predicateFile(filename); // predicateFile = database/BitmapBuffer_predicate
	predicateFile.append("_predicate");


	string tempMMap(filename); 
	tempMMap.append("_temp"); //tempMMap=database/BitmapBuffer_temp  ������Ų��������
	TempMMapBuffer::create(tempMMap.c_str(), repo->bitmapImage->getSize());

	std::map<std::string, std::string> properties;
	Properties::parse(path, properties);
	string usedPage = properties["usedpage"];
	string buildUsedPage = properties["buildUsedpage"];
	if (!usedPage.empty()) {
		TempMMapBuffer::getInstance().setUsedPage(stoull(usedPage));
	}
	else {
		TempMMapBuffer::getInstance().setUsedPage(0);
	}
	if (!buildUsedPage.empty()) {
		TempMMapBuffer::getInstance().setBuildUsedPage(stoull(buildUsedPage));
	}
	else {
		TempMMapBuffer::getInstance().setBuildUsedPage(0);
		cout << "load error" << endl;
	}
	//获取当前hid
	{
		ifstream hidin;
		hidin.open(path+"/hid");
		hidin>>repo->hid;
		repo->hid++;
	}
	repo->bitmapPredicateImage = new MMapBuffer(predicateFile.c_str(), 0);
	repo->bitmapBuffer = BitmapBuffer::load(repo->bitmapImage, repo->bitmapPredicateImage);

	//repo->bitmapBuffer->print(2);


	timeval start, end;
	string statFilename = path + "/statistic_sp";
	gettimeofday(&start, NULL);
	statFilename = path + "/statistic_op";
	gettimeofday(&end, NULL);

	repo->buffer = new EntityIDBuffer();

	repo->partitionNum = repo->bitmapPredicateImage->get_length() / ((sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2) * 2);	//numbers of predicate

	repo->workerNum = WORKERNUM;
	repo->indexForTT = new IndexForTT(WORKERNUM);

	repo->sharedMemoryInit();

	//��ʼ������
	gettimeofday(&start, NULL);
	for (size_t i = 1; i <= repo->workerNum; ++i) {
		repo->tripleBitWorker[i] = new GXTripleBitWorker(repo, i);
	}
	//��������
	for (size_t i = 1; i <= repo->workerNum; ++i) {
		boost::thread thrd(boost::thread(boost::bind<void>(&GXTripleBitRepository::tripleBitWorkerInit, repo, i)));
	}

	return repo;
}

void GXTripleBitRepository::tripleBitWorkerInit(int i) {
	tripleBitWorker[i]->Work();
}

Status GXTripleBitRepository::sharedMemoryInit() {
	//Init the transQueueSW shared Memory
	sharedMemoryTransQueueSWInit();

	//Init the tasksQueueWP shared memory
	sharedMemoryTasksQueueWPInit();

	//Init the resultWP shared memory
	sharedMemoryResultWPInit();

	uriMutex = new boost::mutex();

	return OK;
}

Status GXTripleBitRepository::sharedMemoryDestroy() {
	//Destroy the transQueueSW shared Memory
	sharedMemoryTransQueueSWDestroy();
#ifdef MYDEBUG
	cout << "shared memory TransQueueSW destoried" << endl;
#endif

	//Destroy the tasksQueueWP shared memory
	sharedMemoryTasksQueueWPDestroy();
#ifdef MYDEBUG
	cout << "shared memory TasksQueueWP destoried" << endl;
#endif

	//Destroy the ResultWP shared memory
	sharedMemoryResultWPDestroy();
#ifdef MYDEBUG
	cout << "shared memory ResultWP destoried" << endl;
#endif

	return OK;
}

Status GXTripleBitRepository::sharedMemoryTransQueueSWInit() {
	transQueSW = new transQueueSW();
#ifdef DEBUG
	cout << "transQueSW(Master) Address: " << transQueSW << endl;
#endif
	if (transQueSW == NULL) {
		cout << "TransQueueSW Init Failed!" << endl;
		return ERROR_UNKOWN;
	}
	return OK;
}

Status GXTripleBitRepository::sharedMemoryTransQueueSWDestroy() {
	if (transQueSW != NULL) {
		delete transQueSW;
		transQueSW = NULL;
	}
	return OK;
}

Status GXTripleBitRepository::sharedMemoryTasksQueueWPInit() {
	for (size_t partitionID = 1; partitionID <= this->partitionNum; ++partitionID) {
		TasksQueueWP* tasksQueue = new TasksQueueWP();
		if (tasksQueue == NULL) {
			cout << "TasksQueueWP Init Failed!" << endl;
			return ERROR_UNKOWN;
		}
		this->tasksQueueWP.push_back(tasksQueue);

		boost::mutex *wpMutex = new boost::mutex();
		this->tasksQueueWPMutex.push_back(wpMutex);
	}
	return OK;
}

Status GXTripleBitRepository::sharedMemoryTasksQueueWPDestroy() {
	vector<TasksQueueWP*>::iterator iter = this->tasksQueueWP.begin(), limit = this->tasksQueueWP.end();
	for (; iter != limit; ++iter) {
		delete *iter;
		*iter = NULL;
	}
	this->tasksQueueWP.clear();
	this->tasksQueueWP.swap(this->tasksQueueWP);
	for (vector<boost::mutex *>::iterator iter = tasksQueueWPMutex.begin(); iter != tasksQueueWPMutex.end(); iter++) {
		delete *iter;
		*iter = NULL;
	}
	return OK;
}

Status GXTripleBitRepository::sharedMemoryResultWPInit() {
	for (size_t workerID = 1; workerID <= this->workerNum; ++workerID) {
		for (size_t partitionID = 1; partitionID <= this->partitionNum; ++partitionID) {
			ResultBuffer* resBuffer = new ResultBuffer();
			if (resBuffer == NULL) {
				cout << "ResultBufferWP Init Failed!" << endl;
				return ERROR_UNKOWN;
			}

			this->resultWP.push_back(resBuffer);
		}
	}
	return OK;
}

Status GXTripleBitRepository::sharedMemoryResultWPDestroy() {
	vector<ResultBuffer*>::iterator iter = this->resultWP.begin(), limit = this->resultWP.end();
	for (; iter != limit; ++iter) {
		delete *iter;
	}
	this->resultWP.clear();
	this->resultWP.swap(this->resultWP);
	return OK;
}

Status GXTripleBitRepository::tempMMapDestroy() {
	//endPartitionMaster();
	//bitmapBuffer->endUpdate(bitmapPredicateImage, bitmapImage);
	TempMMapBuffer::deleteInstance();
	return OK;
}

static void GXgetQuery(string& queryStr, const char* filename) {
	ifstream f;
	f.open(filename);

	queryStr.clear();

	if (f.fail() == true) {
		MessageEngine::showMessage("open query file error!", MessageEngine::WARNING);
		return;
	}

	char line[250];
	while (!f.eof()) {
		f.getline(line, 250);
		queryStr.append(line);
		queryStr.append(" ");
	}

	f.close();
}

Status GXTripleBitRepository::nextResult(string& str) {
	if (resBegin != resEnd) {
		str = *resBegin;
		resBegin++;
		return OK;
	}
	return ERROR;
}

Status GXTripleBitRepository::execute(string queryStr) {
	resultSet.resize(0);
	return OK;
}

void GXTripleBitRepository::endForWorker() {
	string queryStr("exit");
	for (size_t i = 1; i <= workerNum; ++i) {
		transQueSW->EnQueue(queryStr);
	}
	indexForTT->wait();
}

void GXTripleBitRepository::workerComplete() {
	indexForTT->completeOneTriple();
}

extern char* QUERY_PATH;
void GXTripleBitRepository::cmd_line(FILE* fin, FILE* fout) {
	char cmd[256];
	while (true) {
		fflush(fin);
		fprintf(fout, ">>>");
		fscanf(fin, "%s", cmd);
		resultSet.resize(0);
		if (strcmp(cmd, "dp") == 0 || strcmp(cmd, "dumppredicate") == 0) {
			getPredicateTable()->dump();
		} else if (strcmp(cmd, "query") == 0) {

		} else if (strcmp(cmd, "source") == 0) {

			string queryStr;
			::GXgetQuery(queryStr, string(QUERY_PATH).append("queryLUBM6").c_str());

			if (queryStr.length() == 0)
				continue;
			execute(queryStr);
		} else if (strcmp(cmd, "exit") == 0) {
			break;
		} else {
			string queryStr;
			::GXgetQuery(queryStr, string(QUERY_PATH).append(cmd).c_str());

			if (queryStr.length() == 0)
				continue;
			execute(queryStr);
		}
	}
}

void GXTripleBitRepository::cmd_line_sm(FILE* fin, FILE *fout, const string query_path) {
	ThreadPool::createAllPool();
	char cmd[256];
	while (true) {
		fflush(fin);
		fprintf(fout, ">>>");
		fscanf(fin, "%s", cmd);
		resultSet.resize(0);
		if (strcmp(cmd, "dp") == 0 || strcmp(cmd, "dumppredicate") == 0) {
			getPredicateTable()->dump();
		} else if (strcmp(cmd, "query") == 0) {
		} else if (strcmp(cmd, "source") == 0) {
			string queryStr;
			::GXgetQuery(queryStr, string(query_path).append("queryLUBM6").c_str());
			if (queryStr.length() == 0)
				continue;
		} else if (strcmp(cmd, "exit") == 0) {
			endForWorker();
			break;
		} else {
			string queryStr;
			::GXgetQuery(queryStr, string(query_path).append(cmd).c_str());

			if (queryStr.length() == 0)
				continue;
			transQueSW->EnQueue(queryStr);
		}
	}

	std::map<std::string, std::string> properties;
	properties["usedpage"] = to_string(TempMMapBuffer::getInstance().getUsedPage());
	string db = DATABASE_PATH;
	Properties::persist(db, properties);
	tempMMapDestroy();
	ThreadPool::deleteAllPool();
}

void GXTripleBitRepository::cmd_line_sm(FILE* fin, FILE *fout, const string query_path, const string query) {
	ThreadPool::createAllPool();
	//����ʱ������
	//ShiXu::parse(this->dataBasePath, this->shixu);
	char *cmd = strdup(query.c_str());
	resultSet.resize(0);
	if (strcmp(cmd, "dp") == 0 || strcmp(cmd, "dumppredicate") == 0) {
		getPredicateTable()->dump();
	} else if (strcmp(cmd, "query") == 0) {
	} else if (strcmp(cmd, "source") == 0) {
		string queryStr;
		::GXgetQuery(queryStr, string(query_path).append("queryLUBM6").c_str());
	} else if (strcmp(cmd, "exit") == 0) {
		endForWorker();
	} else {
		string queryStr;
		::GXgetQuery(queryStr, string(query_path).append(cmd).c_str());
		transQueSW->EnQueue(queryStr);
	}
	endForWorker();

	std::map<std::string, std::string> properties;
	properties["usedpage"] = to_string(TempMMapBuffer::getInstance().getUsedPage());
	string db = DATABASE_PATH;
	Properties::persist(db, properties);

	tempMMapDestroy();
	ThreadPool::deleteAllPool();
}

extern char* QUERY_PATH;
void GXTripleBitRepository::cmd_line_cold(FILE *fin, FILE *fout, const string cmd) {
	string queryStr;
	GXgetQuery(queryStr, string(QUERY_PATH).append(cmd).c_str());
	if (queryStr.length() == 0) {
		cout << "queryStr.length() == 0!" << endl;
		return;
	}
	cout << "DataBase: " << DATABASE_PATH << " Query:" << cmd << endl;
	transQueSW->EnQueue(queryStr);
	endForWorker();
	tempMMapDestroy();
}

extern char* QUERY_PATH;
void GXTripleBitRepository::cmd_line_warm(FILE *fin, FILE *fout, const string cmd) {
	string queryStr;
	GXgetQuery(queryStr, string(QUERY_PATH).append(cmd).c_str());
	if (queryStr.length() == 0) {
		cout << "queryStr.length() == 0" << endl;
		return;
	}
	cout << "DataBase: " << DATABASE_PATH << " Query:" << cmd << endl;
	for (int i = 0; i < 10; i++) {
		transQueSW->EnQueue(queryStr);
	}
	endForWorker();
	tempMMapDestroy();
}

//�����ݿ��������
void GXTripleBitRepository::insertData(const string& query_path) {
	//ɶҲû����ʹ���̵߳�ʱ�����û�У����Զ������̣߳�Ȼ�󷵻ش������߳�
	ThreadPool::createAllPool();

	resultSet.resize(0);

	//���Ӳ�������
	string queryStr("insert_path:");
	transQueSW->EnQueue(queryStr.append(query_path));

	//�����ַ���"exit"���������
	endForWorker();

	//�־û���������ʹ�õ�ҳ��
	std::map<std::string, std::string> properties;
	properties["usedpage"] = to_string(TempMMapBuffer::getInstance().getUsedPage());
	properties["buildUsedpage"] = to_string(TempMMapBuffer::getInstance().getBuildUsedPage());
	string db = DATABASE_PATH;
	Properties::persist(db, properties);

	//������ݿ�״̬
	//bitmapBuffer->print(2);

	//���ٲ����mmapbuffer
	tempMMapDestroy();

	//���������߳�
	ThreadPool::deleteAllPool();
}

void GXTripleBitRepository::print() {
	//tripleBitWorker[0]->Print();
}








int CXTripleBitRepository::colNo = INT_MAX - 1;

CXTripleBitRepository::CXTripleBitRepository() {
	this->UriTable = NULL;
	this->preTable = NULL;
	this->bitmapBuffer = NULL;
	buffer = NULL;
	this->transQueSW = NULL;

	subjectStat = NULL;
	subPredicateStat = NULL;
	objectStat = NULL;
	objPredicateStat = NULL;

	bitmapImage = NULL;
	bitmapIndexImage = NULL;
	bitmapPredicateImage = NULL;
}

CXTripleBitRepository::~CXTripleBitRepository() {
	TempMMapBuffer::deleteInstance();

	if (buffer != NULL)
		delete buffer;
	buffer = NULL;

	if (UriTable != NULL)
		delete UriTable;
	UriTable = NULL;
#ifdef DEBUG
	cout << "uri table delete" << endl;
#endif
	if (preTable != NULL)
		delete preTable;
	preTable = NULL;
#ifdef DEBUG
	cout << "predicate table delete" << endl;
#endif
	if (bitmapBuffer != NULL)
		delete bitmapBuffer;
	bitmapBuffer = NULL;
#ifdef DEBUG
	cout << "bitmapBuffer delete" << endl;
#endif
	if (subjectStat != NULL)
		delete subjectStat;
	subjectStat = NULL;

	if (subPredicateStat != NULL)
		delete subPredicateStat;
	subPredicateStat = NULL;

	if (objectStat != NULL)
		delete objectStat;
	objectStat = NULL;

	if (objPredicateStat != NULL)
		delete objPredicateStat;
	objPredicateStat = NULL;

	if (bitmapImage != NULL)
		delete bitmapImage;
	bitmapImage = NULL;

#ifdef DEBUG
	cout << "bitmapImage delete" << endl;
#endif

	if (bitmapIndexImage != NULL)
		delete bitmapIndexImage;
	bitmapIndexImage = NULL;

#ifdef DEBUG
	cout << "bitmap index image delete" << endl;
#endif

	if (bitmapPredicateImage != NULL)
		delete bitmapPredicateImage;
	bitmapPredicateImage = NULL;
#ifdef DEBUG
	cout << "bitmap predicate image" << endl;
#endif

	if (tripleBitWorker.size() == workerNum) {
		for (size_t i = 1; i <= workerNum; ++i) {
			if (tripleBitWorker[i] != NULL)
				delete tripleBitWorker[i];
			tripleBitWorker[i] = NULL;
		}
	}
#ifdef MYDEBUG
	cout << "CXTripleBitWorker delete" << endl;
#endif


	for (size_t i = 1; i <= partitionNum; ++i) {
		if (partitionMaster[i] != NULL)
			delete partitionMaster[i];
		partitionMaster[i] = NULL;
	}
#ifdef MYDEBUG
	cout << "partitionMaster delete" << endl;
#endif

	if (indexForTT != NULL)
		delete indexForTT;
	indexForTT = NULL;
}

bool CXTripleBitRepository::find_pid_by_string(PID& pid, const string& str) {
	if (preTable->getIDByPredicate(str.c_str(), pid) != OK)
		return false;
	return true;
}

bool CXTripleBitRepository::find_pid_by_string_update(PID& pid, const string& str) {
	if (preTable->getIDByPredicate(str.c_str(), pid) != OK) {
		if (preTable->insertTable(str.c_str(), pid) != OK)
			return false;
	}
	return true;
}

bool CXTripleBitRepository::find_soid_by_string(SOID& soid, const string& str) {
	if (UriTable->getIdByURI(str.c_str(), soid) != URI_FOUND)
		return false;
	return true;
}

bool CXTripleBitRepository::find_soid_by_string_update(SOID& soid, const string& str) {
	if (UriTable->getIdByURI(str.c_str(), soid) != URI_FOUND) {
		if (UriTable->insertTable(str.c_str(), soid) != OK)
			return false;
	}
	return true;
}

bool CXTripleBitRepository::find_string_by_pid(string& str, PID& pid) {
	preTable->getPredicateByID(str,pid);

	if (str.length() == 0)
		return false;
	return true;
}

bool CXTripleBitRepository::find_string_by_soid(string& str, SOID& soid) {
	if (UriTable->getURIById(str, soid) == URI_NOT_FOUND)
		return false;

	return true;
}

int CXTripleBitRepository::get_predicate_count(PID pid) {
	int count1 = bitmapBuffer->getChunkManager(pid, ORDERBYS)->getTripleCount();
	int count2 = bitmapBuffer->getChunkManager(pid, ORDERBYO)->getTripleCount();

	return count1 + count2;
}

bool CXTripleBitRepository::lookup(const string& str, ID& id) {
	if (preTable->getIDByPredicate(str.c_str(), id) != OK && UriTable->getIdByURI(str.c_str(), id) != URI_FOUND)
		return false;

	return true;
}
int CXTripleBitRepository::get_object_count(ID objectID) {
	((OneConstantStatisticsBuffer*)objectStat)->getStatis(objectID);
	return objectID;
}

int CXTripleBitRepository::get_subject_count(ID subjectID) {
	((OneConstantStatisticsBuffer*)subjectStat)->getStatis(subjectID);
	return subjectID;
}

int CXTripleBitRepository::get_subject_predicate_count(ID subjectID, ID predicateID) {
	subPredicateStat->getStatis(subjectID, predicateID);
	return subjectID;
}

int CXTripleBitRepository::get_object_predicate_count(ID objectID, ID predicateID) {
	objPredicateStat->getStatis(objectID, predicateID);
	return objectID;
}

int CXTripleBitRepository::get_subject_object_count(ID subjectID, ID objectID) {
	return 1;
}

Status CXTripleBitRepository::getSubjectByObjectPredicate(ID oid, ID pid) {
	pos = 0;
	return OK;
}

ID CXTripleBitRepository::next() {
	ID id;
	Status s = buffer->getID(id, pos);
	if (s != OK)
		return 0;

	pos++;
	return id;
}

CXTripleBitRepository* CXTripleBitRepository::create(const string& path) {
	DATABASE_PATH = (char*)path.c_str();
	CXTripleBitRepository* repo = new CXTripleBitRepository();
	repo->dataBasePath = path;

	repo->UriTable = URITable::load(path);
	repo->preTable = PredicateTable::load(path);

	{
		ifstream hidin;
		hidin.open(path+"/hid");
		hidin>>repo->hid;
		repo->hid++;
	}

	string filename = path + "BitmapBuffer";
	repo->bitmapImage = new MMapBuffer(filename.c_str(), 0);

	string predicateFile(filename);
	predicateFile.append("_predicate");



	string tempMMap(filename);
	tempMMap.append("_temp");
	TempMMapBuffer::create(tempMMap.c_str(), repo->bitmapImage->getSize());


	std::map<std::string, std::string> properties;
	Properties::parse(path, properties);
	string usedPage = properties["usedpage"];
	if (!usedPage.empty()) {
		TempMMapBuffer::getInstance().setUsedPage(stoull(usedPage));
	}
	else {
		TempMMapBuffer::getInstance().setUsedPage(0);
	}

	repo->bitmapPredicateImage = new MMapBuffer(predicateFile.c_str(), 0);

	repo->bitmapBuffer = BitmapBuffer::load(repo->bitmapImage, repo->bitmapPredicateImage);
	

	filename = path + "/statIndex";
	MMapBuffer* indexBufferFile = MMapBuffer::create(filename.c_str(), 0);
	char* indexBuffer = indexBufferFile->get_address();

	string statFilename = path + "/subject_statis";
	repo->subjectStat = OneConstantStatisticsBuffer::load(StatisticsBuffer::SUBJECT_STATIS, statFilename, indexBuffer);
	statFilename = path + "/object_statis";
	repo->objectStat = OneConstantStatisticsBuffer::load(StatisticsBuffer::OBJECT_STATIS, statFilename, indexBuffer);
	statFilename = path + "/subjectpredicate_statis";
	repo->subPredicateStat = TwoConstantStatisticsBuffer::load(StatisticsBuffer::SUBJECTPREDICATE_STATIS, statFilename, indexBuffer);
	statFilename = path + "/objectpredicate_statis";
	repo->objPredicateStat = TwoConstantStatisticsBuffer::load(StatisticsBuffer::OBJECTPREDICATE_STATIS, statFilename, indexBuffer);

#ifdef DEBUG
	cout << "subject count: " << ((OneConstantStatisticsBuffer*)repo->subjectStat)->getEntityCount() << endl;
	cout << "object count: " << ((OneConstantStatisticsBuffer*)repo->objectStat)->getEntityCount() << endl;
#endif

	repo->buffer = new EntityIDBuffer();

#ifdef DEBUG
	cout << "load complete!" << endl;
#endif

	repo->partitionNum = repo->bitmapPredicateImage->get_length() / ((sizeof(ID) + sizeof(SOType) + sizeof(size_t) * 2) * 2);
	repo->workerNum = WORKERNUM;
	repo->indexForTT = new IndexForTT(WORKERNUM);

#ifdef DEBUG
	cout << "partitionNumber: " << repo->partitionNum << endl;
#endif

	repo->sharedMemoryInit();

	for (size_t i = 1; i <= repo->workerNum; ++i) {
		repo->tripleBitWorker[i] = new CXTripleBitWorker(repo, i);
	}

	for (size_t i = 1; i <= repo->workerNum; ++i) {
		boost::thread thrd(boost::thread(boost::bind<void>(&CXTripleBitRepository::tripleBitWorkerInit, repo, i)));
	}

	for (size_t i = 1; i <= repo->partitionNum; ++i) {
		repo->partitionMaster[i] = new PartitionMaster(repo, i);
	}

	return repo;
}


void CXTripleBitRepository::tripleBitWorkerInit(int i) {
	tripleBitWorker[i]->Work();
}

void CXTripleBitRepository::partitionMasterInit(CXTripleBitRepository*& repo, int i) {
	repo->partitionMaster[i] = new PartitionMaster(repo, i);
	repo->partitionMaster[i]->Work();
}

Status CXTripleBitRepository::sharedMemoryInit() {
	//Init the transQueueSW shared Memory
	sharedMemoryTransQueueSWInit();

	//Init the tasksQueueWP shareed memory
	sharedMemoryTasksQueueWPInit();

	//Init the resultWP shared memory
	sharedMemoryResultWPInit();

	uriMutex = new boost::mutex();

	return OK;
}

Status CXTripleBitRepository::sharedMemoryDestroy() {
	//Destroy the transQueueSW shared Memory
	sharedMemoryTransQueueSWDestroy();
#ifdef MYDEBUG
	cout << "shared memory TransQueueSW destoried" << endl;
#endif

	//Destroy the tasksQueueWP shared memory
	sharedMemoryTasksQueueWPDestroy();
#ifdef MYDEBUG
	cout << "shared memory TasksQueueWP destoried" << endl;
#endif

	//Destroy the ResultWP shared memory
	sharedMemoryResultWPDestroy();
#ifdef MYDEBUG
	cout << "shared memory ResultWP destoried" << endl;
#endif

	return OK;
}

Status CXTripleBitRepository::sharedMemoryTransQueueSWInit() {
	transQueSW = new transQueueSW();
#ifdef DEBUG
	cout << "transQueSW(Master) Address: " << transQueSW << endl;
#endif
	if (transQueSW == NULL) {
		cout << "TransQueueSW Init Failed!" << endl;
		return ERROR_UNKOWN;
	}
	return OK;
}


Status CXTripleBitRepository::sharedMemoryTransQueueSWDestroy() {
	if (transQueSW != NULL) {
		delete transQueSW;
		transQueSW = NULL;
	}
	return OK;
}

Status CXTripleBitRepository::sharedMemoryTasksQueueWPInit() {
	for (size_t partitionID = 1; partitionID <= this->partitionNum; ++partitionID) {
		CXTasksQueueWP* tasksQueue = new CXTasksQueueWP();
		if (tasksQueue == NULL) {
			cout << "CXTasksQueueWP Init Failed!" << endl;
			return ERROR_UNKOWN;
		}
		this->tasksQueueWP.push_back(tasksQueue);

		boost::mutex* wpMutex = new boost::mutex();
		this->tasksQueueWPMutex.push_back(wpMutex);
	}
	return OK;
}

Status CXTripleBitRepository::sharedMemoryTasksQueueWPDestroy() {
	vector<CXTasksQueueWP*>::iterator iter = this->tasksQueueWP.begin(), limit = this->tasksQueueWP.end();
	for (; iter != limit; ++iter) {
		delete* iter;
	}
	this->tasksQueueWP.clear();
	this->tasksQueueWP.swap(this->tasksQueueWP);
	return OK;
}

Status CXTripleBitRepository::sharedMemoryResultWPInit() {
	for (size_t workerID = 1; workerID <= this->workerNum; ++workerID) {
		for (size_t partitionID = 1; partitionID <= this->partitionNum; ++partitionID) {
			ResultBuffer* resBuffer = new ResultBuffer();
			if (resBuffer == NULL) {
				cout << "ResultBufferWP Init Failed!" << endl;
				return ERROR_UNKOWN;
			}

			this->resultWP.push_back(resBuffer);
		}
	}
	return OK;
}

Status CXTripleBitRepository::sharedMemoryResultWPDestroy() {
	vector<ResultBuffer*>::iterator iter = this->resultWP.begin(), limit = this->resultWP.end();
	for (; iter != limit; ++iter) {
		delete* iter;
	}
	this->resultWP.clear();
	this->resultWP.swap(this->resultWP);
	return OK;
}

Status CXTripleBitRepository::tempMMapDestroy() {
	if (TempMMapBuffer::getInstance().getUsedPage() == 0) {
		TempMMapBuffer::deleteInstance();
		return OK;
	}

	endPartitionMaster();

	//bitmapBuffer->endUpdate(bitmapPredicateImage, bitmapImage);
	TempMMapBuffer::deleteInstance();
	return OK;
}

void CXTripleBitRepository::endPartitionMaster() {
}

static void CXgetQuery(string& queryStr, const char* filename) {
	ifstream f;
	f.open(filename);

	queryStr.clear();

	if (f.fail() == true) {
		MessageEngine::showMessage("open query file error!", MessageEngine::WARNING);
		return;
	}

	char line[150];
	while (f.peek() != EOF) {
		f.getline(line, 150);

		queryStr.append(line);
		queryStr.append(" ");
	}

	f.close();
}

Status CXTripleBitRepository::nextResult(string& str) {
	if (resBegin != resEnd) {
		str = *resBegin;
		resBegin++;
		return OK;
	}
	return ERROR;
}

Status CXTripleBitRepository::execute(string& queryStr) {
	resultSet.resize(0);
	cout << "execute!" << endl;
	transQueSW->EnQueue(queryStr);
	cout << "back from enqueue" << endl;
	//indexForTT->wait();
	sleep(3);
	cout << "back from wait" << endl;
	return OK;
}

void CXTripleBitRepository::endForWorker() {
	string queryStr("exit");
	for (size_t i = 1; i <= workerNum; ++i) {
		transQueSW->EnQueue(queryStr);
	}
	indexForTT->wait();
}

void CXTripleBitRepository::workerComplete() {
	indexForTT->completeOneTriple();
}
void CXTripleBitRepository::print(int f,CXTripleBitRepository*& repo){
	bitmapBuffer->print1(f,repo);
}
void CXTripleBitRepository::enqueue(FILE* fin, FILE* fout){
	char cmd[256];
	CXThreadPool::createAllPool();
	resultSet.resize(0);
	for(int i = 0 ;i<hid;i++){
		string h1 = "select ?x where{ <hid"+to_string(i)+">  <rdfs> ?x .} ";
		string h2 = "select ?x where{ <hid"+to_string(i)+">  <rdfp> ?x .} ";
		string h3 = "select ?x where{ <hid"+to_string(i)+">  <rdfst> ?x .} ";
		string h4 = "select ?x where{ <hid"+to_string(i)+">  <rdfet> ?x .} ";
		string h5 = "select ?x where{ <hid"+to_string(i)+">  <rdfo> ?x .} ";
		// CXTripleBitWorker* worker = new CXTripleBitWorker(this,1);

		// worker->Execute1(h1);
		// ofstream h1file,h2file,h3file,h4file,h5file;
		// h1file.open("./sparql/h1");
		// h1file<<h1;

		transQueSW->EnQueue(h1);
		transQueSW->EnQueue(h2);
		transQueSW->EnQueue(h3);
		transQueSW->EnQueue(h4);
		transQueSW->EnQueue(h5);
		
	}
	

	while (true) {
		cout<<"输入exit退出"<<endl;
		fflush(fin);
		fprintf(fout, ">>>");
		fscanf(fin, "%s", cmd);
		if (strcmp(cmd, "exit") == 0) {
			break;
		}
	}
	ifstream tempfile;
	tempfile.open("tempfile");
	tempfile.close();
	// std::remove(string("tempfile").c_str());
	// while(!transQueSW->Queue_Empty()){
	// 	if(transQueSW->Queue_Empty()){
	// 		break;
	// 	}
	// }
	

	CXThreadPool::getChunkPool().wait();
	tempMMapDestroy();
	CXThreadPool::deleteAllPool();
}

extern char* QUERY_PATH;
void CXTripleBitRepository::cmd_line(const string query_path) {
	
			string queryStr;
			::CXgetQuery(queryStr, string(query_path).c_str());
			cout<<queryStr<<endl;
			transQueSW->EnQueue(queryStr);
		
}

void CXTripleBitRepository::cmd_line_sm(FILE* fin, FILE* fout, const string query_path) {
	CXThreadPool::createAllPool();
	char cmd[256];
	while (true) {
		fflush(fin);
		fprintf(fout, ">>>");
		fscanf(fin, "%s", cmd);
		resultSet.resize(0);
		if (strcmp(cmd, "dp") == 0 || strcmp(cmd, "dumppredicate") == 0) {
			getPredicateTable()->dump();
		}
		else if (strcmp(cmd, "query") == 0) {
		}
		else if (strcmp(cmd, "source") == 0) {
			string queryStr;
			::CXgetQuery(queryStr, string(query_path).append("queryLUBM6").c_str());
			if (queryStr.length() == 0)
				continue;
		}
		else if (strcmp(cmd, "exit") == 0) {
			endForWorker();
			break;
		}
		else {
			string queryStr;
			::CXgetQuery(queryStr, string(query_path).append(cmd).c_str());

			if (queryStr.length() == 0)
				continue;
			transQueSW->EnQueue(queryStr);
		}
	}

	CXThreadPool::getChunkPool().wait();
	tempMMapDestroy();
	CXThreadPool::deleteAllPool();
}

extern char* QUERY_PATH;
void CXTripleBitRepository::cmd_line_cold(FILE* fin, FILE* fout, const string cmd) {
	string queryStr;
	CXgetQuery(queryStr, string(QUERY_PATH).append(cmd).c_str());
	if (queryStr.length() == 0) {
		cout << "queryStr.length() == 0!" << endl;
		return;
	}
	cout << "DataBase: " << DATABASE_PATH << " Query:" << cmd << endl;
	transQueSW->EnQueue(queryStr);
	endForWorker();
	tempMMapDestroy();
}

extern char* QUERY_PATH;
void CXTripleBitRepository::cmd_line_warm(FILE* fin, FILE* fout, const string cmd) {
	string queryStr;
	CXgetQuery(queryStr, string(QUERY_PATH).append(cmd).c_str());
	if (queryStr.length() == 0) {
		cout << "queryStr.length() == 0" << endl;
		return;
	}
	cout << "DataBase: " << DATABASE_PATH << " Query:" << cmd << endl;
	for (int i = 0; i < 10; i++) {
		transQueSW->EnQueue(queryStr);
	}
	endForWorker();
	tempMMapDestroy();
}


char*  CXTripleBitRepository::query_simple(const string querypath){
	// ofstream refile;
	// refile.open("results",ios::out);
	string dataall = "";
	char err[10] = "error";
	ifstream queryfile;
	vector<pair<ID,ID>> result;
	int querytype;
	queryfile.open(querypath);
	string temp,subject,predicate,object;
	ID predicateID , subjectID,objectID;
	while(queryfile>>temp){
		if(temp!="{"&&temp!="where{"){
			continue;
		}else{
			// cout<<temp<<endl;
			queryfile>>subject>>predicate>>object;
		}
	}
	
	if(predicate==""){
		cout<<"error"<<endl;
	}else{
		if(subject[0]=='?'){
			querytype=1;//find s by po;
			predicate = predicate.substr(1,strlen(predicate.c_str())-2);
			object = object.substr(1,strlen(object.c_str())-2);
			if(preTable->getIDByPredicate(predicate.c_str(),predicateID)==PREDICATE_NOT_BE_FINDED){
				return err;
			}
			UriTable->getIdByURI(object.c_str(),objectID);
			subject = -1;
		}else if(object[0]=='?'){
			querytype=0; // find o by sp;
			predicate = predicate.substr(1,strlen(predicate.c_str())-2);
			subject = subject.substr(1,strlen(subject.c_str())-2);
			if(preTable->getIDByPredicate(predicate.c_str(),predicateID)==PREDICATE_NOT_BE_FINDED){
				return err;
			}
			UriTable->getIdByURI(subject.c_str(),subjectID);
			object = -1;
		}
	}
	result = bitmapBuffer->query_simple(querytype,predicateID,subjectID,objectID);
	string so,t;
	if(querytype==0){
		for(auto i = result.begin();i!=result.end();i++){
			ID soid,tid;
			soid = i->first;
			tid = i->second;
			UriTable->getURIById(so,soid);
			UriTable->getURIById(t,tid);
			cout<<subject<<" "<<predicate<<" "<<so<<" "<<t<<endl;
			dataall = dataall+subject+"-?-"+predicate+"-?-"+so+"-?-"+t+"-?-";
		}
	}else{
		for(auto i = result.begin();i!=result.end();i++){
			ID soid,tid;
			soid = i->first;
			tid = i->second;
			UriTable->getURIById(so,soid);
			UriTable->getURIById(t,tid);
			cout<<so<<" "<<predicate<<" "<<object<<" "<<t<<endl;
			dataall = dataall+so+"-?-"+predicate+"-?-"+object+"-?-"+t+"-?-";
		}
	}

	char* re = new char[strlen(dataall.c_str())+1];
		// string re;
		// re = dataall;
	strcpy(re,dataall.c_str());
	return re;
}
