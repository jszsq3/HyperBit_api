/*
 * TripleBitRespository.h
 *
 *  Created on: May 13, 2010
 *      Author: root
 */

#ifndef TRIPLEBITRESPOSITORY_H_
#define TRIPLEBITRESPOSITORY_H_

class TripleBitBuilder;
class PredicateTable;
class URITable;
class URIStatisticsBuffer;
class BitmapBuffer;
class EntityIDBuffer;
class MMapBuffer;
class GXTripleBitWorker;
class CXTripleBitWorker;
class PartitionMaster;
class transQueueSW;
class TasksQueueWP;
class CXTasksQueueWP;
class ResultBuffer;
class IndexForTT;

#include "IRepository.h"
#include "TripleBit.h"
#include "StatisticsBuffer.h"
#include "GXTripleBitWorker.h"
#include "PartitionMaster.h"
#include "ThreadPool.h"

class GXTripleBitRepository: public GXIRepository {
public:
	map<string, map<string, vector<string>>> shixu;
public:
	

	PredicateTable * preTable;
	URITable* UriTable;
	BitmapBuffer* bitmapBuffer;
//	StatisticsBuffer *spStatisBuffer, *opStatisBuffer;
	StatisticsBuffer* subjectStat, * subPredicateStat, * objectStat, * objPredicateStat;
	EntityIDBuffer* buffer;
	int pos;
	string dataBasePath;

//	something about shared memory
	size_t workerNum, partitionNum;
	transQueueSW* transQueSW;
	vector<TasksQueueWP*> tasksQueueWP;
	vector<ResultBuffer*> resultWP;
	vector<boost::mutex *> tasksQueueWPMutex;
	boost::mutex * uriMutex;

	map<ID, GXTripleBitWorker*> tripleBitWorker;
	map<ID, PartitionMaster*> partitionMaster;

	MMapBuffer* bitmapImage, *bitmapPredicateImage;

	vector<string> resultSet;
	vector<string>::iterator resBegin;
	vector<string>::iterator resEnd;
	size_t hid;
//	some thing about QueryTest
	IndexForTT *indexForTT;

public:
//	vector<boost::mutex> tasksQueueMutex;

public:
	GXTripleBitRepository();
	virtual ~GXTripleBitRepository();

	//SO(id,string)transform
	bool find_soid_by_string(SOID& soid, const string& str);
	bool find_soid_by_string_update(SOID& soid, const string& str);
	bool find_string_by_soid(string& str, SOID& soid);

	//P(id,string)transform
	bool find_pid_by_string(PID& pid, const string& str);
	bool find_pid_by_string_update(PID& pid, const string& str);
	bool find_string_by_pid(string& str, ID& pid);

	//create a Repository specific in the path .
	static GXTripleBitRepository* create(const string &path);

	//Get some statistics information
	int get_predicate_count(PID pid);
	int get_subject_count(ID subjectID);
	int get_object_count(double object, char objType);
	int get_subject_predicate_count(ID subjectID, ID predicateID);
	int get_object_predicate_count(double object, ID predicateID, char objType);
	int get_subject_object_count(ID subjectID, double object, char objType = STRING);
	void save_cast(const string dir);


	PredicateTable* getPredicateTable() const {
		return preTable;
	}
	URITable* getURITable() const {
		return UriTable;
	}
	BitmapBuffer* getBitmapBuffer() const {
		return bitmapBuffer;
	}
	transQueueSW* getTransQueueSW() {
		return transQueSW;
	}
	vector<TasksQueueWP*> getTasksQueueWP() {
		return tasksQueueWP;
	}
	vector<ResultBuffer*> getResultWP() {
		return resultWP;
	}
	vector<boost::mutex*> getTasksQueueWPMutex() {
		return tasksQueueWPMutex;
	}
	boost::mutex* getUriMutex() {
		return uriMutex;
	}

	size_t getWorkerNum() {
		return workerNum;
	}
	size_t getPartitionNum() {
		return partitionNum;
	}
	string getDataBasePath() {
		return dataBasePath;
	}

	Status getSubjectByObjectPredicate(double object, ID pod, char objType = STRING);
	ID next();

	//lookup string id;
	bool lookup(const string& str, ID& id);
	Status nextResult(string& str);
	Status execute(string query);
	size_t getResultSize() const {
		return resultSet.size();
	}

	void tripleBitWorkerInit(int i);

	Status sharedMemoryInit();
	Status sharedMemoryDestroy();
	Status sharedMemoryTransQueueSWInit();
	Status sharedMemoryTransQueueSWDestroy();
	Status sharedMemoryTasksQueueWPInit();
	Status sharedMemoryTasksQueueWPDestroy();
	Status sharedMemoryResultWPInit();
	Status sharedMemoryResultWPDestroy();

	Status tempMMapDestroy();

	void endForWorker();
	void workerComplete();

	void cmd_line(FILE* fin, FILE* fout);
	void cmd_line_sm(FILE* fin, FILE* fout, const string query_path);
	void cmd_line_sm(FILE* fin, FILE* fout, const string query_path, const string query);
	void cmd_line_cold(FILE* fin, FILE* fout, const string cmd);
	void cmd_line_warm(FILE* fin, FILE* fout, const string cmd);
	void insertData(const string& query_path);


	static int colNo;

	void print();
};


class CXTripleBitRepository : public IRepository {
private:
	PredicateTable* preTable;
	URITable* UriTable;
	BitmapBuffer* bitmapBuffer;
	StatisticsBuffer* subjectStat, * subPredicateStat, * objectStat, * objPredicateStat;
	EntityIDBuffer* buffer;
	int pos;
	string dataBasePath;

	//	something about shared memory
	size_t workerNum, partitionNum;
	transQueueSW* transQueSW;
	vector<CXTasksQueueWP*> tasksQueueWP;
	vector<ResultBuffer*> resultWP;
	vector<boost::mutex*> tasksQueueWPMutex;
	boost::mutex* uriMutex;


	map<ID, PartitionMaster*> partitionMaster;

	MMapBuffer* bitmapImage, * bitmapIndexImage, * bitmapPredicateImage;

	vector<string> resultSet;
	vector<string>::iterator resBegin;
	vector<string>::iterator resEnd;


	//	some thing about QueryTest
	IndexForTT* indexForTT;

public:
	map<ID, CXTripleBitWorker*> tripleBitWorker;
	size_t hid;
	//	vector<boost::mutex> tasksQueueMutex;
	// CXTripleBitWorker* worker;
public:
	CXTripleBitRepository();
	virtual ~CXTripleBitRepository();

	//SO(id,string)transform
	bool find_soid_by_string(SOID& soid, const string& str);
	bool find_soid_by_string_update(SOID& soid, const string& str);
	bool find_string_by_soid(string& str, SOID& soid);

	//P(id,string)transform
	bool find_pid_by_string(PID& pid, const string& str);
	bool find_pid_by_string_update(PID& pid, const string& str);
	bool find_string_by_pid(string& str, ID& pid);

	//create a Repository specific in the path .
	static CXTripleBitRepository* create(const string& path);

	//Get some statistics information
	int	get_predicate_count(PID pid);
	int get_subject_count(ID subjectID);
	int get_object_count(ID objectID);
	int get_subject_predicate_count(ID subjectID, ID predicateID);
	int get_object_predicate_count(ID objectID, ID predicateID);
	int get_subject_object_count(ID subjectID, ID objectID);

	PredicateTable* getPredicateTable() const { return preTable; }
	URITable* getURITable() const { return UriTable; }
	BitmapBuffer* getBitmapBuffer() const { return bitmapBuffer; }
	transQueueSW* getTransQueueSW() { return transQueSW; }
	vector<CXTasksQueueWP*> getTasksQueueWP() { return tasksQueueWP; }
	vector<ResultBuffer*> getResultWP() { return resultWP; }
	vector<boost::mutex*> getTasksQueueWPMutex() { return tasksQueueWPMutex; }
	boost::mutex* getUriMutex() { return uriMutex; }

	size_t getWorkerNum() { return workerNum; }
	size_t getPartitionNum() { return partitionNum; }
	PartitionMaster* getPartitionMaster(ID partitionID) { return partitionMaster[partitionID]; }
	string getDataBasePath() { return dataBasePath; }


	StatisticsBuffer* getStatisticsBuffer(StatisticsBuffer::StatisticsType type) {
		switch (type) {
		case StatisticsBuffer::SUBJECT_STATIS:
			return subjectStat;
		case StatisticsBuffer::OBJECT_STATIS:
			return objectStat;
		case StatisticsBuffer::SUBJECTPREDICATE_STATIS:
			return subPredicateStat;
		case StatisticsBuffer::OBJECTPREDICATE_STATIS:
			return objPredicateStat;
		}

		return NULL;
	}

	//scan the database;
	Status getSubjectByObjectPredicate(ID oid, ID pod);
	ID next();

	//lookup string id;
	bool lookup(const string& str, ID& id);
	Status nextResult(string& str);
	Status execute(string& query);
	size_t getResultSize() const { return resultSet.size(); }
	void getResultSet(vector<string>& res) { res = tripleBitWorker[1]->resultSet; }
	void tripleBitWorkerInit(int i);
	void partitionMasterInit(CXTripleBitRepository*& repo, int i);

	Status sharedMemoryInit();
	Status sharedMemoryDestroy();
	Status sharedMemoryTransQueueSWInit();
	Status sharedMemoryTransQueueSWDestroy();
	Status sharedMemoryTasksQueueWPInit();
	Status sharedMemoryTasksQueueWPDestroy();
	Status sharedMemoryResultWPInit();
	Status sharedMemoryResultWPDestroy();

	Status tempMMapDestroy();
	void endPartitionMaster();

	void endForWorker();
	void workerComplete();
	void enqueue(FILE* fin, FILE* fout);
	void print(int f,CXTripleBitRepository*& repo);
	void cmd_line(const string queryStr);
	void cmd_line_sm(FILE* fin, FILE* fout, const string query_path);
	void cmd_line_cold(FILE* fin, FILE* fout, const string cmd);
	void cmd_line_warm(FILE* fin, FILE* fout, const string cmd);
	char* query_simple(const string querypath);
	static int colNo;
};
#endif /* TRIPLEBITRESPOSITORY_H_ */
