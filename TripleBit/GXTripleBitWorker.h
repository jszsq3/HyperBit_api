/*
 * GXTripleBitWorker.h
 *
 *  Created on: 2013-6-28
 *      Author: root
 */

#ifndef TRIPLEBITWORKER_H_
#define TRIPLEBITWORKER_H_

class BitmapBuffer;
class URITable;
class PredicateTable;
class GXTripleBitRepository;
class CXTripleBitRepository;
class TripleBitQueryGraph;
class EntityIDBuffer;
class HashJoin;
class SPARQLLexer;
class SPARQLParser;
class QuerySemanticAnalysis;
class PlanGenerator;
class transQueueSW;
class transaction;
class TasksQueueWP; 
class CXTasksQueueWP;
class GXTripleBitWorkerQuery;
class CXTripleBitWorkerQuery;

#include "TripleBit.h"
#include <boost/thread/thread.hpp>
using namespace boost;

class GXTripleBitWorker {
private:
	GXTripleBitRepository* tripleBitRepo;
	PredicateTable* preTable;
	URITable* uriTable;
	BitmapBuffer* bitmapBuffer;
	transQueueSW* transQueSW;

	QuerySemanticAnalysis* semAnalysis;
	PlanGenerator* planGen;
	TripleBitQueryGraph* queryGraph;
	vector<string> resultSet;
	vector<string>::iterator resBegin;
	vector<string>::iterator resEnd;
	vector<GXTripleNode> insertNodes; //����������

	struct Triple {
		string subject;
		string predicate;
		string object;
		string time;
		Triple(string subject, string predicate, string object, string time):
			subject(subject), predicate(predicate), object(object),time(time) {}
	};
	map<ID, vector<Triple*>> unParseTriple;

	ID workerID;

	boost::mutex* uriMutex;

	GXTripleBitWorkerQuery* workerQuery;
	transaction *trans;

public:
	static triplebit::atomic<size_t> insertTripleCount;
	GXTripleBitWorker(GXTripleBitRepository* repo, ID workID);
	Status Execute(string& queryString);
	virtual ~GXTripleBitWorker();
	void parse(string& line, string& subject, string& predicate, string& object, string& time);
	void parse_new(string& line,GXTripleNode &tripleNode);
	bool N3Parse(const char* fileName);
	Status NTriplesParse(const char* subject, const char* predicate, const char* object, const char* time, GXTripleNode& tripleNode);
	void Work();
};


class CXTripleBitWorker
{
public:
	vector<string> resultSet;
private:
	CXTripleBitRepository* tripleBitRepo;
	PredicateTable* preTable;
	URITable* uriTable;
	BitmapBuffer* bitmapBuffer;
	transQueueSW* transQueSW;
	CXTripleBitWorkerQuery* workerQuery;

	QuerySemanticAnalysis* semAnalysis;
	PlanGenerator* planGen;
	TripleBitQueryGraph* queryGraph;

	vector<string>::iterator resBegin;
	vector<string>::iterator resEnd;

	ID workerID;

	boost::mutex* uriMutex;


	transaction* trans;

public:
	CXTripleBitWorker(CXTripleBitRepository* repo, ID workID);
	
	Status Execute(string& queryString);
	Status Execute1(string& queryString);
	~CXTripleBitWorker() {}
	void Work();
	void Print();
};
#endif /* TRIPLEBITWORKER_H_ */
