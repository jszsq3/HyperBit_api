/*
 * GXTripleBitWorker.cpp
 *
 *  Created on: 2013-6-28
 *      Author: root
 */

#include "SPARQLLexer.h"
#include "SPARQLParser.h"
#include "QuerySemanticAnalysis.h"
#include "PlanGenerator.h"
#include "TripleBitQueryGraph.h"
#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "GXTripleBitRepository.h"
#include "URITable.h"
#include "PredicateTable.h"
#include "EntityIDBuffer.h"
#include "util/BufferManager.h"
#include "GXTripleBitWorker.h"
#include "comm/TransQueueSW.h"
#include "GXTripleBitWorkerQuery.h"
#include "GXTempBuffer.h"
#include "OSFile.h"
#include "TurtleParser.hpp"

//#define MYDEBUG
#define TOTAL_TIME

triplebit::atomic<size_t> GXTripleBitWorker::insertTripleCount(0);

//��ʼ��һ������
GXTripleBitWorker::GXTripleBitWorker(GXTripleBitRepository *repo, ID workID)
{
	tripleBitRepo = repo;
	preTable = repo->getPredicateTable();
	uriTable = repo->getURITable();
	bitmapBuffer = repo->getBitmapBuffer();
	transQueSW = repo->getTransQueueSW();
	uriMutex = repo->getUriMutex();

	workerID = workID;

	queryGraph = new TripleBitQueryGraph();
	// planGen = new PlanGenerator(*repo);
	// semAnalysis = new QuerySemanticAnalysis(*repo);
	planGen = NULL;
	semAnalysis = NULL;
	workerQuery = new GXTripleBitWorkerQuery(repo, workerID);
	
}

GXTripleBitWorker::~GXTripleBitWorker()
{
	if (workerQuery != NULL)
	{
		delete workerQuery;
		workerQuery = NULL;
	}
	if (semAnalysis != NULL)
	{
		delete semAnalysis;
		semAnalysis = NULL;
	}
	if (planGen != NULL)
	{
		delete planGen;
		planGen = NULL;
	}
	if (queryGraph != NULL)
	{
		delete queryGraph;
		queryGraph = NULL;
	}
	transQueSW = NULL;
	uriMutex = NULL;
	uriTable = NULL;
	preTable = NULL;
	bitmapBuffer = NULL;
	tripleBitRepo = NULL;
}

void GXTripleBitWorker::Work()
{
	while (1)
	{
		trans = transQueSW->DeQueue();
		string queryString = trans->transInfo;
		if (queryString == "exit")
		{
			delete trans;
			tripleBitRepo->workerComplete();
			break;
		}
		Execute(queryString);
		delete trans;
	}
}

void GXTripleBitWorker::parse(string &line, string &subject, string &predicate, string &object, string &time)
{
	int l, r;

	l = line.find_first_of('<') + 1;
	r = line.find_first_of('>');
	subject = line.substr(l, r - l);
	line = line.substr(r + 1);

	l = line.find_first_of('<') + 1;
	r = line.find_first_of('>');
	predicate = line.substr(l, r - l);
	line = line.substr(r + 1);

	if (line.find_first_of('\"') != string::npos)
	{
		l = line.find_first_of('\"') + 1;
		r = line.find_last_of('\"');
		object = line.substr(l, r - l);
		line = line.substr(r + 1);
	}
	else
	{
		l = line.find_first_of('<') + 1;
		r = line.find_first_of('>');
		object = line.substr(l, r - l);
		line = line.substr(r + 1);
	}

	if (line.find_first_of('<') != string::npos)
	{
		l = line.find_first_of('<') + 1;
		r = line.find_first_of('>');
		time = line.substr(l, r - l);
		// tripleBitRepo->shixu[subject][predicate].push_back(  t + "    " + object);
	}
	else
	{
		time = "no";
	}
}

void GXTripleBitWorker::parse_new(string &line,GXTripleNode &tripleNode)
{
	if (line[0] == '<')
	{
		string subject, predicate, object, time;
		int l, r;
		int p;
		// subject
		l = line.find_first_of('<') + 1;
		r = line.find_first_of('>');
		subject = line.substr(l, r - l);
		line = line.substr(r + 1);
		// predicate
		l = line.find_first_of('<') + 1;
		r = line.find_first_of('>');
		predicate = line.substr(l, r - l);
		line = line.substr(r + 1);
		// object
		if (line.find_first_of('\"') != string::npos)
		{
			l = line.find_first_of('\"') + 1;
			r = line.find_last_of('\"');
			object = line.substr(l, r - l);
			line = line.substr(r + 1);
		}
		else
		{
			l = line.find_first_of('<') + 1;
			r = line.find_first_of('>');
			object = line.substr(l, r - l);
			line = line.substr(r + 1);
		}
		// time
		if (line.find_first_of('<') != string::npos)
		{
			l = line.find_first_of('<') + 1;
			r = line.find_first_of('>');
			time = line.substr(l, r - l);
		}
		else
		{
			time = "no";
		}
		if (subject.length() && predicate.length() && object.length() && time.length())
		{
			Status status = NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), time.c_str(), tripleNode);
			queryGraph->getQuery().GXtripleNodes.push_back(tripleNode);
		}
	}
	else if (line[0] == '{')
	{
		string subject, predicate, object, time;

		time = "no";
		subject = "hid" + to_string(tripleBitRepo->hid);

		const char* deli = "{},.\r";
		char cs[line.size()+1];
		strcpy(cs, line.c_str());
		char* p;

		p = strtok(cs, deli);
		predicate = "rdfp";
		object = p;
		if (subject.length() && predicate.length() && object.length() && time.length())
		{
			Status status = NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), time.c_str(), tripleNode);
			queryGraph->getQuery().GXtripleNodes.push_back(tripleNode);
		}

		p = strtok(NULL, deli);
		predicate = "rdfs";
		object = p;
		if (subject.length() && predicate.length() && object.length() && time.length())
		{
			Status status = NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), time.c_str(), tripleNode);
			queryGraph->getQuery().GXtripleNodes.push_back(tripleNode);
		}

		p = strtok(NULL, deli);
		predicate = "rdfst";
		object = p;
		if (subject.length() && predicate.length() && object.length() && time.length())
		{
			Status status = NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), time.c_str(), tripleNode);
			queryGraph->getQuery().GXtripleNodes.push_back(tripleNode);
		}

		p = strtok(NULL, deli);
		predicate = "rdfet";
		object = p;
		if (subject.length() && predicate.length() && object.length() && time.length())
		{
			Status status = NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), time.c_str(), tripleNode);
			queryGraph->getQuery().GXtripleNodes.push_back(tripleNode);
		}

		p = strtok(NULL, deli);
		while (p) {
			object = p;
			predicate = "rdfo";
			if (subject.length() && predicate.length() && object.length() && time.length())
		{
			Status status = NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), time.c_str(), tripleNode);
			queryGraph->getQuery().GXtripleNodes.push_back(tripleNode);
		}
			p = strtok(NULL, deli);
			object.clear();
		}
		++tripleBitRepo->hid;
	}
}

Status GXTripleBitWorker::NTriplesParse(const char *subject, const char *predicate, const char *object, const char *time, GXTripleNode &tripleNode)
{
	ID subjectID, predicateID, objectID, timeID;
	if (preTable->getIDByPredicate(predicate, predicateID) == PREDICATE_NOT_BE_FINDED)
	{
		preTable->insertTable(predicate, predicateID);
	}
	if (uriTable->getIdByURI(subject, subjectID) == URI_NOT_FOUND)
	{
		uriTable->insertTable(subject, subjectID);
	}
	if (uriTable->getIdByURI(object, objectID) == URI_NOT_FOUND)
	{
		uriTable->insertTable(object, objectID);
	}
	if (uriTable->getIdByURI(time, timeID) == URI_NOT_FOUND)
	{
		uriTable->insertTable(time, timeID);
	}
	tripleNode.subjectID = subjectID;
	tripleNode.predicateID = predicateID;
	tripleNode.objectID = objectID;
	tripleNode.timeID = timeID;
	return OK;
}

bool GXTripleBitWorker::N3Parse(const char *fileName)
{
	//	cerr << "Parsing " << fileName << "..." << endl;
	try
	{
		MMapBuffer *slice = new MMapBuffer(fileName, 0);
		const uchar *reader = (const uchar *)slice->getBuffer(), *tempReader;
		const uchar *limit = (const uchar *)(slice->getBuffer() + slice->getSize());
		// char buffer[256];
		string line, subject, predicate, object, time;
		ID tr_id = 0;
		GXTripleNode tripleNode;
		while (reader < limit)
		{
			tempReader = reader;
			// memset(buffer, 0, sizeof(buffer));
			while (tempReader < limit && *tempReader != '\n')
			{
				tempReader++;
			}
			if (tempReader != reader)
			{
				char buffer[tempReader - reader + 1];
				memset(buffer, 0, sizeof(buffer));
				memcpy(buffer, reader, tempReader - reader);
				tempReader++;
				reader = tempReader;
				line = buffer;
				if (line.empty())
					continue;
				insertTripleCount++;
				parse_new(line,tripleNode);
				// parse(line, subject, predicate, object, time);
				// if (subject.length() && predicate.length() && object.length() && time.length())
				// {
				// 	Status status = NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), time.c_str(), tripleNode);
				// 	queryGraph->getQuery().GXtripleNodes.push_back(tripleNode);
				// }
			}
			else
			{
				reader = tempReader;
				break;
			}
		}
	}
	catch (const TurtleParser::Exception &)
	{
		return false;
	}
	return true;
}

Status GXTripleBitWorker::Execute(string &queryString)
{
	//����
	if (queryString.find_first_of("insert_path:") == 0)
	{
		//�������ݼ�·��
		string insert_path = queryString.substr(queryString.find_first_of(":") + 1);

		queryGraph->Clear();
		queryGraph->setOpType(TripleBitQueryGraph::INSERT_DATA);

		//�����������ݼ�
		N3Parse(insert_path.c_str());

		workerQuery->query(queryGraph, resultSet, trans->transTime);
	}
	//��ѯ
	else
	{
		cout << "chaxun" << endl;
	}
	workerQuery->releaseBuffer();

	return OK;
}

//----------------------------------------------

//#define MYDEBUG
//#define TEST_QUERYTYPE

CXTripleBitWorker::CXTripleBitWorker(CXTripleBitRepository *repo, ID workID)
{
	tripleBitRepo = repo;
	preTable = repo->getPredicateTable();
	uriTable = repo->getURITable();
	bitmapBuffer = repo->getBitmapBuffer();
	transQueSW = repo->getTransQueueSW();
	uriMutex = repo->getUriMutex();

	workerID = workID;

	queryGraph = new TripleBitQueryGraph();
	planGen = new PlanGenerator(*repo);
	semAnalysis = new QuerySemanticAnalysis(*repo);
	workerQuery = new CXTripleBitWorkerQuery(repo, workerID);
}

void CXTripleBitWorker::Work()
{
	while (1)
	{
		trans = transQueSW->DeQueue();
#ifdef DEBUG
		cout << "transTime(sec): " << trans->transTime.tv_sec << endl;
		cout << "transTime(usec): " << trans->transTime.tv_usec << endl;
		cout << "transInfo: " << trans->transInfo << endl;
#endif
		string queryString = trans->transInfo;
		if (queryString == "exit")
		{
			delete trans;
			tripleBitRepo->workerComplete();
			break;
		}
		Execute(queryString);
		delete trans;
	}
}

//��ѯ
extern int refile_mutex;
Status CXTripleBitWorker::Execute(string &queryString)
{
	//ʱ���ѯ
	int mod = 0;
	
	if (queryString.find_first_of("-t") == 0)
	{
		queryString = queryString.substr(queryString.find_first_of("-t") + 2);
		mod = 1;
	}
#ifdef TOTAL_TIME
	struct timeval start, end;
	gettimeofday(&start, NULL);
#endif

	resultSet.resize(0);
	
	SPARQLLexer *lexer = new SPARQLLexer(queryString);
	SPARQLParser *parser = new SPARQLParser(*lexer);
	try
	{
		parser->parse();
	}
	catch (const SPARQLParser::ParserException &e)
	{
		cout << "Parser error: " << e.message << endl;
		return ERR;
	}

	if (parser->getOperationType() == SPARQLParser::QUERY)
	{
		queryGraph->Clear();
		uriMutex->lock();
		if (!this->semAnalysis->transform(*parser, *queryGraph))
		{
			return NOT_FOUND;
		}
		uriMutex->unlock();

		if (queryGraph->knownEmpty() == true)
		{
			cout << "Empty result" << endl;
			refile_mutex = 1;
			resultSet.push_back("-1");
			return OK;
		}

		if (queryGraph->isPredicateConst() == false)
		{
			resultSet.push_back("-1");
			resultSet.push_back("predicate should be constant");
			return ERR;
		}
#ifdef DEBUG
		cout << "---------------After transform----------------" << endl;
		Print();
		cout << endl;
#endif
		planGen->generatePlan(*queryGraph);
#ifdef DEBUG
		cout << "---------------After GeneratePlan-------------" << endl;
		Print();
		cout << endl;
#endif

		workerQuery->query(mod, queryGraph, resultSet, trans->transTime);
#ifdef TOTAL_TIME
		gettimeofday(&end, NULL);
		// cout << " time elapsed: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << " s" << endl;
#endif

		workerQuery->releaseBuffer();
	}
	else
	{
		queryGraph->Clear();

		uriMutex->lock();
		if (!this->semAnalysis->transform(*parser, *queryGraph))
		{
			return ERR;
		}
		uriMutex->unlock();

#ifdef MYDEBUG
		Print();
#endif

		workerQuery->query(mod, queryGraph, resultSet, trans->transTime);

#ifdef TOTAL_TIME
		gettimeofday(&end, NULL);
		cout << " time elapsed: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << " s" << endl;
#endif

		workerQuery->releaseBuffer();
	}
	delete lexer;
	delete parser;
	return OK;
}

Status CXTripleBitWorker::Execute1(string &queryString)
{
	//ʱ���ѯ
	int mod = 0;
	
	if (queryString.find_first_of("-t") == 0)
	{
		queryString = queryString.substr(queryString.find_first_of("-t") + 2);
		mod = 1;
	}
#ifdef TOTAL_TIME
	struct timeval start, end;
	gettimeofday(&start, NULL);
#endif

	resultSet.resize(0);
	
	SPARQLLexer *lexer = new SPARQLLexer(queryString);
	SPARQLParser *parser = new SPARQLParser(*lexer);
	try
	{
		parser->parse();
	}
	catch (const SPARQLParser::ParserException &e)
	{
		cout << "Parser error: " << e.message << endl;
		return ERR;
	}

	if (parser->getOperationType() == SPARQLParser::QUERY)
	{
		queryGraph->Clear();
		uriMutex->lock();
		if (!this->semAnalysis->transform(*parser, *queryGraph))
		{
			return NOT_FOUND;
		}
		uriMutex->unlock();

		if (queryGraph->knownEmpty() == true)
		{
			cout << "Empty result" << endl;
			resultSet.push_back("-1");
			return OK;
		}

		if (queryGraph->isPredicateConst() == false)
		{
			resultSet.push_back("-1");
			resultSet.push_back("predicate should be constant");
			return ERR;
		}
#ifdef DEBUG
		cout << "---------------After transform----------------" << endl;
		Print();
		cout << endl;
#endif
		planGen->generatePlan(*queryGraph);
#ifdef DEBUG
		cout << "---------------After GeneratePlan-------------" << endl;
		Print();
		cout << endl;
#endif

		workerQuery->query1(mod, queryGraph, resultSet, trans->transTime);
#ifdef TOTAL_TIME
		gettimeofday(&end, NULL);
		cout << " time elapsed: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << " s" << endl;
#endif

		workerQuery->releaseBuffer();
	}
	else
	{
		queryGraph->Clear();

		uriMutex->lock();
		if (!this->semAnalysis->transform(*parser, *queryGraph))
		{
			return ERR;
		}
		uriMutex->unlock();

#ifdef MYDEBUG
		Print();
#endif

		workerQuery->query(mod, queryGraph, resultSet, trans->transTime);

#ifdef TOTAL_TIME
		gettimeofday(&end, NULL);
		cout << " time elapsed: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << " s" << endl;
#endif

		workerQuery->releaseBuffer();
	}
	delete lexer;
	delete parser;
	return OK;
}

void CXTripleBitWorker::Print()
{
	TripleBitQueryGraph::SubQuery &query = queryGraph->getQuery();
	unsigned int i, size, j;
	size = query.tripleNodes.size();
	cout << "join triple size: " << size << endl;

	vector<CXTripleNode> &triples = query.tripleNodes;
	for (i = 0; i < size; i++)
	{
		cout << i << " triple: " << endl;
		cout << triples[i].constSubject << " " << triples[i].subject << endl;
		cout << triples[i].constPredicate << " " << triples[i].predicate << endl;
		cout << triples[i].constObject << " " << triples[i].object << endl;
		cout << endl;
	}

	size = query.joinVariables.size();
	cout << "join variables size: " << size << endl;
	vector<TripleBitQueryGraph::JoinVariableNodeID> &variables = query.joinVariables;
	for (i = 0; i < size; i++)
	{
		cout << variables[i] << endl;
	}

	vector<TripleBitQueryGraph::JoinVariableNode> &nodes = query.joinVariableNodes;
	size = nodes.size();
	cout << "join variable nodes size: " << size << endl;
	for (i = 0; i < size; i++)
	{
		cout << i << "variable nodes" << endl;
		cout << nodes[i].value << endl;
		for (j = 0; j < nodes[i].appear_tpnodes.size(); j++)
		{
			cout << nodes[i].appear_tpnodes[j].first << " " << nodes[i].appear_tpnodes[j].second << endl;
		}
		cout << endl;
	}

	size = query.joinVariableEdges.size();
	cout << "join variable edges size: " << size << endl;
	vector<TripleBitQueryGraph::JoinVariableNodesEdge> &edge = query.joinVariableEdges;
	for (i = 0; i < size; i++)
	{
		cout << i << " edge" << endl;
		cout << "From: " << edge[i].from << "To: " << edge[i].to << endl;
	}

	cout << " query type: " << query.joinGraph << endl;
	cout << " root ID: " << query.rootID << endl;
	cout << " query leafs: ";
	size = query.leafNodes.size();
	for (i = 0; i < size; i++)
	{
		cout << query.leafNodes[i] << " ";
	}
	cout << endl;

	vector<ID> &projection = queryGraph->getProjection();
	cout << "variables need to project: " << endl;
	cout << "variable count: " << projection.size() << endl;
	for (i = 0; i < projection.size(); i++)
	{
		cout << projection[i] << endl;
	}
	cout << endl;
}
