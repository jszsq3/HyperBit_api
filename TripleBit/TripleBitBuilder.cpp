/*
 * TripleBitBuilder.cpp
 *
 *  Created on: Apr 6, 2010
 *      Author: root
 */

#include "MemoryBuffer.h"
#include "MMapBuffer.h"
#include "BitmapBuffer.h"
#include "TripleBitBuilder.h"
#include "PredicateTable.h"
#include "StatisticsBuffer.h"
#include "TripleBit.h"
#include "URITable.h"
#include "Sorter.h"

#include <string.h>
#include <pthread.h>
#include <fstream>

//#define MYDEBUG

static int getCharPos(const char* data, char ch) {
	const char * p = data;
	int i = 0;
	while (*p != '\0') {
		if (*p == ch)
			return i + 1;
		p++;
		i++;
	}

	return -1;
}

TripleBitBuilder::TripleBitBuilder(string _dir) : dir(_dir) {

	preTable = new PredicateTable(dir);
	uriTable = new URITable(dir);


	bitmap = new BitmapBuffer(dir);

	statBuffer[0] = new OneConstantStatisticsBuffer(string(dir + "/subject_statis"), StatisticsBuffer::SUBJECT_STATIS);			//subject statistics buffer;
	statBuffer[1] = new OneConstantStatisticsBuffer(string(dir + "/object_statis"), StatisticsBuffer::OBJECT_STATIS);			//object statistics buffer;
	statBuffer[2] = new TwoConstantStatisticsBuffer(string(dir + "/subjectpredicate_statis"), StatisticsBuffer::SUBJECTPREDICATE_STATIS);	//subject-predicate statistics buffer;
	statBuffer[3] = new TwoConstantStatisticsBuffer(string(dir + "/objectpredicate_statis"), StatisticsBuffer::OBJECTPREDICATE_STATIS);	//object-predicate statistics buffer;

	staReifTable = new StatementReificationTable();

	hid = 0;
}

TripleBitBuilder::TripleBitBuilder() {
	preTable = NULL;
	uriTable = NULL;
	bitmap = NULL;
	staReifTable = NULL;
	hid = 0;
}

TripleBitBuilder::~TripleBitBuilder() {
	// TODO Auto-generated destructor stub
#ifdef TRIPLEBITBUILDER_DEBUG
	cout << "Bit map builder destroyed begin " << endl;
#endif
	//mysql = NULL;
	if (preTable != NULL)
		delete preTable;
	preTable = NULL;

	if (uriTable != NULL)
		delete uriTable;
	uriTable = NULL;
	//delete uriStaBuffer;
	if (staReifTable != NULL)
		delete staReifTable;
	staReifTable = NULL;

	if (bitmap != NULL) {
		delete bitmap;
		bitmap = NULL;
	}

}

bool TripleBitBuilder::isStatementReification(const char* object) {
	int pos;

	const char* p;

	if ((pos = getCharPos(object, '#')) != -1) {
		p = object + pos;

		if (strcmp(p, "Statement") == 0 || strcmp(p, "subject") == 0 || strcmp(p, "predicate") == 0
				|| strcmp(p, "object") == 0) {
			return true;
		}
	}

	return false;
}

bool lexDate(string &str, double& date) {
	if (str.empty()) {
		return false;
	}
	TurtleParser::strim(str);
	if (str.empty() || str.length() != 19) {
		return false;
	}
	if (str[0] >= '0' && str[0] <= '9' && str[1] >= '0' && str[1] <= '9' && str[2] >= '0' && str[2] <= '9'
			&& str[3] >= '0' && str[3] <= '9' && str[4] == '-' && str[5] >= '0' && str[5] <= '1' && str[6] >= '0'
			&& str[6] <= '9' && str[7] == '-' && str[8] >= '0' && str[8] <= '3' && str[9] >= '0' && str[9] <= '9'
			&& str[10] == ' ' && str[11] >= '0' && str[11] <= '2' && str[12] >= '0' && str[12] <= '9' && str[13] == ':'
			&& str[14] >= '0' && str[14] <= '5' && str[15] >= '0' && str[15] <= '9' && str[16] == ':' && str[17] >= '0'
			&& str[17] <= '5' && str[18] >= '0' && str[18] <= '9') {
		date = (str[0] - '0');
		date = date * 10 + (str[1] - '0');
		date = date * 10 + (str[2] - '0');
		date = date * 10 + (str[3] - '0');
		date = date * 10 + (str[5] - '0');
		date = date * 10 + (str[6] - '0');
		date = date * 10 + (str[8] - '0');
		date = date * 10 + (str[9] - '0');
		date = date * 10 + (str[11] - '0');
		date = date * 10 + (str[12] - '0');
		date = date * 10 + (str[14] - '0');
		date = date * 10 + (str[15] - '0');
		date = date * 10 + (str[17] - '0');
		date = date * 10 + (str[18] - '0');
		return true;
	}
	return false;
}

//��һ�����ݽ��н���
void TripleBitBuilder::parse(string& line, TempFile& rawFacts) {
	//��ͨ�߽���
	if (line[0] == '<') {
		string subject, predicate, object, time;
		int l, r;
		int p;
		//subject
		l = line.find_first_of('<') + 1;
		r = line.find_first_of('>');
		subject = line.substr(l, r - l);
		line = line.substr(r + 1);
		//predicate
		l = line.find_first_of('<') + 1;
		r = line.find_first_of('>');
		predicate = line.substr(l, r - l);
		line = line.substr(r + 1);
		//object
		if (line.find_first_of('\"') != string::npos) {
			l = line.find_first_of('\"') + 1;
			r = line.find_last_of('\"');
			object = line.substr(l, r - l);
			line = line.substr(r + 1);
		}
		else {
			l = line.find_first_of('<') + 1;
			r = line.find_first_of('>');
			object = line.substr(l, r - l);
			line = line.substr(r + 1);
		}
		//time
		if (line.find_first_of('<') != string::npos) {
			l = line.find_first_of('<') + 1;
			r = line.find_first_of('>');
			time = line.substr(l, r - l);
		}
		else {
			time = "no";
		}
		if (subject.length() && predicate.length() && object.length() && time.length()) {
			NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), time.c_str(), rawFacts);
		}
	}
	//����ʱ�򳬱�
	else if(line[0]=='{'){
		
		string subject, predicate, object, time;

		time = "no";
		subject = "hid" + to_string(hid);

		const char* deli = "{},.\r";
		char cs[line.size()+1];
		strcpy(cs, line.c_str());
		char* p;

		p = strtok(cs, deli);
		predicate = "rdfp";
		object = p;
		if (subject.length() && predicate.length() && object.length() && time.length()) {
			NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), time.c_str(), rawFacts);
		}

		p = strtok(NULL, deli);
		predicate = "rdfs";
		object = p;
		if (subject.length() && predicate.length() && object.length() && time.length()) {
			NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), time.c_str(), rawFacts);
		}

		p = strtok(NULL, deli);
		predicate = "rdfst";
		object = p;
		if (subject.length() && predicate.length() && object.length() && time.length()) {
			NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), time.c_str(), rawFacts);
		}

		p = strtok(NULL, deli);
		predicate = "rdfet";
		object = p;
		if (subject.length() && predicate.length() && object.length() && time.length()) {
			NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), time.c_str(), rawFacts);
		}

		p = strtok(NULL, deli);
		while (p) {
			object = p;
			predicate = "rdfo";
			if (subject.length() && predicate.length() && object.length() && time.length()) {
				NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), time.c_str(), rawFacts);
			}
			p = strtok(NULL, deli);
			object.clear();
		}
		++hid;
	}



}

void TripleBitBuilder::NTriplesParse(const char* subject, const char* predicate, const char* object, const char* time,TempFile& facts) {
	ID subjectID, predicateID, objectID,timeID;
	if (preTable->getIDByPredicate(predicate, predicateID) == PREDICATE_NOT_BE_FINDED) {
		preTable->insertTable(predicate, predicateID);
	}
	if (uriTable->getIdByURI(subject, subjectID) == URI_NOT_FOUND) {
		uriTable->insertTable(subject, subjectID);
	}
	if (uriTable->getIdByURI(object, objectID) == URI_NOT_FOUND) {
		uriTable->insertTable(object, objectID);
	}
	if (uriTable->getIdByURI(time, timeID) == URI_NOT_FOUND) {
		uriTable->insertTable(time, timeID);
	}
	facts.writeIDIDID(subjectID, predicateID, objectID);
	facts.writeID(timeID);
}
//谓词占位
void TripleBitBuilder::init_database(){
	ID testsid,testpid=1;
	string teste = "1";
	// if (preTable->getIDByPredicate(predicate, predicateID) == PREDICATE_NOT_BE_FINDED) {
	// 	preTable->insertTable(predicate, predicateID);
	// }
	if (uriTable->getIdByURI(teste.c_str(), testsid) == URI_NOT_FOUND) {
		uriTable->insertTable(teste.c_str(), testsid);
	}
	auto take_place_p = preTable->idtostring;
	auto iter = take_place_p.begin();
	for(;iter!=take_place_p.end();iter++){
		if (preTable->getIDByPredicate_init(iter->second.c_str(), testpid) == PREDICATE_NOT_BE_FINDED) {
			preTable->insertTable_init(iter->second.c_str(), testpid);
		}
		bitmap->insertTriple(testpid,testsid,testsid,testsid,ORDERBYS);
		bitmap->insertTriple(testpid,testsid,testsid,testsid,ORDERBYO);
	}
}
//�ļ�����
//filename�����ݼ�·��
//rawFacts�ǽ����õ��ļ�·�� ��ʽΪ <SID,PID,OID,TID>...  
bool TripleBitBuilder::N3Parse(const char* filename, TempFile& rawFacts) {
	cerr << "Parsing " << filename << "..." << endl;
	MMapBuffer* slice = new MMapBuffer(filename,0);
	const uchar* reader = (const uchar*)slice->getBuffer(), * tempReader;
	const uchar* limit = (const uchar*)(slice->getBuffer() + slice->getSize());
	// char buffer[8192];
	string line;
	//谓词占位符
	init_database();
	while (reader < limit) {
		tempReader = reader;
		
		while (tempReader < limit && *tempReader != '\n') {
			tempReader++;
		}
		if (tempReader != reader) {
			char buffer[tempReader - reader+1];
			memset(buffer, 0, sizeof(buffer));
			memcpy(buffer, reader, tempReader - reader);
			line = buffer;
			// cout<<line<<endl;
			// cout<<buffer[tempReader - reader-1]<<endl;
			parse(line, rawFacts);
		}
		tempReader++;
		reader = tempReader;
	}
	return true;
}

const uchar* TripleBitBuilder::skipIdIdId(const uchar* reader) {
	return TempFile::skipID(TempFile::skipID(TempFile::skipID(TempFile::skipID(reader))));
}
int TripleBitBuilder::compare213(const uchar* left, const uchar* right) {
	ID l1, l2,l3, r1, r2, r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);
	return cmpTriples(l2, l1, l3, r2, r1, r3);
}
int TripleBitBuilder::compare231(const uchar* left, const uchar* right) {
	ID l1, l2, l3, r1, r2, r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);
	return cmpTriples(l2, l3, l1,  r2, r3, r1);
}

int TripleBitBuilder::compare123(const uchar* left, const uchar* right) {
	ID l1, l2,l3, r1, r2,r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);
	return cmpTriples(l1, l2, l3, r1, r2, r3);
}

int TripleBitBuilder::compare321(const uchar* left, const uchar* right) {
	ID l1, l2,l3, r1, r2,r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);
	return cmpTriples(l3, l2, l1,  r3, r2, r1);
}

void print(TempFile& infile, char* outfile) {
	MemoryMappedFile mappedIn;
	assert(mappedIn.open(infile.getFile().c_str()));
	const uchar* reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();

	// Produce tempfile
	ofstream out(outfile);
	while (reader < limit) {
		out << *(ID*) reader << "\t" << *(ID*) (reader + 4) << "\t" << *(double*) (reader + 8) << *(char*) (reader + 16) << endl;
		reader += 17;
	}
	mappedIn.close();
	out.close();
}

Status TripleBitBuilder::resolveTriples(TempFile& rawFacts) {//rawFacts = <SID,PID,OID,TID>...
	int insertNum=0;
	ID subjectID, lastSubjectID, predicateID, lastPredicateID, objectID, lastObjectID,timeID,lastTimeID;
	//string predicates;
	//bool diff = false;
	//size_t count1 = 0;

	unsigned count0 = 0, count1 = 0;
	TempFile sortedBySubject("./SortByS"), sortedByObject("./SortByO");


	MemoryMappedFile mappedIn;
	const uchar* reader;
	const uchar* limit;

	//��S����
	cout << "Sort by Subject..." << endl;
	Sorter::sort(rawFacts, sortedBySubject, skipIdIdId, compare123);
	cout << "Sort by Subject Over." << endl;
	sortedBySubject.close();

	assert(mappedIn.open(sortedBySubject.getFile().c_str()));
	reader = mappedIn.getBegin();
	limit = mappedIn.getEnd();


	loadTriple(reader, subjectID, predicateID, objectID, timeID);//readerָ���Զ�����
	lastSubjectID = subjectID;
	lastPredicateID = predicateID;
	lastObjectID = objectID;
	lastTimeID = timeID;
	bitmap->insertTriple(predicateID, subjectID, objectID,timeID, ORDERBYS);insertNum++;
	count0 = count1 = 1;


	while (reader < limit) {
		loadTriple(reader, subjectID, predicateID, objectID, timeID);
		if (lastSubjectID == subjectID && lastPredicateID == predicateID && lastObjectID == objectID && lastTimeID == timeID) {
			continue;
		}
		if (subjectID != lastSubjectID) {
			((OneConstantStatisticsBuffer*)statBuffer[0])->addStatis(lastSubjectID, count0);
			statBuffer[2]->addStatis(lastSubjectID, lastPredicateID, count1);
			count0 = count1 = 1;
		}
		else if (predicateID != lastPredicateID) {
			statBuffer[2]->addStatis(lastSubjectID, lastPredicateID, count1);
			count0++; count1 = 1;
		}
		else {
			count0++; count1++;
		}
		bitmap->insertTriple(predicateID, subjectID, objectID, timeID, ORDERBYS);insertNum++;
		lastSubjectID = subjectID;
		lastPredicateID = predicateID;
		lastObjectID = objectID;
		lastTimeID = timeID;
	}



	mappedIn.close();
	bitmap->flush();

	cout<<"TotalInsertNum:"<<insertNum<<endl;

	//��O����
	cerr << "Sort by Object..." << endl;
	Sorter::sort(rawFacts, sortedByObject, skipIdIdId, compare321);
	cerr << "Sort by Object Over." << endl;
	sortedByObject.close();
	assert(mappedIn.open(sortedByObject.getFile().c_str()));
	reader = mappedIn.getBegin();
	limit = mappedIn.getEnd();

	loadTriple(reader, subjectID, predicateID, objectID, timeID);
	lastSubjectID = subjectID;
	lastPredicateID = predicateID;
	lastObjectID = objectID;
	lastTimeID = timeID;
	bitmap->insertTriple(predicateID, subjectID, objectID,timeID, ORDERBYO);
	count0 = count1 = 1;

	while (reader < limit) {
		loadTriple(reader, subjectID, predicateID, objectID, timeID);
		if (lastSubjectID == subjectID && lastPredicateID == predicateID && lastObjectID == objectID) {
			continue;
		}
		if (objectID != lastObjectID) {
			((OneConstantStatisticsBuffer*)statBuffer[1])->addStatis(lastObjectID, count0);
			statBuffer[3]->addStatis(lastObjectID, lastPredicateID, count1);
			count0 = count1 = 1;
		}
		else if (predicateID != lastPredicateID) {
			statBuffer[3]->addStatis(lastObjectID, lastPredicateID, count1);
			count0++; count1 = 1;
		}
		else {
			count0++; count1++;
		}
		bitmap->insertTriple(predicateID, subjectID, objectID, timeID, ORDERBYO);
		lastSubjectID = subjectID;
		lastPredicateID = predicateID;
		lastObjectID = objectID;
		lastTimeID = timeID;
	}

	mappedIn.close();
	bitmap->flush();

	rawFacts.discard();
	sortedByObject.discard();
	sortedBySubject.discard();
}

Status TripleBitBuilder::startBuildN3(string fileName) {
	TempFile rawFacts("./test");

	ifstream in((char*) fileName.c_str());
	if (!in.is_open()) {
		cerr << "Unable to open " << fileName << endl;
		return ERROR;
	}
	if (!N3Parse(fileName.c_str(), rawFacts)) {
		in.close();
		return ERROR;
	}

	in.close();

	//�ѻ���������ȫ��ˢ�µ��ļ�rawFacts
	rawFacts.flush();

	cout << "Parsing Over." << endl;

	resolveTriples(rawFacts);

	return OK;
}

//Status TripleBitBuilder::buildIndex() {
//	// build hash index;
//	MMapBuffer* bitmapIndex;
//	cout << "build hash index for subject" << endl;
//	for (map<ID, ChunkManager*>::iterator iter = bitmap->predicate_managers[0].begin();
//			iter != bitmap->predicate_managers[0].end(); iter++) {
//		if (iter->second != NULL) {
//			iter->second->buildChunkIndex();
//			iter->second->getChunkIndex()->save(bitmapIndex);
//		}
//	}
//
//	cout << "build hash index for object" << endl;
//	for (map<ID, ChunkManager*>::iterator iter = bitmap->predicate_managers[1].begin();
//			iter != bitmap->predicate_managers[1].end(); iter++) {
//		if (iter->second != NULL) {
//			iter->second->buildChunkIndex();
//			iter->second->getChunkIndex()->save(bitmapIndex);
//		}
//	}
//
//	return OK;
//}

void TripleBitBuilder::save_cast(){
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

Status TripleBitBuilder::endBuild() {
	//λͼ����־û�
	bitmap->save();
	save_cast();
	//ͳ����Ϣ�־û�
	ofstream ofile(string(dir + "/statIndex").c_str());
	MMapBuffer* indexBuffer = NULL;
	((OneConstantStatisticsBuffer*)statBuffer[0])->save(indexBuffer);
	((OneConstantStatisticsBuffer*)statBuffer[1])->save(indexBuffer);
	((TwoConstantStatisticsBuffer*)statBuffer[2])->save(indexBuffer);
	((TwoConstantStatisticsBuffer*)statBuffer[3])->save(indexBuffer);
	delete indexBuffer;

	return OK;
}

void TripleBitBuilder::print(int f, TripleBitBuilder*& builder) {
	bitmap->print(f,builder);
}
