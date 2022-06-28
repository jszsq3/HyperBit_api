#ifndef _TRIPLEBIT_H_
#define _TRIPLEBIT_H_

#include <cstring>
#include <iostream>
#include <sstream>
#include <fstream>
#include <map>
#include <vector>
#include <set>
#include <stdlib.h>
#include <stdio.h>
#include <malloc.h>
#include <math.h>
#include <sys/time.h>
#include <stack>
#include <tr1/memory>
#include <float.h>
#include <assert.h>
#include <limits.h>
#include <algorithm>
#include <unistd.h>
using namespace std;

#include "MessageEngine.h"
#include <boost/thread/thread.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>

#include "util/atomic.hpp"
#include <pthread.h>
using namespace boost;

//#define MYDEBUG
//#define RESULT_TIME
//#define PATTERN_TIME
//#define TOTAL_TIME
//#define TTDEBUG
//#define PRINT_BUFFERSIZE
#define LANG 0

template<class T> string toStr(T tmp) {
	stringstream ss;
	ss << tmp;
	return ss.str();
}

const unsigned int THREAD_GROUP_NUMBER = 8;

//bitmap settings
const unsigned int INIT_PAGE_COUNT = 1024;
const unsigned int INCREMENT_PAGE_COUNT = 1024;
const unsigned int CHUNK_SIZE = 16;

//uri settings
const unsigned int URI_INIT_PAGE_COUNT = 256;
const unsigned int URI_INCREMENT_PAGE_COUNT = 256;

//reification settings
const unsigned int REIFICATION_INIT_PAGE_COUNT = 2;
const unsigned int REIFICATION_INCREMENT_PAGE_COUNT = 2;

//column buffer settings
const unsigned int COLUMN_BUFFER_INIT_PAGE_COUNT = 2;
const unsigned int COLUMN_BUFFER_INCREMENT_PAGE_COUNT = 2;

//URI statistics buffer settings
const unsigned int STATISTICS_BUFFER_INIT_PAGE_COUNT = 1;
const unsigned int STATISTICS_BUFFER_INCREMENT_PAGE_COUNT = 1;

//entity buffer settings
const unsigned int ENTITY_BUFFER_INIT_PAGE_COUNT = 1;
const unsigned int ENTITY_BUFFER_INCREMENT_PAGE_COUNT = 2;


//middle result buffer settings
const unsigned int MIDRESULT_BUFFER_INIT_PAGE_COUNT = 1;
const unsigned int MIDRESULT_BUFFER_INCREMENT_PAGE_COUNT = 2;

//temp buffer settings
const unsigned int TEMPMMAPBUFFER_INIT_PAGE = 1000;
const unsigned int TEMPBUFFER_INIT_PAGE_COUNT = 1;
const unsigned int TEMPMMAP_INIT_PAGE_COUNT = 1;
const unsigned int INCREMENT_TEMPMMAP_PAGE_COUNT = 1;

//complex buffer settings
const unsigned int COMPLEX_BUFFER_INIT_PAGE_COUNT = 1;
const unsigned int COMPLEX_BUFFER_INCREMENT_PAGE_COUNT = 1;
const unsigned int NO_TEMPBUFFER_PAGE = UINT_MAX;

//hash index
const unsigned int HASH_RANGE = 200;
const unsigned int HASH_CAPACITY = 100000 / HASH_RANGE;
const unsigned int HASH_CAPACITY_INCREASE = 100000 / HASH_RANGE;
const unsigned int SECONDARY_HASH_RANGE = 10;
const unsigned int SECONDARY_HASH_CAPACITY = 100000 / SECONDARY_HASH_RANGE;
const unsigned int SECONDARY_HASH_CAPACITY_INCREASE = 100000 / SECONDARY_HASH_RANGE;

//num
const unsigned int ONE = 1;
const unsigned int TWO = 2;
const unsigned int THREE = 3;

extern char* DATABASE_PATH;
extern char* QUERY_PATH;
extern int cthread;
extern int gthread;
extern int tripleSize;
extern int windowSize;

//thread pool
const unsigned int WORKERNUM = 1;
const unsigned int WORK_THREAD_NUMBER = 1;
const unsigned int PARTITION_THREAD_NUMBER = 1;
const unsigned int CHUNK_THREAD_NUMBER = 1;
const unsigned int SCAN_JOIN_THREAD_NUMBER = 1;
const double ELAPSED = 0.00000001;

enum Status {
	OK = 1, 
	NOT_FIND = -1, 
	OUT_OF_MEMORY = -5, 
	PTR_IS_FULL = -11, 
	PTR_IS_NOT_FULL = -10, 
	CHUNK_IS_FULL = -21, 
	CHUNK_IS_NOT_FULL = -20, 
	PREDICATE_NOT_BE_FINDED = -30, 
	CHARBUFFER_IS_FULL = -40, 
	CHARBUFFER_IS_NOT_FULL = -41, 
	URI_NOT_FOUND = -50, 
	URI_FOUND = -51, 
	PREDICATE_FILE_NOT_FOUND = -60,
	PREDICATE_FILE_END = -61, 
	PREDICATE_NOT_FOUND = -70, 
	PREDICATE_FOUND = -71, 
	REIFICATION_NOT_FOUND, 
	FINISH_WIRITE, 
	FINISH_READ, 
	ERROR, 
	SUBJECTID_NOT_FOUND, 
	OBJECTID_NOT_FOUND, 
	COLUMNNO_NOT_FOUND,
	BUFFER_NOT_FOUND, 
	ENTITY_NOT_INCLUDED, 
	NO_HIT,
	NOT_OPENED, 	// file was not previously opened
	END_OF_FILE, 	// read beyond end of file or no space to extend file
	LOCK_ERROR, 	// file is used by another program
	NO_MEMORY,
	URID_NOT_FOUND,
	ALREADY_EXISTS,
	NOT_FOUND,
	CREATE_FAILURE,
	NOT_SUPPORT,
	ID_NOT_MATCH,
	ERROR_UNKOWN,
	BUFFER_MODIFIED,
	NULL_RESULT,
	TOO_MUCH,
	ERR,
	DATA_EXSIT,
	DATA_DELETE,
	DATA_NONE,
};

//join shape of patterns within a join variable.
enum JoinShape {
	STAR, 
	CHAIN
};

enum OrderByType {
	ORDERBYS, ORDERBYO
};

enum DataType {
	NONE, BOOL, BOOL_DELETE, CHAR, CHAR_DELETE, INT, INT_DELETE, UNSIGNED_INT, UNSIGNED_INT_DELETE, FLOAT, FLOAT_DELETE, DATE, TIME, DATE_DELETE, LONGLONG, LONGLONG_DELETE, DOUBLE, DOUBLE_DELETE, STRING, STRING_DELETE
};

enum StatisticsType {
	SUBJECTPREDICATE_STATIS, OBJECTPREDICATE_STATIS
};

enum NODEEDGETYPE {
	STARTEDGE, TARGETEDGE
};

enum EntityType {
	PREDICATE = 1 << 0, 
	SUBJECT = 1 << 1, 
	OBJECT = 1 << 2, 
	DEFAULT = -1
};

typedef long long int64;
typedef unsigned char word;
typedef word* word_prt;
typedef word_prt bitVector_ptr;
typedef unsigned int ID;
typedef unsigned int TripleNodeID;
typedef bool SOType;
typedef unsigned int SOID;
typedef unsigned int PID;
typedef bool status;
typedef short COMPRESS_UNIT;
typedef unsigned int uint;
typedef unsigned char uchar;
typedef unsigned short ushort;
typedef unsigned long long ulonglong;
typedef long long longlong;
typedef size_t OffsetType;
typedef size_t HashCodeType;

const ID ID_MIN = 1;
const ID ID_MAX = UINT_MAX;

extern const ID INVALID_ID;

#define BITVECTOR_INITIAL_SIZE 100
#define BITVECTOR_INCREASE_SIZE 100

#define BITMAP_INITIAL_SIZE 100
#define BITMAP_INCREASE_SIZE 30

#define BUFFER_SIZE 1024
//#define DEBUG 1
#ifndef NULL
#define NULL 0
#endif

//something about shared memory
#define MAXTRANSNUM 1023
#define MAXTASKNUMWP 100
#define MAXRESULTNUM 10
#define MAXCHUNKTASK 50

//#define PRINT_RESULT 1
//#define TEST_TIME 1
#define WORD_SIZE (sizeof(word))

inline uchar Length_2_Type(uchar xLen, uchar yLen) {
	return (xLen - 1) * 4 + yLen;
}

//
inline uchar Type_2_Length(uchar type) {
	return (type - 1) / 4 + (type - 1) % 4 + 2;
}

inline void Type_2_Length(uchar type, uchar& xLen, uchar& yLen) {
	xLen = (type - 1) / 4 + 1;
	yLen = (type - 1) % 4 + 1;
}

struct LengthString {
	const char * str;
	uint length;
	void dump(FILE * file) {
		for (uint i = 0; i < length; i++)
			fputc(str[i], file);
	}
	LengthString() :
			str(NULL), length(0) {
	}
	LengthString(const char * str) {
		this->str = str;
		this->length = strlen(str);
	}
	LengthString(const char * str, uint length) {
		this->str = str;
		this->length = length;
	}
	LengthString(const std::string & rhs) :
			str(rhs.c_str()), length(rhs.length()) {
	}
	bool equals(LengthString * rhs) {
		if (length != rhs->length)
			return false;
		for (uint i = 0; i < length; i++)
			if (str[i] != rhs->str[i])
				return false;
		return true;
	}
	bool equals(const char * str, uint length) {
		if (this->length != length)
			return false;
		for (uint i = 0; i < length; i++)
			if (this->str[i] != str[i])
				return false;
		return true;
	}
	bool equals(const char * str) {
		if (length != strlen(str))
			return false;
		for (uint i = 0; i < length; i++)
			if (this->str[i] != str[i])
				return false;
		return str[length] == 0;
	}
	bool copyTo(char * buff, uint bufflen) {
		if (length < bufflen) {
			for (uint i = 0; i < length; i++)
				buff[i] = str[i];
			buff[length] = 0;
			return true;
		}
		return false;
	}
};

//���µ�tripleNode
struct GXTripleNode {
	ID subjectID, predicateID, objectID,timeID;
	ID chunkIDOfS;
	ID chunkIDOfO;
	// Which of the three values are constants?
	bool constSubject, constPredicate, constObject;
	enum Op {
		FINDSBYPO, 
		FINDOBYSP, 
		FINDPBYSO, 
		FINDSBYP, 
		FINDOBYP, 
		FINDPBYS, 
		FINDPBYO, 
		FINDSBYO, 
		FINDOBYS, 
		FINDS, 
		FINDP, 
		FINDO, 
		NOOP, 
		FINDSPBYO, 
		FINDSOBYP, 
		FINDPOBYS, 
		FINDPSBYO, 
		FINDOSBYP, 
		FINDOPBYS, 
		FINDSOBYNONE, 
		FINDOSBYNONE, 
		FINDSPO, 
		FINDSPBYNONE, 
		FINDPOBYNONE
	};

	TripleNodeID tripleNodeID;
	/// define the first scan operator
	Op scanOperation;
	int selectivity;

	// Is there an implicit join edge to another GXTripleNode?
	GXTripleNode() {
		subjectID = 0;
		predicateID = 0;
		objectID = 0;
		timeID = 0;
		constSubject = 0;
		constPredicate = 0;
		constObject = 0;
		tripleNodeID = 0;
		scanOperation = GXTripleNode::FINDP;
		selectivity = -1;
	}
	GXTripleNode(const GXTripleNode &orig) {
		subjectID = orig.subjectID;
		predicateID = orig.predicateID;
		objectID = orig.objectID;
		timeID = orig.timeID;

		constSubject = orig.constSubject;
		constPredicate = orig.constPredicate;
		constObject = orig.constObject;

		tripleNodeID = orig.tripleNodeID;

		scanOperation = orig.scanOperation;

		selectivity = orig.selectivity;

	}
	GXTripleNode& operator=(const GXTripleNode& orig) {
		subjectID = orig.subjectID;
		predicateID = orig.predicateID;
		objectID = orig.objectID;
		timeID = orig.timeID;

		constSubject = orig.constSubject;
		constPredicate = orig.constPredicate;
		constObject = orig.constObject;

		tripleNodeID = orig.tripleNodeID;

		scanOperation = orig.scanOperation;

		selectivity = orig.selectivity;

		return *this;
	}

	void print() {
		cout << "subjectID: " << subjectID << "\t predicateID: " << predicateID << "\t object: " << objectID << endl;
		cout << "constSubject: " << constSubject << "\t constPredicate: " << constPredicate << "\t constObject: " << constObject << endl;
		cout << "tripleNodeID: " << tripleNodeID << "\t scanOperation: " << scanOperation << "\t selectivity: " << selectivity << endl;
	}
};

//��ѯ��tripleNode
struct CXTripleNode {
	ID subject, predicate, object;
	// Which of the three values are constants?
	bool constSubject, constPredicate, constObject;
	enum Op {
		FINDSBYPO,
		FINDOBYSP,
		FINDPBYSO,
		FINDSBYP,
		FINDOBYP,
		FINDPBYS,
		FINDPBYO,
		FINDSBYO,
		FINDOBYS,
		FINDS,
		FINDP,
		FINDO,
		NOOP,
		FINDSPBYO,
		FINDSOBYP,
		FINDPOBYS,
		FINDPSBYO,
		FINDOSBYP,
		FINDOPBYS,
		FINDSOBYNONE,
		FINDOSBYNONE
	};

	TripleNodeID tripleNodeID;
	/// define the first scan operator
	Op scanOperation;
	int selectivity;
	vector<pair<int, int> > varLevel;
	// Is there an implicit join edge to another CXTripleNode?
	CXTripleNode() {
		subject = 0;
		predicate = 0;
		object = 0;
		constSubject = 0;
		constPredicate = 0;
		constObject = 0;
		tripleNodeID = 0;
		scanOperation = CXTripleNode::FINDP;
		selectivity = -1;
	}
	CXTripleNode(const CXTripleNode& orig)
	{
		subject = orig.subject;
		predicate = orig.predicate;
		object = orig.object;
		constSubject = orig.constSubject;
		constPredicate = orig.constPredicate;
		constObject = orig.constObject;
		tripleNodeID = orig.tripleNodeID;
		scanOperation = orig.scanOperation;
		selectivity = orig.selectivity;
		varLevel = orig.varLevel;
	}
	CXTripleNode& operator=(const CXTripleNode& orig)
	{
		subject = orig.subject;
		predicate = orig.predicate;
		object = orig.object;
		constSubject = orig.constSubject;
		constPredicate = orig.constPredicate;
		constObject = orig.constObject;
		tripleNodeID = orig.tripleNodeID;
		scanOperation = orig.scanOperation;
		selectivity = orig.selectivity;
		varLevel = orig.varLevel;
		return *this;
	}

	bool operator<(const CXTripleNode& other) const
	{
		return selectivity < other.selectivity;
	}

	bool static idSort(const CXTripleNode& a, const CXTripleNode& b) {
		return a.tripleNodeID < b.tripleNodeID;
	}
};

//���µ�һ����Ԫ��
struct GXChunkTriple {
	ID subjectID;
	ID predicateID;
	ID objectID;
	ID timeID;
	GXTripleNode::Op operation;
	GXChunkTriple(){}
	GXChunkTriple(ID subjectID, ID predicateID, ID objectID,ID timeID, GXTripleNode::Op operation):
		subjectID(subjectID), predicateID(predicateID), objectID(objectID),timeID(timeID), operation(operation){}
	GXChunkTriple(ID subjectID, ID objectID, ID timeID,GXTripleNode::Op operation):
			subjectID(subjectID), objectID(objectID), timeID(timeID),operation(operation){}

};

inline unsigned long long getTicks() {
	timeval t;
	gettimeofday(&t, 0);
	return static_cast<unsigned long long>(t.tv_sec) * 1000 + (t.tv_usec / 1000);
}

inline string getDataType(char dataType) {
	switch (dataType) {
	/*case NONE:
		return "NONE";
	case BOOL:
		return "BOOL";
	case BOOL_DELETE:
		return "BOOL_DELETE";*/
	case CHAR:
		return "CHAR";
	/*case CHAR_DELETE:
		return "CHAR_DELETE";
	case INT:
		return "INT";
	case INT_DELETE:
		return "INT_DELETE";
	case UNSIGNED_INT:
		return "UNSIGNED_INT";
	case UNSIGNED_INT_DELETE:
		return "UNSIGNED_INT_DELETE";
	case FLOAT:
		return "FLOAT";
	case FLOAT_DELETE:
		return "FLOAT_DELETE";
	case DATE:
		return "DATE";
	case DATE_DELETE:
		return "DATE_DELETE";
	case LONGLONG:
		return "LONGLONG";
	case LONGLONG_DELETE:
		return "LONGLONG_DELETE";
	case DOUBLE:
		return "DOUBLE";
	case DOUBLE_DELETE:
		return "DOUBLE_DELETE";*/
	case STRING:
		return "STRING";
	/*case STRING_DELETE:
		return "STRING_DELETE";*/
	default:
		return "NONE";
	}
}


inline int compare_double(double x, double y) {
	if (abs(x - y) <= ELAPSED) {
		return 0;
	} else if (x - y < -ELAPSED) {
		return -1;
	} else {
		return 1;
	}
}

#endif // _TRIPLEBIT_H_
