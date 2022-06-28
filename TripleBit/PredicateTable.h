/*
 * PredicateTable.h
 *
 *  Created on: Mar 11, 2010
 *      Author: root
 */

#ifndef PREDICATETABLE_H_
#define PREDICATETABLE_H_

#include "StringIDSegment.h"
#include "TripleBit.h"
#include "util/TripleBitLock.h"

class PredicateTable {
public:
	StringIDSegment* prefix_segment;
	StringIDSegment* suffix_segment;

	string SINGLE;

	TripleBitMutexLock lock;

	int predicatenum;
	map<ID,string> idtostring;
	map<string,ID> stringtoid;
	map<ID,int> isused;

private:
	Status getPrefix(const char* URI,LengthString & prefix, LengthString & suffix);
public:
	PredicateTable() :
			SINGLE("single") {
	}
	PredicateTable(const string dir);
	virtual ~PredicateTable();
	void load_predicate_cast(const string dir);
	void load_cast(const string dir);
	Status insertTable(const char* str, ID& id);
	Status insertTable_init(const char* str, ID& id);
	Status insertTable_sync(const char* URI, ID& id);
	Status getPredicateByID(string& URI, ID id);
	Status getPredicateByID_init(string& URI, ID id);
	Status getIDByPredicate(const char* str, ID& id);
	Status getIDByPredicate_init(const char* str, ID& id);
	size_t getSize() {
		return prefix_segment->getSize() + suffix_segment->getSize();
	}

	size_t getPredicateNo();

	void dump();
public:
	static PredicateTable* load(const string dir);
};

#endif /* PREDICATETABLE_H_ */
