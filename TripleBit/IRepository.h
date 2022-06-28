/*
 * IRepository.h
 *
 *  Created on: 2010-4-9
 *      Author: wdl
 */

#ifndef IREPOSITORY_H_
#define IREPOSITORY_H_

#include "TripleBit.h"

class GXIRepository {

public:
	GXIRepository() {
	}
	virtual ~GXIRepository() {
	}


	//SO(id,string)transform
	virtual bool find_soid_by_string(SOID& soid, const std::string& str) = 0;
	virtual bool find_soid_by_string_update(SOID& soid, const std::string& str) = 0;
	virtual bool find_string_by_soid(std::string& str, SOID& soid) = 0;

	//P(id,string)transform
	virtual bool find_pid_by_string(PID& pid, const std::string& str) = 0;
	virtual bool find_pid_by_string_update(PID& pid, const std::string& str) = 0;
	virtual bool find_string_by_pid(std::string& str, ID& pid) = 0;


	//Get some statistics information
	virtual int get_predicate_count(PID pid) = 0;
	virtual int get_subject_count(ID subjectID) = 0;
	virtual int get_object_count(double object, char objType = STRING) = 0;
	virtual int get_subject_predicate_count(ID subjectID, ID predicateID) = 0;
	virtual int get_object_predicate_count(double object, ID predicateID, char objType = STRING) = 0;
	virtual int get_subject_object_count(ID subjectID, double object, char objType = STRING) = 0;

	//scan the database;
	virtual Status getSubjectByObjectPredicate(double object, ID pod, char objType = STRING) = 0;
	virtual ID next() = 0;

	//Get the id by string;
	virtual bool lookup(const string& str, ID& id) = 0;
};

class IRepository {

public:
	IRepository() {}
	virtual ~IRepository() {}


	//SO(id,string)transform
	virtual bool find_soid_by_string(SOID& soid, const std::string& str) = 0;
	virtual bool find_soid_by_string_update(SOID& soid, const std::string& str) = 0;
	virtual bool find_string_by_soid(std::string& str, SOID& soid) = 0;

	//P(id,string)transform
	virtual bool find_pid_by_string(PID& pid, const std::string& str) = 0;
	virtual bool find_pid_by_string_update(PID& pid, const std::string& str) = 0;
	virtual bool find_string_by_pid(std::string& str, ID& pid) = 0;


	//Get some statistics information
	virtual int	get_predicate_count(PID pid) = 0;
	virtual int get_subject_count(ID subjectID) = 0;
	virtual int get_object_count(ID objectID) = 0;
	virtual int get_subject_predicate_count(ID subjectID, ID predicateID) = 0;
	virtual int get_object_predicate_count(ID objectID, ID predicateID) = 0;
	virtual int get_subject_object_count(ID subjectID, ID objectID) = 0;

	//scan the database;
	virtual Status getSubjectByObjectPredicate(ID oid, ID pod) = 0;
	virtual ID next() = 0;

	//Get the id by string;
	virtual bool lookup(const string& str, ID& id) = 0;
};

#endif /* IREPOSITORY_H_ */
