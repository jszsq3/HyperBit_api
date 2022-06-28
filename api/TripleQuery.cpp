/*
 * TripleBitQuery.cpp
 *
 *  Created on: Apr 12, 2011
 *      Author: root
 */

#include "../TripleBit/TripleBit.h"
#include "../TripleBit/GXTripleBitRepository.h"
#include "../TripleBit/OSFile.h"
#include "../TripleBit/MMapBuffer.h"
#include "../TripleBit/GXTripleBitWorkerQuery.h"
//#include "tripleBitQueryAPI.h"
#include <vector>

//define 

char* DATABASE_PATH;
char* QUERY_PATH;
int gthread, cthread, tripleSize, windowSize;
CXTripleBitRepository* repo;
extern int refile_mutex;
extern "C"
{
	char* search(char* database,char* querypath){

		DATABASE_PATH = database;
		QUERY_PATH = querypath;
		gthread = 1;
		cthread = 1;
		tripleSize = 0;
		windowSize = 1;
		
		repo = CXTripleBitRepository::create(database);

// 		ifstream in;
// 		string querystr;
// 		string dataall = "";
// 		string subject,predicate,object;
		
// // cout<<database<<endl;
// // cout<<querypath<<endl;
// 		repo->cmd_line(querypath);
// 		while(refile_mutex==0){
// 			sleep(0.01);
// 		}
// 		// while(access("results",0)!=0){
// 		// 	// cout<<"文件不存在"<<endl;
// 		// }
// 		// sleep(1);
// 		cout<<refile_mutex<<endl;
// 		in.open("results",ios::out);
// 		while(in>>subject>>predicate>>object){
// 			dataall = dataall+subject+"-?-"+predicate+"-?-"+object+"-?-"+"no-?-";
// 		}
// 		char* re = new char[strlen(dataall.c_str())+1];
// 		// string re;
// 		// re = dataall;
// 		strcpy(re,dataall.c_str());
// 		// cout<<re<<endl;
// 		std::remove("results");
// 		refile_mutex=0;
		char *re = repo->query_simple(querypath);
		return re;
	}

	char* search_hyper(char* database,char* querypath){

		DATABASE_PATH = database;
		QUERY_PATH = querypath;
		gthread = 1;
		cthread = 1;
		tripleSize = 0;
		windowSize = 1;
		
		repo = CXTripleBitRepository::create(database);

		ifstream in;
		string querystr;
		string dataall = "";
		string hid,subject,predicate,object,stime,etime;
		
// cout<<database<<endl;
// cout<<querypath<<endl;
		repo->cmd_line(querypath);
		while(refile_mutex==0){
			sleep(0.01);
		}
		// while(access("results",0)!=0){
		// 	// cout<<"文件不存在"<<endl;
		// }
		// sleep(1);
		cout<<refile_mutex<<endl;
		if(access("results",0)!=0){
			cout<<"====================="<<endl;
			return ;
		}
		in.open("results",ios::out);
		while(in>>hid>>subject>>predicate>>object>>stime>>etime){
			dataall = dataall+hid+"-?-"+subject+"-?-"+predicate+"-?-"+object+"-?-"+stime+"-?-"+etime+"-?-";
		}
		char* re = new char[strlen(dataall.c_str())+1];
		// string re;
		// re = dataall;
		strcpy(re,dataall.c_str());
		// cout<<re<<endl;
		std::remove("results");
		refile_mutex=0;
		return re;
	}

	int insert(char* database,char* insertpath){

		DATABASE_PATH = database;
		QUERY_PATH = insertpath;
		gthread = 1;
		cthread = 1;
		tripleSize = 0;
		windowSize = 1;
		string dir = DATABASE_PATH;
		
		GXTripleBitRepository* repo = GXTripleBitRepository::create(database);
		repo->insertData(insertpath);
		repo->save_cast(dir);
		cout<<"插入成功"<<endl;
		delete repo;
		std::remove(insertpath);
		return 0;
	}
}
// int main(int argc, char* argv[]) {
	
// 	if (argc < 3) {
// 		fprintf(stderr, "Usage: %s <TripleBit Directory> <Query files Directory>\n", argv[0]);
// 		return -1;
// 	}

// 	DATABASE_PATH = argv[1];
// 	QUERY_PATH = argv[2];
// 	gthread = 1;
// 	cthread = 1;
// 	tripleSize = 0;
// 	windowSize = 1;

	
// 	//开始时间
// 	// timeval start, end;
// 	// gettimeofday(&start, NULL);
// 	CXTripleBitRepository* repo = CXTripleBitRepository::create(argv[1]);
// 	//结束时间
// 	// gettimeofday(&end, NULL);
// 	// cout << "Load time: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << "s" << endl;

// 	if (repo == NULL) {
// 		return -1;
// 	}


// 	//普通图查询
// 	if (argc == 3) {
// 		repo->cmd_line(argv[2]);
// 	}
// 	else {
// 		cout << "not assist" << endl;
// 	}

	

// 	return 0;
// }
