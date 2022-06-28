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
//#include "tripleBitQueryAPI.h"
#include <vector>

//define 
char* DATABASE_PATH;
char* QUERY_PATH;
int gthread, cthread, tripleSize, windowSize;
// extern int refile_mutex;
int main(int argc, char* argv[]) {
	
	if(argc == 2){
		cout<<"全部导出"<<endl;

		DATABASE_PATH = argv[1];
		gthread = 1;
		cthread = 1;
		tripleSize = 0;
		windowSize = 1;

		
		//开始时间
		timeval start, end;
		gettimeofday(&start, NULL);
		CXTripleBitRepository* repo = CXTripleBitRepository::create(argv[1]);
		//结束时间
		gettimeofday(&end, NULL);
		cout << "Load time: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << "s" << endl;

		if (repo == NULL) {
			return -1;
		}

	

		// if (h1.length() == 0)
		// 	continue;
		std::remove(string("tripleedge.txt").c_str());
		std::remove(string("hyperedge.txt").c_str());
		repo->print(2,repo);
		// repo->enqueue(h2);
		// repo->cmd_line_sm(stdin, stdout, "sparql/");

		// repo->enqueue(exit);

	}else{
		if (argc < 3) {
			fprintf(stderr, "Usage: %s <TripleBit Directory> <Query files Directory>\n", argv[0]);
			return -1;
		}

		DATABASE_PATH = argv[1];
		QUERY_PATH = argv[2];
		gthread = 1;
		cthread = 1;
		tripleSize = 0;
		windowSize = 1;

		
		//开始时间
		timeval start, end;
		gettimeofday(&start, NULL);
		CXTripleBitRepository* repo = CXTripleBitRepository::create(argv[1]);
		//结束时间
		gettimeofday(&end, NULL);
		cout << "Load time: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << "s" << endl;

		if (repo == NULL) {
			return -1;
		}


		//普通图查询
		if (argc == 3) {
			repo->cmd_line_sm(stdin, stdout, argv[2]);
			// repo->cmd_line(argv[2]);
			// while(1){

			// }
		}
		else if(argc ==4 ){
			//test
			char* re = repo->query_simple(argv[2]);
			cout<<re<<endl;
		}
		else {
			cout << "not assist" << endl;
		}
	}
	

	return 0;
}
