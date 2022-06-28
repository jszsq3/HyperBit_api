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

//����
char* DATABASE_PATH;
char* QUERY_PATH;
int gthread;
int cthread;
int tripleSize;
int windowSize;

int main(int argc, char* argv[]) {
	//��ʼʱ��
	timeval start, end;
	gettimeofday(&start, NULL);

	if (argc < 4) {
		fprintf(stderr, "Usage: %s <TripleBit Directory> <Query files Directory>\n", argv[0]);//argv[1] = database/  argv[2] = dataset 
		return -1;
	}

	DATABASE_PATH = argv[1];
	QUERY_PATH = argv[2];
	string dir = DATABASE_PATH;
	GXTripleBitRepository* repo = GXTripleBitRepository::create(argv[1]);

	if (repo == NULL) {
		return -1;
	}

	gthread = atoi(argv[3]);//�����߳���
	cthread = atoi(argv[4]);//��ѯ�߳���
	windowSize = atoi(argv[5]);
	repo->insertData(argv[2]);
	repo->save_cast(dir);
	delete repo;
	repo = NULL;

	//����ʱ��
	gettimeofday(&end, NULL);
	cout << "Insert time: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << "s" << endl;

	return 0;
}
