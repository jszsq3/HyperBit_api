/*
 * TempMMapBuffer.h
 *
 *  Created on: 2014-1-14
 *      Author: root
 */

#ifndef TEMPMMAPBUFFER_H_
#define TEMPMMAPBUFFER_H_

#include "TripleBit.h"

class TempMMapBuffer {
private:
	int fd;//�ļ�ָ��
	char volatile *mmapAddr; //ӳ�䵽�ڴ����ʼ��ַ
	string filename; //�ļ�����
	size_t size;  //�ļ��ܵĴ�С(B)
	size_t usedPage; //�Ѿ�ʹ�õ�ҳ��
	size_t buildUsedpage;
	pthread_mutex_t mutex;//����������

	static TempMMapBuffer *instance;

private:
	TempMMapBuffer(const char *filename, size_t initSize);
	~TempMMapBuffer();
	uchar* resize(size_t incrementSize);
	//Status resize(size_t newSize, bool clear);
	void memset(char value);

public:
	uchar *getBuffer();
	uchar *getBuffer(int pos);
	void discard();
	void close();
	Status flush();
	size_t getSize() {
		return size;
	}
	size_t getLength() {
		return size;
	}
	uchar *getAddress() const {
		return (uchar*) mmapAddr;
	}
	uchar* getPage(size_t &pageNo);

	void setUsedPage(const size_t usedPage){
		this->usedPage = usedPage;
	}
	size_t getUsedPage() {
		return usedPage;
	}

	void setBuildUsedPage(const size_t usedPage) {
		this->buildUsedpage = usedPage;
	}
	size_t getBuildUsedPage() {
		return buildUsedpage;
	}

	void checkSize(size_t needPage);

public:
	static void create(const char *filename, size_t initSize);
	static TempMMapBuffer &getInstance();
	static void deleteInstance();
};

#endif /* TEMPMMAPBUFFER_H_ */
