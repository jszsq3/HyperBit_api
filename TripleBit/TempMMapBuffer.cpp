/*
 * TempMMapBuffer.cpp
 *
 *  Created on: 2014-1-14
 *      Author: root
 */

#include "TempMMapBuffer.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <stdio.h>
#include "MemoryBuffer.h"

TempMMapBuffer *TempMMapBuffer::instance = NULL;

TempMMapBuffer::TempMMapBuffer(const char *_filename, size_t initSize) : filename(_filename) {
	fd = open(filename.c_str(), O_CREAT | O_RDWR, 0666);
	if (fd < 0) {
		perror(_filename);
		MessageEngine::showMessage("Create tempMap file error", MessageEngine::ERROR);
	}

	size = lseek(fd, 0, SEEK_END);
	if (size < initSize) {
		size = initSize;
		if (ftruncate(fd, initSize) != 0) {
			perror(_filename);
			MessageEngine::showMessage("ftruncate file error", MessageEngine::ERROR);
		}
	}
	if (lseek(fd, 0, SEEK_SET) != 0) {
		perror(_filename);
		MessageEngine::showMessage("lseek file error", MessageEngine::ERROR);
	}

	mmapAddr = (char volatile*) mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (mmapAddr == (char volatile *) MAP_FAILED) {
		perror(_filename);
		cout << "size: " << size << endl;
		MessageEngine::showMessage("map file to memory error", MessageEngine::ERROR);
	}

	//usedPage = 0;
	pthread_mutex_init(&mutex, NULL);
}

TempMMapBuffer::~TempMMapBuffer() {
	pthread_mutex_destroy(&mutex);
}

Status TempMMapBuffer::flush() {
	if (msync((char*) mmapAddr, size, MS_SYNC) == 0) {
		return OK;
	}
	return ERROR;
}
//增加文件大小
uchar* TempMMapBuffer::resize(size_t incrementSize) {
	size_t newSize = size + incrementSize;
	uchar *newAddr = NULL;
	if (munmap((char*) mmapAddr, size) != 0) {
		MessageEngine::showMessage("resize-munmap error!", MessageEngine::ERROR);
	}
	if (ftruncate(fd, newSize) != 0) {
		MessageEngine::showMessage("resize-ftruncate file error!", MessageEngine::ERROR);
	}
	if ((newAddr = (uchar*) mmap(NULL, newSize, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0)) == (uchar*) MAP_FAILED) {
		MessageEngine::showMessage("mmap buffer resize error!", MessageEngine::ERROR);
	}
	mmapAddr = (char volatile*) newAddr;
	::memset((uchar*) mmapAddr + size, 0, incrementSize);
	size = newSize;
	return (uchar*)mmapAddr;
}

void TempMMapBuffer::discard() {
	munmap((char*) mmapAddr, size);
	::close(fd);
	unlink(filename.c_str());
}

void TempMMapBuffer::close() {
	flush();
	munmap((char*) mmapAddr, size);
	::close(fd);
}

uchar *TempMMapBuffer::getBuffer() {
	return (uchar*) mmapAddr;
}

uchar *TempMMapBuffer::getBuffer(int pos) {
	return (uchar*) mmapAddr + pos;
}

/*
Status TempMMapBuffer::resize(size_t newSize, bool clear) {
	uchar *newAddr = NULL;
	if (munmap((char*) mmapAddr, size) != 0 || ftruncate(fd, newSize) != 0 || (newAddr = (uchar*) mmap(NULL, newSize, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0)) == (uchar*) MAP_FAILED) {
		MessageEngine::showMessage("mmap buffer resize error!", MessageEngine::ERROR);
		return ERROR;
	}

	mmapAddr = (char volatile*) newAddr;

	::memset((uchar*) mmapAddr + size, 0, newSize - size);
	size = newSize;
	return OK;
}
*/

void TempMMapBuffer::memset(char value) {
	::memset((char*) mmapAddr, value, size);
}

void TempMMapBuffer::create(const char *filename, size_t initSize = TEMPMMAPBUFFER_INIT_PAGE * MemoryBuffer::pagesize) {
	instance = new TempMMapBuffer(filename, initSize);
}

TempMMapBuffer &TempMMapBuffer::getInstance() {
	if (instance == NULL) {
		perror("instance must not be NULL");
	}
	return *instance;
}

void TempMMapBuffer::deleteInstance() {
	if (instance != NULL) {
		//instance->discard();
		instance->close();
		delete instance;
		instance = NULL;
	}
}

uchar* TempMMapBuffer::getPage(size_t &pageNo) {
	pthread_mutex_lock(&mutex);
	uchar* rt;
	//判断是否需要增加文件大小
	if (usedPage * MemoryBuffer::pagesize >= size) {
		pthread_mutex_unlock(&mutex);
		cout << "TempMMapBuffer::getPage error" << endl;
	}
	rt = getAddress() + usedPage * MemoryBuffer::pagesize;
	pageNo = usedPage;
	usedPage++;
	pthread_mutex_unlock(&mutex);
	return rt;
}


void TempMMapBuffer::checkSize(size_t needPage){
	if ((needPage + usedPage) * MemoryBuffer::pagesize > size){
		resize(needPage * MemoryBuffer::pagesize);
	}
}

