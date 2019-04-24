#include <cstdlib>
#include <cstdio>
#include <thread>
#include "lockfreelist.h"
using namespace std;

const int THREAD_COUNT = 2;

void runThread(LockfreeList* list, int threadId)
{
    list->Insert(threadId);
}

int main()
{
    LockfreeList* list = new LockfreeList();

	thread threads[THREAD_COUNT]; // Create our threads
	for(long i=0; i<THREAD_COUNT; i++) { // Start our threads
		threads[i] = thread(runThread, list, i);
	}
	for(int i=0; i<THREAD_COUNT; i++) { // Wait for all threads to complete
		threads[i].join();
	}

    list->Print();
}