#include <cstdlib>
#include <cstdio>
#include <thread>
#include "lockfreelist/lockfreelist.h"
#include "boostinglist/boostinglist.h"
#include "durabletxn/dtx.h"
using namespace std;

const int THREAD_COUNT = 2;

static const int NUM_OF_TRANSACTIONS = 10;
static const int TRANSACTION_SIZE = 2;



void runThread(LockfreeList* list, int threadId)
{
    list->Insert(threadId);
}

// void runBoostingThread(BoostingList* list, int threadId)
// {
//     list->Init();

//     BoostingList::ReturnCode ret;

//     for(int i = 0; i < 2; i++)
//     {
//         ret = list->Insert(threadId+i);
//         if(ret != BoostingList::OK)
//         {
//             list->OnAbort();
//             return;
//         }
//     }

//     list->OnCommit();

// }

void runDurableBoostingThread(BoostingList* list, int threadId, Allocator<LockfreeList::Node>* m_nodeAllocator)
{
    m_nodeAllocator->Init();
    list->Init(m_nodeAllocator);
    DTX::INIT(); // should be run once per thread

    BoostingList::ReturnCode ret;

    for(int t = 0; t < NUM_OF_TRANSACTIONS; t++) {
        DTX::TX_BEGIN();

        for(int i = 0; i < TRANSACTION_SIZE; i++)
        {
            ret = list->Insert(threadId+(i + 1) + (t+1) * 10);
            if(ret != BoostingList::OK)
            {
                list->OnAbort();
                break;
            }
        }
        if(ret == BoostingList::OK) {
            DTX::TX_COMMIT();
            list->OnCommit();            
        }


    }

}

int main()
{
    // LockfreeList* lockfreelist = new LockfreeList();
    Allocator<LockfreeList::Node> m_nodeAllocator(NUM_OF_TRANSACTIONS * THREAD_COUNT * sizeof(LockfreeList::Node) * TRANSACTION_SIZE, THREAD_COUNT, sizeof(LockfreeList::Node));


	thread threads[THREAD_COUNT]; // Create our threads
	// for(long i=0; i<THREAD_COUNT; i++) { // Start our threads
	// 	threads[i] = thread(runThread, lockfreelist, i);
	// }
	// for(int i=0; i<THREAD_COUNT; i++) { // Wait for all threads to complete
	// 	threads[i].join();
	// }

 //    lockfreelist->Print();


    BoostingList* boostinglist = new BoostingList();

	for(long i=0; i<THREAD_COUNT; i++) { // Start our threads
		// threads[i] = thread(runBoostingThread, boostinglist, i);
        threads[i] = thread(runDurableBoostingThread, boostinglist, i, &m_nodeAllocator);
        
	}
	for(int i=0; i<THREAD_COUNT; i++) { // Wait for all threads to complete
		threads[i].join();
	}

    boostinglist->Print();
}