
#include "common/allocator.h"
#include <cstdlib>
#include <cstdio>
#include <thread>
#include "lockfreelist/lockfreelist.h"
#include "boostinglist/boostinglist.h"
#include "lftt/translist.h"
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

void runDurableLFTTThread(TransList* list, int threadId, Allocator<TransList::Node>* m_nodeAllocator, Allocator<TransList::Desc>* m_descAllocator, Allocator<TransList::NodeDesc>* m_nodeDescAllocator) {
    
    m_nodeAllocator->Init();
    m_descAllocator->Init();
    m_nodeDescAllocator->Init();


    for(int t = 0; t < NUM_OF_TRANSACTIONS; t++) {

        TransList::Desc* desc = m_descAllocator->Alloc();
        desc->size = TRANSACTION_SIZE;
        desc->status = TransList::ACTIVE;

        for(uint32_t i = 0; i < TRANSACTION_SIZE; ++i)
        {
            desc->ops[i].type = TransList::OpType::INSERT; 
            desc->ops[i].key = (threadId + 1) * 10 + (i + 1) + (t+1) * 100; 
        }

        bool ret = list->ExecuteOps(desc);
        if(!ret)
            printf("transaction %d was failed!\n\r", t);
    }

}

void test_lockfreelist() {
    thread threads[THREAD_COUNT]; // Create our threads
    Allocator<LockfreeList::Node> m_nodeAllocator(NUM_OF_TRANSACTIONS * THREAD_COUNT * sizeof(LockfreeList::Node) * TRANSACTION_SIZE, THREAD_COUNT, sizeof(LockfreeList::Node));
    LockfreeList* lockfreelist = new LockfreeList(&m_nodeAllocator);
	for(long i=0; i<THREAD_COUNT; i++) { // Start our threads
		threads[i] = thread(runThread, lockfreelist, i);
	}
	for(int i=0; i<THREAD_COUNT; i++) { // Wait for all threads to complete
		threads[i].join();
	}

    lockfreelist->Print();    
}

void test_boosting() {
    thread threads[THREAD_COUNT];
    Allocator<LockfreeList::Node> m_nodeAllocator(NUM_OF_TRANSACTIONS * THREAD_COUNT * sizeof(LockfreeList::Node) * TRANSACTION_SIZE, THREAD_COUNT, sizeof(LockfreeList::Node));
    BoostingList* boostinglist = new BoostingList();

	for(long i=0; i<THREAD_COUNT; i++) { // Start our threads
		threads[i] = thread(runDurableBoostingThread, boostinglist, i, &m_nodeAllocator);
        
	}
	for(int i=0; i<THREAD_COUNT; i++) { // Wait for all threads to complete
		threads[i].join();
	}

    boostinglist->Print();

}

void test_lftt() {
    thread threads[THREAD_COUNT]; // Create our threads

    Allocator<TransList::Node> m_nodeAllocator("m_nodeAllocator6", NUM_OF_TRANSACTIONS * THREAD_COUNT * sizeof(TransList::Node) * TRANSACTION_SIZE, THREAD_COUNT, sizeof(TransList::Node));
    Allocator<TransList::Desc> m_descAllocator("m_descAllocator6", NUM_OF_TRANSACTIONS * THREAD_COUNT * TransList::Desc::SizeOf(TRANSACTION_SIZE), THREAD_COUNT, TransList::Desc::SizeOf(TRANSACTION_SIZE));
    Allocator<TransList::NodeDesc> m_nodeDescAllocator("m_nodeDescAllocator6",NUM_OF_TRANSACTIONS * THREAD_COUNT * sizeof(TransList::NodeDesc) * TRANSACTION_SIZE, THREAD_COUNT, sizeof(TransList::NodeDesc));

    TransList* transList = new TransList(&m_nodeAllocator, &m_descAllocator, &m_nodeDescAllocator);

	for(long i=0; i<THREAD_COUNT; i++) { // Start our threads
        threads[i] = thread(runDurableLFTTThread, transList, i, &m_nodeAllocator, &m_descAllocator, &m_nodeDescAllocator);
        
	}
	for(int i=0; i<THREAD_COUNT; i++) { // Wait for all threads to complete
		threads[i].join();
	}

    transList->Print();    

}

int main()
{
    
    test_lftt();   


}