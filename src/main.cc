
#include "common/allocator.h"
#include <cstdlib>
#include <cstdio>
#include <thread>

#include "lockfreelist/lockfreelist.h"
#include "boostinglist/boostinglist.h"
#include "lftt/translist.h"
#include "durabletxn/dtx.h"
#include "logging.h"
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

        DTX::PERSIST(desc);

        for(uint32_t i = 0; i < TRANSACTION_SIZE; ++i)
        {
            desc->ops[i].type = TransList::OpType::INSERT; 
            desc->ops[i].key = (threadId + 1) * 10 + (i + 1) + (t+1) * 100; 
            DTX::PERSIST(&(desc->ops[i]));
        }

        bool ret = list->ExecuteOps(desc);
        if(!ret)
            logger.pmem_durableds_elog("transaction ", t , " was failed!");
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



static const char* NODE_ALLOCATOR_NAME = "m_nodeAllocator16";
static const char* DESC_ALLOCATOR_NAME = "m_descAllocator16";
static const char* NODE_DESC_ALLOCATOR_NAME = "m_nodeDescAllocator16";

static const char* NODE_ALLOCATOR_ALLOCATOR_NAME = "m_nodeAllocatorAllocator16";
static const char* DESC_ALLOCATOR_ALLOCATOR_NAME = "m_descAllocatorAllocator16";
static const char* NODE_DESC_ALLOCATOR_ALLOCATOR_NAME = "m_nodeDescAllocatorAllocator16";

void test_lftt() {
    thread threads[THREAD_COUNT]; // Create our threads


    Allocator<Allocator<TransList::Node>> m_nodeAllocatorAllocator(NODE_ALLOCATOR_ALLOCATOR_NAME, sizeof(Allocator<Allocator<TransList::Node>>), 1, sizeof(Allocator<Allocator<TransList::Node>>));
    Allocator<Allocator<TransList::Desc>> m_descAllocatorAllocator(DESC_ALLOCATOR_ALLOCATOR_NAME, sizeof(Allocator<Allocator<TransList::Desc>>), 1, sizeof(Allocator<Allocator<TransList::Desc>>));
    Allocator<Allocator<TransList::NodeDesc>> m_nodeDescAllocatorAllocator(NODE_DESC_ALLOCATOR_ALLOCATOR_NAME, sizeof(Allocator<Allocator<TransList::NodeDesc>>), 1, sizeof(Allocator<Allocator<TransList::NodeDesc>>));


    m_nodeAllocatorAllocator.Init();
    m_descAllocatorAllocator.Init();
    m_nodeDescAllocatorAllocator.Init();

    Allocator<TransList::Node>* m_nodeAllocator = new (m_nodeAllocatorAllocator.Alloc()) Allocator<TransList::Node>(NODE_ALLOCATOR_NAME, NUM_OF_TRANSACTIONS * (THREAD_COUNT + 1)* sizeof(TransList::Node) * TRANSACTION_SIZE, THREAD_COUNT + 1, sizeof(TransList::Node));
    Allocator<TransList::Desc>* m_descAllocator = new (m_descAllocatorAllocator.Alloc()) Allocator<TransList::Desc>(DESC_ALLOCATOR_NAME, NUM_OF_TRANSACTIONS * THREAD_COUNT * TransList::Desc::SizeOf(TRANSACTION_SIZE), THREAD_COUNT, TransList::Desc::SizeOf(TRANSACTION_SIZE));
    Allocator<TransList::NodeDesc>* m_nodeDescAllocator = new (m_nodeDescAllocatorAllocator.Alloc()) Allocator<TransList::NodeDesc>(NODE_DESC_ALLOCATOR_NAME,NUM_OF_TRANSACTIONS * THREAD_COUNT * sizeof(TransList::NodeDesc) * TRANSACTION_SIZE, THREAD_COUNT, sizeof(TransList::NodeDesc));

    m_nodeAllocator->Init();


    TransList* transList = new TransList(m_nodeAllocator, m_descAllocator, m_nodeDescAllocator);



	for(long i=0; i<THREAD_COUNT; i++) { // Start our threads
        threads[i] = thread(runDurableLFTTThread, transList, i, m_nodeAllocator, m_descAllocator, m_nodeDescAllocator);
        
	}
	for(int i=0; i<THREAD_COUNT; i++) { // Wait for all threads to complete
		threads[i].join();
	}

    transList->Print();    

}

void runRecoveryDurableLFTTThread(TransList* list, int threadId, Allocator<TransList::Node>* m_nodeAllocator, Allocator<TransList::Desc>* m_descAllocator, Allocator<TransList::NodeDesc>* m_nodeDescAllocator) {
    
    m_nodeAllocator->Init();
    m_descAllocator->Init();
    m_nodeDescAllocator->Init();



    TransList::Node* new_node = m_nodeAllocator->Alloc();
    // printf("threadId=%d, key=%d, new_node=%p, new_node->next=%p\n\r", threadId, new_node->key, new_node, &(new_node->next));
    // printf("threadId=%d, new_node=%p, new_node->next=%p\n\r",  );
    // new_node = m_nodeAllocator->Alloc();
    // printf("threadId=%d, %d\n\r", threadId, new_node->key);
    // printf("new_node=%p, new_node->next=%p\n\r",  new_node, &(new_node->next));    
    int a = 0;
    while(new_node != nullptr && a < 40) {
        logger.pmem_durableds_dlog("threadId=", threadId, ", key=", new_node->key,", new_node=", new_node, ", &new_node->next=", &(new_node->next),", new_node->next=", new_node->next);
        new_node =  m_nodeAllocator->Alloc();
        a++;
    }

    

    // for(int t = 0; t < NUM_OF_TRANSACTIONS; t++) {

    //     TransList::Desc* desc = m_descAllocator->Alloc();
    //     desc->size = TRANSACTION_SIZE;
    //     desc->status = TransList::ACTIVE;

    //     DTX::PERSIST(desc);

    //     for(uint32_t i = 0; i < TRANSACTION_SIZE; ++i)
    //     {
    //         desc->ops[i].type = TransList::OpType::INSERT; 
    //         desc->ops[i].key = (threadId + 1) * 10 + (i + 1) + (t+1) * 100; 
    //         DTX::PERSIST(&(desc->ops[i]));
    //     }

    //     bool ret = list->ExecuteOps(desc);
    //     if(!ret)
    //         printf("transaction %d was failed!\n\r", t);
    // }

}

void lftt_recovery() {

    Allocator<Allocator<TransList::Node>> m_nodeAllocatorAllocator(NODE_ALLOCATOR_ALLOCATOR_NAME, sizeof(Allocator<Allocator<TransList::Node>>), 1, sizeof(Allocator<Allocator<TransList::Node>>));
    Allocator<Allocator<TransList::Desc>> m_descAllocatorAllocator(DESC_ALLOCATOR_ALLOCATOR_NAME, sizeof(Allocator<Allocator<TransList::Desc>>), 1, sizeof(Allocator<Allocator<TransList::Desc>>));
    Allocator<Allocator<TransList::NodeDesc>> m_nodeDescAllocatorAllocator(NODE_DESC_ALLOCATOR_ALLOCATOR_NAME, sizeof(Allocator<Allocator<TransList::NodeDesc>>), 1, sizeof(Allocator<Allocator<TransList::NodeDesc>>));


    m_nodeAllocatorAllocator.Init();
    m_descAllocatorAllocator.Init();
    m_nodeDescAllocatorAllocator.Init();

    Allocator<TransList::Node>* m_nodeAllocator = m_nodeAllocatorAllocator.Alloc();
    Allocator<TransList::Desc>* m_descAllocator = m_descAllocatorAllocator.Alloc();
    Allocator<TransList::NodeDesc>* m_nodeDescAllocator = m_nodeDescAllocatorAllocator.Alloc();

    m_nodeAllocator->reload_mem(NODE_ALLOCATOR_NAME);
    m_descAllocator->reload_mem(DESC_ALLOCATOR_NAME);
    m_nodeDescAllocator->reload_mem(NODE_DESC_ALLOCATOR_NAME);

    m_nodeAllocator->Init();
    m_descAllocator->Init();
    m_nodeDescAllocator->Init();

    TransList* transList = new TransList(m_nodeAllocator, m_descAllocator, m_nodeDescAllocator, false);

    transList->Print();

    logger.pmem_durableds_dlog("after transList->Print()\n\r");

}

pmem_durableds_logger logger(pmem_durableds_logger::log_severity_type::debug);


int main()
{
    logger.pmem_durableds_dlog("Hello World!\n\r");
    // test_lftt();  
    lftt_recovery(); 
    logger.pmem_durableds_dlog("Goodbye World!\n\r");
}