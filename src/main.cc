
//#define ENABLE_ASSERT

#include "common/allocator.h"
#include <cstdlib>
#include <cstdio>
#include <thread>

#include "lockfreelist/lockfreelist.h"
#include "boostinglist/boostinglist.h"
#include "lftt/translist.h"
#include "durabletxn/dtx.h"
#include "logging.h"

// #define RUN_COUNTER 19

using namespace std;

const int THREAD_COUNT = 2;

static const int NUM_OF_TRANSACTIONS = 12;
static const int TRANSACTION_SIZE = 2;




static const char *NODE_ALLOCATOR_NAME = "m_nodeAllocator23";
static const char *DESC_ALLOCATOR_NAME = "m_descAllocator23";
static const char *NODE_DESC_ALLOCATOR_NAME = "m_nodeDescAllocator23";

static const char *NODE_ALLOCATOR_ALLOCATOR_NAME = "m_nodeAllocatorAllocator23";
static const char *DESC_ALLOCATOR_ALLOCATOR_NAME = "m_descAllocatorAllocator23";
static const char *NODE_DESC_ALLOCATOR_ALLOCATOR_NAME = "m_nodeDescAllocatorAllocator23";

void runThread(LockfreeList *list, int threadId)
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

void runDurableBoostingThread(BoostingList *list, int threadId, Allocator<LockfreeList::Node> *m_nodeAllocator)
{
    m_nodeAllocator->Init();
    list->Init(m_nodeAllocator);
    DTX::INIT(); // should be run once per thread

    BoostingList::ReturnCode ret;

    for (int t = 0; t < NUM_OF_TRANSACTIONS; t++)
    {
        DTX::TX_BEGIN();

        for (int i = 0; i < TRANSACTION_SIZE; i++)
        {
            ret = list->Insert(threadId + (i + 1) + (t + 1) * 10);
            if (ret != BoostingList::OK)
            {
                list->OnAbort();
                break;
            }
        }
        if (ret == BoostingList::OK)
        {
            DTX::TX_COMMIT();
            list->OnCommit();
        }
    }
}

void runDurableLFTTThreadInsertOnly(TransList *list, int threadId, Allocator<TransList::Node> *m_nodeAllocator, Allocator<TransList::Desc> *m_descAllocator, Allocator<TransList::NodeDesc> *m_nodeDescAllocator)
{

    m_nodeAllocator->Init();
    m_descAllocator->Init();
    m_nodeDescAllocator->Init();

    for (int t = 0; t < NUM_OF_TRANSACTIONS; t++)
    {

        TransList::Desc *desc = m_descAllocator->Alloc();
        desc->size = TRANSACTION_SIZE;
        desc->status = TransList::ACTIVE;

        DTX::PERSIST(desc);

        for (uint32_t i = 0; i < TRANSACTION_SIZE; ++i)
        {
            desc->ops[i].type = TransList::OpType::INSERT;
            desc->ops[i].key = (threadId + 1) * 1000 + (t + 1) * 10 + (i + 1);
            DTX::PERSIST(&(desc->ops[i]));
        }

        bool ret = list->ExecuteOps(desc);
        if (!ret)
            logger.pmem_durableds_elog("transaction ", t, " was failed!");
    }
}

void runDurableLFTTThreadInsertDelete(TransList *list, int threadId, Allocator<TransList::Node> *m_nodeAllocator, Allocator<TransList::Desc> *m_descAllocator, Allocator<TransList::NodeDesc> *m_nodeDescAllocator)
{

    m_nodeAllocator->Init();
    m_descAllocator->Init();
    m_nodeDescAllocator->Init();

    for (int t = 0; t < 3 * NUM_OF_TRANSACTIONS / 4; t++)
    {

        TransList::Desc *desc = m_descAllocator->Alloc();
        desc->size = TRANSACTION_SIZE;
        desc->status = TransList::ACTIVE;

        DTX::PERSIST(desc);

        for (uint32_t i = 0; i < TRANSACTION_SIZE; ++i)
        {
            desc->ops[i].type = TransList::OpType::INSERT;
            desc->ops[i].key = (threadId + 1) * 1000 + (t + 1) * 10 + (i + 1);
            DTX::PERSIST(&(desc->ops[i]));
        }

        bool ret = list->ExecuteOps(desc);
        if (!ret)
            logger.pmem_durableds_elog("transaction ", t, " was failed!");
    }

    for (int t = 0; t < NUM_OF_TRANSACTIONS; t+=4)
    {

        TransList::Desc *desc = m_descAllocator->Alloc();
        desc->size = TRANSACTION_SIZE;
        desc->status = TransList::ACTIVE;

        DTX::PERSIST(desc);

        for (uint32_t i = 0; i < TRANSACTION_SIZE; ++i)
        {
            desc->ops[i].type = TransList::OpType::DELETE;
            desc->ops[i].key = (threadId + 1) * 1000 + (t + 1) * 10 + (i + 1);
            DTX::PERSIST(&(desc->ops[i]));
        }

        bool ret = list->ExecuteOps(desc);
        if (!ret)
            logger.pmem_durableds_elog("transaction ", t, " was failed!");
    }
}

void test_lockfreelist()
{
    thread threads[THREAD_COUNT]; // Create our threads
    Allocator<LockfreeList::Node> m_nodeAllocator(NUM_OF_TRANSACTIONS * THREAD_COUNT * sizeof(LockfreeList::Node) * TRANSACTION_SIZE, THREAD_COUNT, sizeof(LockfreeList::Node));
    LockfreeList *lockfreelist = new LockfreeList(&m_nodeAllocator);
    for (long i = 0; i < THREAD_COUNT; i++)
    { // Start our threads
        threads[i] = thread(runThread, lockfreelist, i);
    }
    for (int i = 0; i < THREAD_COUNT; i++)
    { // Wait for all threads to complete
        threads[i].join();
    }

    lockfreelist->Print();
}

void test_boosting()
{
    thread threads[THREAD_COUNT];
    Allocator<LockfreeList::Node> m_nodeAllocator(NUM_OF_TRANSACTIONS * THREAD_COUNT * sizeof(LockfreeList::Node) * TRANSACTION_SIZE, THREAD_COUNT, sizeof(LockfreeList::Node));
    BoostingList *boostinglist = new BoostingList();

    for (long i = 0; i < THREAD_COUNT; i++)
    { // Start our threads
        threads[i] = thread(runDurableBoostingThread, boostinglist, i, &m_nodeAllocator);
    }
    for (int i = 0; i < THREAD_COUNT; i++)
    { // Wait for all threads to complete
        threads[i].join();
    }

    boostinglist->Print();
}

void test_lftt_insert_only()
{
    thread threads[THREAD_COUNT]; // Create our threads

    Allocator<Allocator<TransList::Node>> m_nodeAllocatorAllocator(NODE_ALLOCATOR_ALLOCATOR_NAME, sizeof(Allocator<Allocator<TransList::Node>>), 1, sizeof(Allocator<Allocator<TransList::Node>>));
    Allocator<Allocator<TransList::Desc>> m_descAllocatorAllocator(DESC_ALLOCATOR_ALLOCATOR_NAME, sizeof(Allocator<Allocator<TransList::Desc>>), 1, sizeof(Allocator<Allocator<TransList::Desc>>));
    Allocator<Allocator<TransList::NodeDesc>> m_nodeDescAllocatorAllocator(NODE_DESC_ALLOCATOR_ALLOCATOR_NAME, sizeof(Allocator<Allocator<TransList::NodeDesc>>), 1, sizeof(Allocator<Allocator<TransList::NodeDesc>>));

    m_nodeAllocatorAllocator.Init();
    m_descAllocatorAllocator.Init();
    m_nodeDescAllocatorAllocator.Init();

    Allocator<TransList::Node> *m_nodeAllocator = new (m_nodeAllocatorAllocator.Alloc()) Allocator<TransList::Node>(NODE_ALLOCATOR_NAME, NUM_OF_TRANSACTIONS * (THREAD_COUNT + 1) * sizeof(TransList::Node) * TRANSACTION_SIZE, THREAD_COUNT + 1, sizeof(TransList::Node));
    Allocator<TransList::Desc> *m_descAllocator = new (m_descAllocatorAllocator.Alloc()) Allocator<TransList::Desc>(DESC_ALLOCATOR_NAME, NUM_OF_TRANSACTIONS * THREAD_COUNT * TransList::Desc::SizeOf(TRANSACTION_SIZE), THREAD_COUNT, TransList::Desc::SizeOf(TRANSACTION_SIZE));
    Allocator<TransList::NodeDesc> *m_nodeDescAllocator = new (m_nodeDescAllocatorAllocator.Alloc()) Allocator<TransList::NodeDesc>(NODE_DESC_ALLOCATOR_NAME, NUM_OF_TRANSACTIONS * THREAD_COUNT * sizeof(TransList::NodeDesc) * TRANSACTION_SIZE, THREAD_COUNT, sizeof(TransList::NodeDesc));

    m_nodeAllocator->Init();

    TransList *transList = new TransList(m_nodeAllocator, m_descAllocator, m_nodeDescAllocator);

    for (long i = 0; i < THREAD_COUNT; i++)
    { // Start our threads
        threads[i] = thread(runDurableLFTTThreadInsertOnly, transList, i, m_nodeAllocator, m_descAllocator, m_nodeDescAllocator);
    }
    for (int i = 0; i < THREAD_COUNT; i++)
    { // Wait for all threads to complete
        threads[i].join();
    }

    transList->Print();
}

void test_lftt_insert_delete()
{
    thread threads[THREAD_COUNT]; // Create our threads

    Allocator<Allocator<TransList::Node>> m_nodeAllocatorAllocator(NODE_ALLOCATOR_ALLOCATOR_NAME, sizeof(Allocator<Allocator<TransList::Node>>), 1, sizeof(Allocator<Allocator<TransList::Node>>));
    Allocator<Allocator<TransList::Desc>> m_descAllocatorAllocator(DESC_ALLOCATOR_ALLOCATOR_NAME, sizeof(Allocator<Allocator<TransList::Desc>>), 1, sizeof(Allocator<Allocator<TransList::Desc>>));
    Allocator<Allocator<TransList::NodeDesc>> m_nodeDescAllocatorAllocator(NODE_DESC_ALLOCATOR_ALLOCATOR_NAME, sizeof(Allocator<Allocator<TransList::NodeDesc>>), 1, sizeof(Allocator<Allocator<TransList::NodeDesc>>));

    m_nodeAllocatorAllocator.Init();
    m_descAllocatorAllocator.Init();
    m_nodeDescAllocatorAllocator.Init();

    Allocator<TransList::Node> *m_nodeAllocator = new (m_nodeAllocatorAllocator.Alloc()) Allocator<TransList::Node>(NODE_ALLOCATOR_NAME, NUM_OF_TRANSACTIONS * (THREAD_COUNT + 1) * sizeof(TransList::Node) * TRANSACTION_SIZE, THREAD_COUNT + 1, sizeof(TransList::Node));
    Allocator<TransList::Desc> *m_descAllocator = new (m_descAllocatorAllocator.Alloc()) Allocator<TransList::Desc>(DESC_ALLOCATOR_NAME, NUM_OF_TRANSACTIONS * THREAD_COUNT * TransList::Desc::SizeOf(TRANSACTION_SIZE), THREAD_COUNT, TransList::Desc::SizeOf(TRANSACTION_SIZE));
    Allocator<TransList::NodeDesc> *m_nodeDescAllocator = new (m_nodeDescAllocatorAllocator.Alloc()) Allocator<TransList::NodeDesc>(NODE_DESC_ALLOCATOR_NAME, NUM_OF_TRANSACTIONS * THREAD_COUNT * sizeof(TransList::NodeDesc) * TRANSACTION_SIZE, THREAD_COUNT, sizeof(TransList::NodeDesc));

    m_nodeAllocator->Init();

    TransList *transList = new TransList(m_nodeAllocator, m_descAllocator, m_nodeDescAllocator);

    for (long i = 0; i < THREAD_COUNT; i++)
    { // Start our threads
        threads[i] = thread(runDurableLFTTThreadInsertDelete, transList, i, m_nodeAllocator, m_descAllocator, m_nodeDescAllocator);
    }
    for (int i = 0; i < THREAD_COUNT; i++)
    { // Wait for all threads to complete
        threads[i].join();
    }

    transList->Print();
}

void runRecoveryDurableLFTTThread(TransList *list, int threadId, Allocator<TransList::Node> *m_nodeAllocator, Allocator<TransList::Desc> *m_descAllocator, Allocator<TransList::NodeDesc> *m_nodeDescAllocator)
{

    m_nodeAllocator->Init();
    m_descAllocator->Init();
    m_nodeDescAllocator->Init();

    TransList::Node *new_node = m_nodeAllocator->Alloc();
    // printf("threadId=%d, key=%d, new_node=%p, new_node->next=%p\n\r", threadId, new_node->key, new_node, &(new_node->next));
    // printf("threadId=%d, new_node=%p, new_node->next=%p\n\r",  );
    // new_node = m_nodeAllocator->Alloc();
    // printf("threadId=%d, %d\n\r", threadId, new_node->key);
    // printf("new_node=%p, new_node->next=%p\n\r",  new_node, &(new_node->next));
    int a = 0;
    while (new_node != nullptr && a < 40)
    {
        logger.pmem_durableds_dlog("threadId=", threadId, ", key=", new_node->key, ", new_node=", new_node, ", &new_node->next=", &(new_node->next), ", new_node->next=", new_node->next);
        new_node = m_nodeAllocator->Alloc();
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

void lftt_recovery()
{

    Allocator<Allocator<TransList::Node>> m_nodeAllocatorAllocator(NODE_ALLOCATOR_ALLOCATOR_NAME, sizeof(Allocator<Allocator<TransList::Node>>), 1, sizeof(Allocator<Allocator<TransList::Node>>));
    Allocator<Allocator<TransList::Desc>> m_descAllocatorAllocator(DESC_ALLOCATOR_ALLOCATOR_NAME, sizeof(Allocator<Allocator<TransList::Desc>>), 1, sizeof(Allocator<Allocator<TransList::Desc>>));
    Allocator<Allocator<TransList::NodeDesc>> m_nodeDescAllocatorAllocator(NODE_DESC_ALLOCATOR_ALLOCATOR_NAME, sizeof(Allocator<Allocator<TransList::NodeDesc>>), 1, sizeof(Allocator<Allocator<TransList::NodeDesc>>));

    m_nodeAllocatorAllocator.Init();
    m_descAllocatorAllocator.Init();
    m_nodeDescAllocatorAllocator.Init();

    Allocator<TransList::Node> *m_nodeAllocator = m_nodeAllocatorAllocator.getFirst();
    Allocator<TransList::Desc> *m_descAllocator = m_descAllocatorAllocator.getFirst();
    Allocator<TransList::NodeDesc> *m_nodeDescAllocator = m_nodeDescAllocatorAllocator.getFirst();

    m_nodeAllocator->print();

    m_nodeAllocator->reload_mem(NODE_ALLOCATOR_NAME);
    m_descAllocator->reload_mem(DESC_ALLOCATOR_NAME);
    m_nodeDescAllocator->reload_mem(NODE_DESC_ALLOCATOR_NAME);

    m_nodeAllocator->print();

    m_nodeAllocator->Init();
    m_descAllocator->Init();
    m_nodeDescAllocator->Init();

    m_nodeAllocator->print();
    m_descAllocator->print();
    m_nodeDescAllocator->print();

    TransList *transList = new TransList(m_nodeAllocator, m_descAllocator, m_nodeDescAllocator, false);

    transList->Print();

    // transList->CheckConsistency();

    for(int i = 0; i < THREAD_COUNT; i++) {
        TransList::Desc *curr = m_descAllocator->getFirstForThread(i);
        TransList::Desc *next = m_descAllocator->getNextForThread(curr, i);
        while(curr != nullptr) {
            printf("curr=%p\n\r", curr);
            logger.pmem_durableds_dlog(curr->toString());
            curr = next;
            next = m_descAllocator->getNextForThread(next, i);
        }
    }

    



    logger.pmem_durableds_dlog("after transList->Print()\n\r");
}

pmem_durableds_logger logger(pmem_durableds_logger::log_severity_type::debug);

int main()
{
    logger.pmem_durableds_dlog("Hello World!\n\r");
    // test_lftt_insert_only();
    // test_lftt_insert_delete();
    // test_lftt();
    lftt_recovery();
    logger.pmem_durableds_dlog("Goodbye World!\n\r");
}