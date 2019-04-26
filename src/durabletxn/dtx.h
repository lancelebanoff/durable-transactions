#ifndef DTX_H
#define DTX_H

#include <immintrin.h>
#include <vector>
#include <boost/any.hpp>
#include "../lockfreelist/lockfreelist.h"

// Preprocessor parameter that determines whether or not to provide durability
#define USING_DURABLE_TXN 1

/* 
 * A single entry in the undo log.
 * It holds a pointer to data that will be changed, along with the old data.
 */
template <typename T>
struct LogEntry
{
    T* ptr;
    T oldData;

    LogEntry(T* ptr, T oldData)
    {
        this.ptr = ptr;
        this.oldData = oldData;
    }
};

enum TxStatus
{
    ACTIVE,
    COMMITTED
};

/* 
 * A log containing all of the write instructions that will be executed by the current transaction.
 * Each UndoLog is owned by a single thread.
 */
struct UndoLog
{
    std::vector<LogEntry<boost::any>>* entries;
    TxStatus status;

    void Init();

    template <typename T>
    void Push(T* ptr, T oldData);

    void Uninit();
};

/* 
 * Class for durable transaction support.
 * 
 * To allow a thread to run durable transactions:
 *      INIT()
 * 
 * To begin a durable transaction:
 *      TX_BEGIN()
 * 
 * Within the transaction, change all write instructions (including CAS) as follows:
 *      Before:
 *          n->next = right;
 *      After:
 *          CREATE_UNDO_LOG_ENTRY(&n);
 *          n->next = right;
 *          PERSIST(&n);
 * 
 * To commit a durable transaction:
 *      TX_COMMIT()
 */
class DTX
{
public:

    static __thread UndoLog* log;

    /* 
    * Creates an undo log for the thread.
    * Should be called for each thread before it runs any transactions.
    */
    static void INIT()
    {
        log = new UndoLog();
    }

    /* 
     * Begins a transaction.
     */
    static void TX_BEGIN()
    {
    #ifdef USING_DURABLE_TXN
        log->Init();
        log->status = ACTIVE;
        PERSIST(&(log->status));
    #endif
    }

    /* 
     * Commits a transaction.
     */
    static void TX_COMMIT()
    {
    #ifdef USING_DURABLE_TXN
        log->status = COMMITTED;
        PERSIST(&(log->status));
        log->Uninit();
    #endif
    }

    /* 
     * Adds a new entry to the undo log and persists it.
     * Should be called before executing a write instruction in a transaction.
     */
    template <typename T>
    static void CREATE_UNDO_LOG_ENTRY(T* ptr)
    {
    #ifdef USING_DURABLE_TXN
        log->Push(ptr, *ptr);
        PERSIST(ptr);
    #endif
    }

    /* 
     * A persistence barrier.
     * Inputs a pointer, and flushes the contents of that pointer, followed by an SFENCE.
     */
    template <typename T>
    static void PERSIST(T* ptr)
    {
    #ifdef USING_DURABLE_TXN
        _mm_clflush(ptr);
        _mm_sfence();
    #endif
    }

    /* 
     * TODO:
     * TX_ABORT()
     */

    /* 
     * TODO:
     * RECOVER()
     */
};


#endif /* end of include guard: DTX_H */