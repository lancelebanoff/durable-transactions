#ifndef DTX_H
#define DTX_H

#include <immintrin.h>
#include <vector>
#include <boost/any.hpp>
#include "../lockfreelist/lockfreelist.h"

#define USING_DURABLE_TXN 1

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

struct UndoLog
{
    std::vector<LogEntry<boost::any>>* entries;
    TxStatus status;

    void Init();

    template <typename T>
    void Push(T* ptr, T oldData);

    void Uninit();
};

class DTX
{
public:

    static __thread UndoLog* log;

    static void INIT()
    {
        log = new UndoLog();
    }

    static void TX_BEGIN()
    {
    #ifdef USING_DURABLE_TXN
        log->Init();
        log->status = ACTIVE;
        PERSIST(&(log->status));
    #endif
    }

    static void TX_COMMIT()
    {
    #ifdef USING_DURABLE_TXN
        log->status = COMMITTED;
        PERSIST(&(log->status));
        log->Uninit();
    #endif
    }

    template <typename T>
    static void CREATE_UNDO_LOG_ENTRY(T* ptr)
    {
    #ifdef USING_DURABLE_TXN
        log->Push(ptr, *ptr);
        PERSIST(ptr);
    #endif
    }

    template <typename T>
    static void PERSIST(T* ptr)
    {
    #ifdef USING_DURABLE_TXN
        _mm_clflush(ptr);
        _mm_sfence();
    #endif
    }

};


#endif /* end of include guard: DTX_H */