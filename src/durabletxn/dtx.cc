
#include "../lockfreelist/lockfreelist.h"
#include "dtx.h"

// static __thread UndoLog* log = NULL;

__thread UndoLog* DTX::log;




void UndoLog::Init()
{
    entries = new std::map<uintptr_t, LogEntry>();
}

void UndoLog::Push(void* ptr, int size)
{
    auto key = reinterpret_cast<std::uintptr_t>(ptr);
    entries->insert(std::pair<uintptr_t, LogEntry>(key, LogEntry(ptr, size)));
}

void UndoLog::Uninit()
{
    delete entries;
}