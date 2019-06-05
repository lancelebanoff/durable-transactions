#ifndef SETADAPTOR_H
#define SETADAPTOR_H

#include "../lftt/list/translist.h"
#include "../lftt/skiplist/transskip.h"
// #include "rstm/list/rstmlist.hpp"
// #include "boosting/list/boostinglist.h"
// #include "boosting/skiplist/boostingskip.h"
#include "../common/allocator.h"
// #include "ostm/skiplist/stmskip.h"

enum SetOpType
{
    FIND = 0,
    INSERT,
    DELETE
};

struct SetOperator
{
    uint8_t type;
    uint32_t key;
};

enum SetOpStatus
{
    LIVE = 0,
    COMMITTED,
    ABORTED
};

typedef std::vector<SetOperator> SetOpArray;

template<typename T>
class SetAdaptor
{
};

template<>
class SetAdaptor<TransList>
{
public:
    SetAdaptor(uint64_t cap, uint64_t threadCount, uint32_t transSize)
        : m_descAllocator(cap * threadCount * TransList::Desc::SizeOf(transSize), threadCount, TransList::Desc::SizeOf(transSize))
        , m_nodeAllocator(cap * threadCount *  sizeof(TransList::Node) * transSize, threadCount, sizeof(TransList::Node))
        , m_nodeDescAllocator(cap * threadCount *  sizeof(TransList::NodeDesc) * transSize, threadCount, sizeof(TransList::NodeDesc))
        , m_list(&m_nodeAllocator, &m_descAllocator, &m_nodeDescAllocator)
    { }

    void Init()
    {
        m_descAllocator.Init();
        m_nodeAllocator.Init();
        m_nodeDescAllocator.Init();
        m_list.ResetMetrics();
    }

    void Uninit(){}

    bool ExecuteOps(const SetOpArray& ops)
    {
        //TransList::Desc* desc = m_list.AllocateDesc(ops.size());
        TransList::Desc* desc = m_descAllocator.Alloc();
        desc->size = ops.size();
        desc->status = TransList::ACTIVE;

        for(uint32_t i = 0; i < ops.size(); ++i)
        {
            desc->ops[i].type = ops[i].type; 
            desc->ops[i].key = ops[i].key; 
        }

        return m_list.ExecuteOps(desc);
    }

private:
    Allocator<TransList::Desc> m_descAllocator;
    Allocator<TransList::Node> m_nodeAllocator;
    Allocator<TransList::NodeDesc> m_nodeDescAllocator;
    TransList m_list;
};

template<>
class SetAdaptor<trans_skip>
{
public:
    SetAdaptor(uint64_t cap, uint64_t threadCount, uint32_t transSize)
        : m_descAllocator(cap * threadCount * Desc::SizeOf(transSize), threadCount, Desc::SizeOf(transSize))
        , m_nodeDescAllocator(cap * threadCount *  sizeof(NodeDesc) * transSize, threadCount, sizeof(NodeDesc))
    { 
        m_skiplist = transskip_alloc(&m_descAllocator, &m_nodeDescAllocator);
        init_transskip_subsystem(); 
    }

    ~SetAdaptor()
    {
        transskip_free(m_skiplist);
    }

    void Init()
    {
        m_descAllocator.Init();
        m_nodeDescAllocator.Init();
        transskip_reset_metrics();
    }

    void Uninit()
    {
        destroy_transskip_subsystem(); 
    }

    bool ExecuteOps(const SetOpArray& ops)
    {
        //TransList::Desc* desc = m_list.AllocateDesc(ops.size());
        Desc* desc = m_descAllocator.Alloc();
        desc->size = ops.size();
        desc->status = LIVE;

        for(uint32_t i = 0; i < ops.size(); ++i)
        {
            desc->ops[i].type = ops[i].type; 
            desc->ops[i].key = ops[i].key; 
        }

        return execute_ops(m_skiplist, desc);
    }

private:
    Allocator<Desc> m_descAllocator;
    Allocator<NodeDesc> m_nodeDescAllocator;
    trans_skip* m_skiplist;
};




#endif /* end of include guard: SETADAPTOR_H */
