//------------------------------------------------------------------------------
// 
//     
//
//------------------------------------------------------------------------------


#include <cstdlib>
#include <cstdio>
#include <new>
#include <set>
#include "translist.h"
#include "../durabletxn/dtx.h"
#include "../logging.h"

#define SET_MARK(_p)    ((Node *)(((uintptr_t)(_p)) | 1))
#define CLR_MARK(_p)    ((Node *)(((uintptr_t)(_p)) & ~1))
#define CLR_MARKD(_p)    ((NodeDesc *)(((uintptr_t)(_p)) & ~1))
#define IS_MARKED(_p)     (((uintptr_t)(_p)) & 1)

extern pmem_durableds_logger logger;
__thread TransList::HelpStack helpStack;

TransList::TransList(Allocator<Node>* nodeAllocator, Allocator<Desc>* descAllocator, Allocator<NodeDesc>* nodeDescAllocator, bool newList)
    : m_nodeAllocator(nodeAllocator)
    , m_descAllocator(descAllocator)
    , m_nodeDescAllocator(nodeDescAllocator)
{
    if(newList) {
        logger.pmem_durableds_dlog("TransList constructor was called as a newlist \n\r");
        m_tail = new (m_nodeAllocator->Alloc()) Node(0xffffffff, NULL, NULL);
        m_head = new (m_nodeAllocator->Alloc()) Node(0, m_tail, NULL);
    }else {
        logger.pmem_durableds_dlog("TransList constructor was called as an existing list \n\r");
        m_tail = m_nodeAllocator->getFirst();
        m_head = m_nodeAllocator->getNext(m_tail);
    }

}

TransList::~TransList()
{
    printf("Total commit %u, abort (total/fake) %u/%u\n", g_count_commit, g_count_abort, g_count_fake_abort);
    //Print();

    ASSERT_CODE
    (
        printf("Total node count %u, Inserts (total/new) %u/%u, Deletes (total/new) %u/%u, Finds %u\n", g_count, g_count_ins, g_count_ins_new, g_count_del , g_count_del_new, g_count_fnd);
    );

    //Node* curr = m_head;
    //while(curr != NULL)
    //{
        //free(curr);
        //curr = curr->next;
    //}
}

TransList::Desc* TransList::AllocateDesc(uint8_t size)
{
    Desc* desc = m_descAllocator->Alloc();
    desc->size = size;
    desc->status = ACTIVE;
    
    return desc;
}

bool TransList::ExecuteOps(Desc* desc)
{
    helpStack.Init();

    HelpOps(desc, 0);

    bool ret = desc->status != ABORTED;

    ASSERT_CODE
    (
        if(ret)
        {
            for(uint32_t i = 0; i < desc->size; ++i)
            {
                if(desc->ops[i].type == INSERT)
                {
                    __sync_fetch_and_add(&g_count, 1);
                }
                else if(desc->ops[i].type == DELETE)
                {
                    __sync_fetch_and_sub(&g_count, 1);
                }
                else
                {
                    __sync_fetch_and_add(&g_count_fnd, 1);
                }
            }
        }
    );

    return ret;
}


void TransList::ResetMetrics()
{
    g_count_commit = 0;
    g_count_abort = 0;
    g_count_fake_abort = 0;
}

inline void TransList::MarkForDeletion(const std::vector<Node*>& nodes, const std::vector<Node*>& preds, Desc* desc)
{
    // printf("MarkForDeletion was called with size=%d! \n\r", nodes.size());
    // Mark nodes for logical deletion
    for(uint32_t i = 0; i < nodes.size(); ++i)
    {
        Node* n = nodes[i];
        if(n != NULL)
        {
            NodeDesc* nodeDesc = n->nodeDesc;

            if(nodeDesc->desc == desc)
            {
                if(__sync_bool_compare_and_swap(&n->nodeDesc, nodeDesc, SET_MARK(nodeDesc)))
                {
                    DTX::PERSIST(&n->nodeDesc);
                    Node* pred = preds[i];
                    Node* succ = CLR_MARK(__sync_fetch_and_or(&n->next, 0x1));
                    DTX::PERSIST(&n->next);
                    __sync_bool_compare_and_swap(&pred->next, n, succ);
                    DTX::PERSIST(&pred->next);
                }
            }
        }
    }
}

inline void TransList::HelpOps(Desc* desc, uint8_t opid)
{
    if(desc->status != ACTIVE)
    {
        return;
    }

    //Cyclic dependcy check
    if(helpStack.Contain(desc))
    {
        if(__sync_bool_compare_and_swap(&desc->status, ACTIVE, ABORTED))
        {
            DTX::PERSIST(&desc);
            __sync_fetch_and_add(&g_count_abort, 1);
            __sync_fetch_and_add(&g_count_fake_abort, 1);
        }

        return;
    }

    ReturnCode ret = OK;
    std::vector<Node*> delNodes;
    std::vector<Node*> delPredNodes;
    std::vector<Node*> insNodes;
    std::vector<Node*> insPredNodes;


    

    helpStack.Push(desc);

    while(desc->status == ACTIVE && ret != FAIL && opid < desc->size)
    {
        const Operator& op = desc->ops[opid];

        if(op.type == INSERT)
        {
            Node* inserted;
            Node* pred;
            ret = Insert(op.key, desc, opid, inserted, pred);

            insNodes.push_back(inserted);
            insPredNodes.push_back(pred);
        }
        else if(op.type == DELETE)
        {
            Node* deleted;
            Node* pred;
            ret = Delete(op.key, desc, opid, deleted, pred);            

            delNodes.push_back(deleted);
            delPredNodes.push_back(pred);
        }
        else
        {
            ret = Find(op.key, desc, opid);
        }
        
        opid++;
    }

    helpStack.Pop();

    if(ret != FAIL)
    {
        // printf("transaction was succesfull \n\r");
        if(__sync_bool_compare_and_swap(&desc->status, ACTIVE, COMMITTED))
        {
            // printf("transaction status was changed to committed: %d\n\r", desc->status);
            
            DTX::PERSIST(desc);

            // printf("after persist: %d\n\r", desc->status);

            MarkForDeletion(delNodes, delPredNodes, desc);
            __sync_fetch_and_add(&g_count_commit, 1);
        }else {
            logger.pmem_durableds_elog("failed to change the transaction status to committed");
        }
    }
    else
    {
        logger.pmem_durableds_elog("transaction was failed!");
        if(__sync_bool_compare_and_swap(&desc->status, ACTIVE, ABORTED))
        {
            DTX::PERSIST(desc);
            logger.pmem_durableds_dlog("marked for deletion! \n\r");
            MarkForDeletion(insNodes, insPredNodes, desc);
            __sync_fetch_and_add(&g_count_abort, 1);
        }     
    }
}

inline TransList::ReturnCode TransList::Insert(uint32_t key, Desc* desc, uint8_t opid, Node*& inserted, Node*& pred)
{
    inserted = NULL;
    NodeDesc* nodeDesc = new(m_nodeDescAllocator->Alloc()) NodeDesc(desc, opid);

    DTX::PERSIST(nodeDesc);

    Node* new_node = NULL;
    Node* curr = m_head;

    while(true)
    {
        LocatePred(pred, curr, key);

        if(!IsNodeExist(curr, key))
        {
            //Node* pred_next = pred->next;

            if(desc->status != ACTIVE)
            {
                return FAIL;
            }

            //if(pred_next == curr)
            //{
                if(new_node == NULL)
                {
                    
                    new_node = new(m_nodeAllocator->Alloc()) Node(key, NULL, nodeDesc);
                }
                new_node->next = curr;
                DTX::PERSIST(&new_node);

                Node* pred_next = __sync_val_compare_and_swap(&pred->next, curr, new_node);

                DTX::PERSIST(&pred->next);

                if(pred_next == curr)
                {
                    ASSERT_CODE
                        (
                         __sync_fetch_and_add(&g_count_ins, 1);
                         __sync_fetch_and_add(&g_count_ins_new, 1);
                        );

                    inserted = new_node;
                    return OK;
                }
            //}

            // Restart
            curr = IS_MARKED(pred_next) ? m_head : pred;
        }
        else 
        {
            NodeDesc* oldCurrDesc = curr->nodeDesc;

            if(IS_MARKED(oldCurrDesc))
            {
                if(!IS_MARKED(curr->next))
                {
                    (__sync_fetch_and_or(&curr->next, 0x1));
                    DTX::PERSIST(&curr->next);
                }
                curr = m_head;
                continue;
            }

            FinishPendingTxn(oldCurrDesc, desc);

            if(IsSameOperation(oldCurrDesc, nodeDesc))
            {
                return SKIP;
            }

            if(!IsKeyExist(oldCurrDesc))
            {
                NodeDesc* currDesc = curr->nodeDesc;

                if(desc->status != ACTIVE)
                {
                    return FAIL;
                }

                //if(currDesc == oldCurrDesc)
                {
                    //Update desc 
                    currDesc = __sync_val_compare_and_swap(&curr->nodeDesc, oldCurrDesc, nodeDesc);
                    DTX::PERSIST(&curr->nodeDesc);

                    if(currDesc == oldCurrDesc)
                    {
                        ASSERT_CODE
                            (
                             __sync_fetch_and_add(&g_count_ins, 1);
                            );

                        inserted = curr;
                        return OK; 
                    }
                }
            }
            else
            {
                return FAIL;
            }
        }
    }
}

inline TransList::ReturnCode TransList::Delete(uint32_t key, Desc* desc, uint8_t opid, Node*& deleted, Node*& pred)
{
    deleted = NULL;
    NodeDesc* nodeDesc = new(m_nodeDescAllocator->Alloc()) NodeDesc(desc, opid);
    Node* curr = m_head;

    while(true)
    {
        LocatePred(pred, curr, key);

        if(IsNodeExist(curr, key))
        {
            NodeDesc* oldCurrDesc = curr->nodeDesc;

            if(IS_MARKED(oldCurrDesc))
            {
                return FAIL;
                //Help removed deleted nodes
                //if(!IS_MARKED(curr->next))
                //{
                    //__sync_fetch_and_or(&curr->next, 0x1);
                //}
                //curr = m_head;
                //continue;
            }

            FinishPendingTxn(oldCurrDesc, desc);

            if(IsSameOperation(oldCurrDesc, nodeDesc))
            {
                return SKIP;
            }

            if(IsKeyExist(oldCurrDesc))
            {
                NodeDesc* currDesc = curr->nodeDesc;

                if(desc->status != ACTIVE)
                {
                    return FAIL;
                }

                //if(currDesc == oldCurrDesc)
                {
                    //Update desc 
                    DTX::PERSIST(nodeDesc);
                    currDesc = __sync_val_compare_and_swap(&curr->nodeDesc, oldCurrDesc, nodeDesc);
                    DTX::PERSIST(&curr->nodeDesc);

                    if(currDesc == oldCurrDesc)
                    {
                        ASSERT_CODE
                            (
                             __sync_fetch_and_add(&g_count_del, 1);
                            );

                        deleted = curr;
                        return OK; 
                    }
                }
            }
            else
            {
                return FAIL;
            }  
        }
        else 
        {
            return FAIL;      
        }
    }
}


inline bool TransList::IsSameOperation(NodeDesc* nodeDesc1, NodeDesc* nodeDesc2)
{
    return nodeDesc1->desc == nodeDesc2->desc && nodeDesc1->opid == nodeDesc2->opid;
}


inline TransList::ReturnCode TransList::Find(uint32_t key, Desc* desc, uint8_t opid)
{
    NodeDesc* nodeDesc = NULL;
    Node* pred;
    Node* curr = m_head;

    while(true)
    {
        LocatePred(pred, curr, key);

        if(IsNodeExist(curr, key))
        {
            NodeDesc* oldCurrDesc = curr->nodeDesc;

            if(IS_MARKED(oldCurrDesc))
            {
                if(!IS_MARKED(curr->next))
                {
                    (__sync_fetch_and_or(&curr->next, 0x1));
                    DTX::PERSIST(&curr->next);
                }
                curr = m_head;
                continue;
            }

            FinishPendingTxn(oldCurrDesc, desc);

            if(nodeDesc == NULL) nodeDesc = new(m_nodeDescAllocator->Alloc()) NodeDesc(desc, opid);

            if(IsSameOperation(oldCurrDesc, nodeDesc))
            {
                return SKIP;
            }

            if(IsKeyExist(oldCurrDesc))
            {
                NodeDesc* currDesc = curr->nodeDesc;

                if(desc->status != ACTIVE)
                {
                    return FAIL;
                }

                //if(currDesc == oldCurrDesc)
                {
                    //Update desc 
                    DTX::PERSIST(nodeDesc);
                    currDesc = __sync_val_compare_and_swap(&curr->nodeDesc, oldCurrDesc, nodeDesc);
                    DTX::PERSIST(&curr->nodeDesc);

                    if(currDesc == oldCurrDesc)
                    {
                        return OK; 
                    }
                }
            }
            else
            {
                return FAIL;
            }
        }
        else 
        {
            return FAIL;
        }
    }
}

inline bool TransList::IsNodeExist(Node* node, uint32_t key)
{
    return node != NULL && node->key == key;
}

inline void TransList::FinishPendingTxn(NodeDesc* nodeDesc, Desc* desc)
{
    // The node accessed by the operations in same transaction is always active 
    if(nodeDesc->desc == desc)
    {
        return;
    }

    HelpOps(nodeDesc->desc, nodeDesc->opid + 1);
}

inline bool TransList::IsNodeActive(NodeDesc* nodeDesc)
{
    return nodeDesc->desc->status == COMMITTED;
}

inline bool TransList::IsKeyExist(NodeDesc* nodeDesc)
{
    bool isNodeActive = IsNodeActive(nodeDesc);
    uint8_t opType = nodeDesc->desc->ops[nodeDesc->opid].type;

    // printf("nodeDesc->desc->status=%d, nodeDesc->opid=%d\n\r", nodeDesc->desc->status, nodeDesc->opid);

    return  (opType == FIND) || (isNodeActive && opType == INSERT) || (!isNodeActive && opType == DELETE);
}

inline void TransList::LocatePred(Node*& pred, Node*& curr, uint32_t key)
{
    Node* pred_next;

    while(curr->key < key)
    {
        pred = curr;
        pred_next = CLR_MARK(pred->next);
        curr = pred_next;

        while(IS_MARKED(curr->next))
        {
            curr = CLR_MARK(curr->next);
        }

        if(curr != pred_next)
        {
            //Failed to remove deleted nodes, start over from pred
            if(!__sync_bool_compare_and_swap(&pred->next, pred_next, curr))
            {
                curr = m_head;
            } 
            else {
                DTX::PERSIST(&pred->next);
            }

            //__sync_bool_compare_and_swap(&pred->next, pred_next, curr);
        }
    }

    ASSERT(pred, "pred must be valid");
}

void TransList::CheckConsistency(std::set<int> existingKeySet)
{
    Node* curr = m_head->next;
    while(curr != m_tail) {
        std::set<int>::iterator it = existingKeySet.find(curr->key);
        if(it == existingKeySet.end()) {
            if(IsKeyExist(CLR_MARKD(curr->nodeDesc))) {
                logger.pmem_durableds_elog("Error: something is wrong with the key: ", curr->key, " it is not supposed to be in the list ");        
            } else {
                logger.pmem_durableds_dlog( curr->key, " is in the list, but is marked for deletion ");        
            }
        } else {
            logger.pmem_durableds_dlog( curr->key, " is in the list");
            existingKeySet.erase(it);
        }
        curr = CLR_MARK(curr->next);
    }
    if(existingKeySet.size() > 0) {
        logger.pmem_durableds_elog("The following elements were supposed to be in the list, but they were not found:");
        for(std::set<int>::iterator it = existingKeySet.begin(); it != existingKeySet.end(); ++it) {
            logger.pmem_durableds_elog("\t", *it);
        }
    }
}


void TransList::Print()
{
    Node* curr = m_head->next;

    while(curr != m_tail)
    {
        printf("Node [%p] Key [%u] Status [%s], curr->next=%p\n\r", curr, curr->key, IsKeyExist(CLR_MARKD(curr->nodeDesc))? "Exist":"Inexist", curr->next);
        curr = CLR_MARK(curr->next);
        
    }
}
