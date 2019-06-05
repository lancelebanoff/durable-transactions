#ifndef TRANSMDLIST_H
#define TRANSMDLIST_H

#include <cstdint>
#include <vector>
#include "../../common/assert.h"
#include "../../common/allocator.h"

#include "../../durabletxn/dtx.h"

template<uint8_t D = 16>
class TxMdList
{
public:

    typedef std::array<uint8_t, D> Coord;
    enum OpStatus
    {
        ACTIVE = 0,
        COMMITTED,
        ABORTED,
    };

    enum PersistStatus
    {
        MAYBE = 0,
        IN_PROGRESS,
        PERSISTED,
    };    

    enum ReturnCode
    {
        OK = 0,
        SKIP,
        FAIL
    };

    enum OpType
    {
        FIND = 0,
        INSERT,
        DELETE
    };

    struct Operator
    {
        uint8_t type;
        uint32_t key;
    };

    struct Desc
    {
        static size_t SizeOf(uint8_t size)
        {
            // return sizeof(uint8_t) + sizeof(uint8_t) + sizeof(Operator) * size;
            return sizeof(Desc) + sizeof(Operator) * size;
        }

        // Status of the transaction: values in [0, size] means live txn, values -1 means aborted, value -2 means committed.
        volatile uint8_t status;
        volatile uint8_t persistStatus;
        uint8_t size;
        Operator ops[];
    };
    
    struct NodeDesc
    {
        NodeDesc(Desc* _desc, uint8_t _opid)
            : desc(_desc), opid(_opid){}

        Desc* desc;
        uint8_t opid;
    };

    struct Node
    {
        Node();
        Node(uint32_t _key, NodeDesc* _nodeDesc);
        void Fill(const PredQuery& loc);
        
        static Coord KeyToCoord(uint32_t key);

        Coord coord;
        uint32_t key;
        NodeDesc* nodeDesc;
        
        std::atomic<Node*> child[D];
        std::atomic<uint16_t> adopt;        //higher 8 bits contain dp; lower 8 bits for dc
    };
    
    struct HelpStack
    {
        void Init()
        {
            index = 0;
        }

        void Push(Desc* desc)
        {
            ASSERT(index < 255, "index out of range");

            helps[index++] = desc;
        }

        void Pop()
        {
            ASSERT(index > 0, "nothing to pop");

            index--;
        }

        bool Contain(Desc* desc)
        {
            for(uint8_t i = 0; i < index; i++)
            {
                if(helps[i] == desc)
                {
                    return true;
                }
            }

            return false;
        }

        Desc* helps[256];
        uint8_t index;
    };

    TxMdList(Allocator<Node>* nodeAllocator, Allocator<Desc>* descAllocator, Allocator<NodeDesc>* nodeDescAllocator);
    ~TxMdList();

    bool ExecuteOps(Desc* desc);

    Desc* AllocateDesc(uint8_t size);
    void ResetMetrics();

    Coord coord;

    struct PredQuery
    {
        PredQuery(Node* _curr);
        void Reset(Node* _curr);
        
        Node* pred;
        Node* curr;
        uint8_t dp;
        uint8_t dc;
    };    

private:

    ReturnCode Insert(uint32_t key, Desc* desc, uint8_t opid, Node*& inserted, Node*& pred);
    ReturnCode Delete(uint32_t key, Desc* desc, uint8_t opid, Node*& deleted, Node*& pred);
    ReturnCode Find(uint32_t key, Desc* desc, uint8_t opid);

    bool IsNodeExist(Node* pred, Node* curr, uint8_t dp, uint8_t dc);
    void HelpOps(Desc* desc, uint8_t opid);
    bool IsSameOperation(NodeDesc* nodeDesc1, NodeDesc* nodeDesc2);
    void FinishPendingTxn(NodeDesc* nodeDesc, Desc* desc);
    bool IsNodeActive(NodeDesc* nodeDesc);
    bool IsKeyExist(NodeDesc* nodeDesc);
    void LocatePred(const Coord& coord, PredQuery& loc);

    TriState UpdateNodeInfo(PredQuery& loc, TxInfo* info, bool wantKey);
    TriState DoInsert(PredQuery& loc, const KeyType& key, TxInfo* info, GCProxy& gcProxy);

    void LocatePred(const Coord& coord, PredQuery& loc);
    void MarkForDeletion(PredQuery& loc, Desc* desc);    

    void AdoptChildren(Node* n, uint8_t _dp, uint8_t _dc);
    Node* FetchOrFlag(std::atomic<Node*>& atomic_ptr);

    void Print();

private:
    Node* head;
    GC gc;
};


#endif /* end of include guard: TRANSMDLIST_H */    
