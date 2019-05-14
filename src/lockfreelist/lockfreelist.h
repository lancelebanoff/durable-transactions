#ifndef LLIST_H_ 
#define LLIST_H_

#include "../common/allocator.h"
#include <cstdint>
#include "../durabletxn/dtx.h"


// boolean CAS_PTR for ease of coding.
#define CAS_PTR_BOOL(addr, old, new) (old == CAS_PTR(addr, old, new))
#define MEM_BLOCK_SIZE 1000000 //16MB (node_t = 16b)
#define MEM_BLOCK_CNT 500 // 8GB of mem max

class LockfreeList
{
public:
    struct Node 
    {
        uint32_t key;
        Node* next;
    };

    LockfreeList(Allocator<Node>* nodeAllocator);
    ~LockfreeList();

    bool Find(uint32_t key);
    bool Insert(uint32_t key);
    bool Delete(uint32_t key);

    int Size();

    void Print();

private:
    Node* LocatePred(uint32_t key, Node** left_node);

private:
    Node* m_head;
    Node* m_tail;

    // Memory pool
    Allocator<Node>* m_nodeAllocator;
    Node** mem; // Memory blocks
    uint32_t memptr; // Current cell
};

#endif
