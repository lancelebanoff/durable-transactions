//------------------------------------------------------------------------------
// 
//     
//
//------------------------------------------------------------------------------

#include <array>

#define SET_MARK(_p)    ((Node *)(((uintptr_t)(_p)) | 1))
#define CLR_MARK(_p)    ((Node *)(((uintptr_t)(_p)) & ~1))
#define CLR_MARKD(_p)    ((TxInfo *)(((uintptr_t)(_p)) & ~1))
#define IS_MARKED(_p)     (((uintptr_t)(_p)) & 1)

template<typename KeyType, uint8_t D, class Hash>
TxMdList<KeyType, D, Hash>::TxMdList()
    : head(new Node)
{
    static_assert(D <= 32, "Dimension must not be greater than 32");
}

template<typename KeyType, uint8_t D, class Hash>
TxMdList<KeyType, D, Hash>::~TxMdList()
{
    // Print();
}

template<typename KeyType, uint8_t D, class Hash>
inline bool TxMdList<KeyType, D, Hash>::Insert(const KeyType& key, TxInfo* info, TxCall& onAbort)
{
    PredQuery loc(head);
    Coord coord = Node::KeyToCoord(Hash()(key));

    TriState ret = RETRY;

    GCProxy gcProxy(gc);

    do
    {
        LocatePred(coord, loc);

        if(IsNodeExist(loc))
        {
            ret = UpdateNodeInfo(loc, info, false);
        }
        else 
        {
            ret = DoInsert(loc, key, info, gcProxy);
        }
    }
    while(ret == RETRY);

    if(ret == SUCCESS)
    {
        // onAbort = std::bind(&MarkNodeInfo, loc.curr, loc.pred, info->desc);
        return true;
    }
    else
    {
        return false;
    }
}

template<typename KeyType, uint8_t D, class Hash>
inline bool TxMdList<KeyType, D, Hash>::Delete(const KeyType& key, TxInfo* info, TxCall& onCommit)
{
    PredQuery loc(head);
    Coord coord = Node::KeyToCoord(Hash()(key));

    TriState ret = RETRY;

    GCProxy gcProxy(gc);

    do
    {
        LocatePred(coord, loc);

        if(IsNodeExist(loc))
        {
            ret = UpdateNodeInfo(loc, info, true);
        }
        else 
        {
            ret = FAIL;
        }
    }
    while(ret == RETRY);

    if(ret == SUCCESS)
    {
        // onCommit = std::bind(&MarkNodeInfo, loc.curr, loc.pred, info->desc);
        return true;
    }
    else
    {
        return false;
    }
}

template<typename KeyType, uint8_t D, class Hash>
inline bool TxMdList<KeyType, D, Hash>::Find(const KeyType& key, TxInfo* info)
{
    PredQuery loc(head);
    Coord coord = Node::KeyToCoord(Hash()(key));

    LocatePred(coord, loc);

    TriState ret = RETRY;

    GCProxy gcProxy(gc);

    do
    {
        LocatePred(coord, loc);

        if(IsNodeExist(loc))
        {
            ret = UpdateNodeInfo(loc, info, true);
        }
        else 
        {
            ret = FAIL;
        }
    }
    while(ret == RETRY);

    return ret == SUCCESS;
}

template<typename KeyType, uint8_t D, class Hash>
inline TriState TxMdList<KeyType, D, Hash>::UpdateNodeInfo(PredQuery& loc, TxInfo* info, bool wantKey)
{
    TxInfo* oldInfo = loc.curr->info;

    if(IS_MARKED(oldInfo))
    {
        // Do_Delete from base lock-free MDList
        Node* predChild = loc.pred->child[loc.dp].load(std::memory_order_acquire);
        if(!IS_MARKED(predChild))
        {
            std::ignore = __sync_fetch_and_or(&predChild, 0x1);
        }
        loc.Reset(head);
        // std::cout << "retry for updatenodeinfo oldinfo marked" << std::endl;
        return RETRY;
    }

    if(oldInfo->desc != info->desc)
    {
        oldInfo->desc->Execute(oldInfo->opid + 1);
    }
    else if(oldInfo->opid >= info->opid)
    {
        return SUCCESS;
    }

    bool hasKey = Base::IsKeyExist(oldInfo, info->desc);

    if((!hasKey && wantKey) || (hasKey && !wantKey))
    {
        return FAIL;
    }
   
    if(info->desc->status != ACTIVE)
    {
        return FAIL;
    }

    if(__sync_bool_compare_and_swap(&loc.curr->info, oldInfo, info))
    {
        return SUCCESS;
    }
    else
    {
        // std::cout << "retry for updatenodeinfo failed cas" << std::endl;
        return RETRY;
    }
}

template<typename KeyType, uint8_t D, class Hash>
inline TriState TxMdList<KeyType, D, Hash>::DoInsert(PredQuery& loc, const KeyType& key, TxInfo* info, GCProxy& gcProxy)
{
    //we must verify txn status immediately before the CAS update
    if(info->desc->status != ACTIVE)
    {
        return FAIL;
    }

    Node* newNode = gcProxy.Alloc(key, info);

    if(loc.curr)
    {
        AdoptChildren(loc.curr, loc.dp, loc.dc);
    }

    newNode->Fill(loc);

    if(loc.pred->child[loc.dp].compare_exchange_weak(loc.curr, newNode, std::memory_order_release, std::memory_order_relaxed))
    {
        AdoptChildren(newNode, 0, D);
        loc.curr = newNode;
        return SUCCESS;
    }
    else
    {
        Node* predChild = loc.pred->child[loc.dp].load(std::memory_order_acquire);
        if(IS_MARKED(predChild))
        {
            loc.Reset(head);
        }
        else
        {
            loc.curr = IS_MARKED(predChild) ? head : loc.pred;
            loc.dc = loc.dp;
        }
        // std::cout << "retry for doinsert failed cas" << std::endl;
        return RETRY;
    }
}

template<typename KeyType, uint8_t D, class Hash>
inline bool TxMdList<KeyType, D, Hash>::IsNodeExist(PredQuery& loc)
{
    return loc.dc == D;
}

template<typename KeyType, uint8_t D, class Hash>
inline void TxMdList<KeyType, D, Hash>::MarkNodeInfo(PredQuery& loc, TxDesc* desc)
{
    TxInfo* info = loc.curr->info;

    if(info->desc == desc)
    {
        if(__sync_bool_compare_and_swap(&loc.curr->info, info, SET_MARK(info)))
        {
            __sync_fetch_and_or(&loc.pred->child[loc.dc], 0x1);
        }
    }
}

template<typename KeyType, uint8_t D, class Hash>
inline void TxMdList<KeyType, D, Hash>::LocatePred(const Coord& coord, PredQuery& loc)
{
    while(loc.dc < D)
    {
        while(loc.curr && coord[loc.dc] > loc.curr->coord[loc.dc])
        {
            loc.dp = loc.dc;
            loc.pred = loc.curr;
            AdoptChildren(loc.curr, loc.dc, loc.dc + 1);
            loc.curr = CLR_MARK(loc.curr->child[loc.dc].load(std::memory_order_relaxed));
        }

        if(loc.curr == nullptr || coord[loc.dc] < loc.curr->coord[loc.dc])
        {
            break;
        }
        else
        {
            ++loc.dc;
        }
    }
}

template<typename KeyType, uint8_t D, class Hash>
inline void TxMdList<KeyType, D, Hash>::AdoptChildren(Node* n, uint8_t _dp, uint8_t _dc)
{
    uint16_t adopt = n->adopt.load(std::memory_order_relaxed);
    if(adopt == 0) return;

    uint8_t dp = adopt >> 8;
    uint8_t dc = adopt & 0x00ff;

    //Read curr node, then read adopt again
    //if adoption is still needed when adopt is read, 
    //then curr must be the correct node to adopt from
    Node* curr = n->child[dc].load(std::memory_order_relaxed);
    adopt = n->adopt.load(std::memory_order_acquire);

    //No need to adopt if [dp, dc) and [_dp, _dc) do not overlap
    if(adopt == 0 || _dc <= dp || _dp >= dc) return;

    for (uint8_t i = dp; i < dc; ++i) 
    {
        //TODO: make sure curr does not need to adopt children
        Node* child = curr->child[i];

        //Children slot of curr_node need to be marked as invalid before we copy them to newNode
        //LANCE: uncomment
        // child = FetchOrFlag(curr->child[i]);
        if(child)
        {
            static Node* nil = NULL; //std::cas does not like immediate value as expected
            n->child[i].compare_exchange_weak(nil, child, std::memory_order_relaxed, std::memory_order_relaxed);
        }
    }

    //Clear the adopt task
    n->adopt.compare_exchange_weak(adopt, 0, std::memory_order_release, std::memory_order_relaxed);
}

// template<typename KeyType, uint8_t D, class Hash>
// inline typename TxMdList<KeyType, D, Hash>::Node* FetchOrFlag(std::atomic<Node*>& atomic_ptr)
// {
//     Node* ptr;
//     do
//     {
//         ptr = atomic_ptr.load(std::memory_order_acquire);
//     }
//     while(!IS_MARKED(ptr) && atomic_ptr.compare_exchange_weak(ptr, SET_MARK(ptr), std::memory_order_release, std::memory_order_relaxed));

//     return CLR_MARK(ptr);
// }

template<typename KeyType, uint8_t D, class Hash>
inline void TxMdList<KeyType, D, Hash>::Print() const
{
    std::string prefix;
    Traverse(head, NULL, 0, prefix);
}

//------------------------------------------------------------------------------
template<typename KeyType, uint8_t D, class Hash>
inline TxMdList<KeyType, D, Hash>::PredQuery::PredQuery(Node* _curr)
: pred(nullptr)
, curr(_curr)
, dp(0)
, dc(0)
{}

template<typename KeyType, uint8_t D, class Hash>
void TxMdList<KeyType, D, Hash>::PredQuery::Reset(Node* _head)
{
    //restart traversal from _head
    pred = nullptr;
    curr = _head;
    dp = 0;
    dc = 0;
}

template<typename KeyType, uint8_t D, class Hash>
inline TxMdList<KeyType, D, Hash>::Node::Node()
: coord()
, key()
, info(nullptr)
, child()
, adopt(0)
{}

template<typename KeyType, uint8_t D, class Hash>
inline TxMdList<KeyType, D, Hash>::Node::Node(const KeyType& _key, TxInfo* _info)
: coord(KeyToCoord(Hash()(_key)))
, key(_key)
, info(_info)
, child()
, adopt(0)
{}

template<typename KeyType, uint8_t D, class Hash>
void TxMdList<KeyType, D, Hash>::Node::Fill(const PredQuery& loc)
{
    //shift dp to higher 8 bits of a 16 bits integer, and mask with dc
    adopt = loc.dp != loc.dc ? ((uint16_t)loc.dp) << 8 | loc.dc : 0;

    //Fill values for newNode, child is set to 1 for all children before dp
    //dp is the dimension where newNode is inserted, all dimension before that are invalid for newNode
    for(uint32_t i = 0; i < loc.dp; ++i)
    {
        child[i] = (Node*)0x1;
    }

    //be careful with the length of memset, should be D - dp NOT (D - 1 - dp)
    memset(child + loc.dp, 0, sizeof(Node*) * (D - loc.dp));

    if(loc.dc < D)
    {
        child[loc.dc] = loc.curr;
    }
}

//------------------------------------------------------------------------------
template<typename KeyType, uint8_t D, class Hash>
inline typename TxMdList<KeyType, D, Hash>::Coord 
TxMdList<KeyType, D, Hash>::Node::KeyToCoord(uint32_t key)
{
    const static uint32_t basis[32] = {0xffffffff, 0x10000, 0x800, 0x100, 0x80, 0x40, 0x20, 0x10, 
                                       0xC, 0xA, 0x8, 0x7, 0x6, 0x5, 0x5, 0x4,
                                       0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3,
                                       0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x2};

    Coord coord;
    uint32_t quotient = key;

    for (int i = D - 1; i >= 0 ; --i)
    {
        coord[i] = quotient % basis[D - 1];
        quotient /= basis[D - 1];
    }

    return coord;
}

//------------------------------------------------------------------------------
template<typename KeyType, uint8_t D, class Hash>
inline void TxMdList<KeyType, D, Hash>::Traverse(Node* n, Node* parent, int d, std::string& prefix) const
{
    printf("%s", (n->info == nullptr || n->info->desc->status == COMMITTED) ? "EXISTS":"      ");
    std::cout << prefix << "Node " << n << " [" << n->key << "]";
    std::cout << "{";
    for (uint8_t i : n->coord)
    {
        std::cout << (int)i << ", ";
    }
    std::cout << "}";
    std::cout << " DIM " << d << " of Parent " << parent << std::endl;

    n = ClearFlag(n);

    // traverse from last dimension up to current dim
    // The valid children include child nodes up to dim
    // e.g. a node on dimension 3 has only valid children on dimensions 3~8
    // for (int i = D - 1; i >= d; --i) 
    for (int i = d; i < D; i++)
    {
        Node* child = n->child[i];

        if(child != NULL)
        {
            prefix.push_back('|');
            prefix.insert(prefix.size(), i, ' ');

            Traverse(child, n, i, prefix);

            prefix.erase(prefix.size() - i - 1, i + 1);
        }
    }
}