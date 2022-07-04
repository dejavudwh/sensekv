#include "Skiplist.hpp"

#include <cstdint>
#include <random>

#include "Arena.hpp"
#include "Entry.hpp"

namespace sensekv
{

// if key1 > key2 return 1
// if key2 > key1 return -1
// if == return 0
int CompareKey(std::vector<std::byte> key1, std::vector<std::byte> key2)
{
    int len = std::min(key1.size(), key2.size());
    for (int i = 0; i < len; i++)
    {
        if (key1[i] < key2[i])
        {
            return -1;
        }
        else if (key1[i] > key2[i])
        {
            return 1;
        }
    }

    int ret = key1.size() > len ? 1 : 0;
    ret = key2.size() > len ? -1 : 0;
    return ret;
}

Skiplist::Skiplist(int64_t arenaSize)
{
    arena = Arena::newArena(arenaSize);
    auto head = Node::newNode(arena, {}, Value{}, kMaxHeight);
    headOffset = arena->getNodeOffset(head);
    height = 1;
}

void Skiplist::add(struct Entry entry)
{
    std::vector<std::byte> key = entry.key;
    struct Value val
    {
        .meta = entry.meta, .expiresAt = entry.expiresAt, .value = &entry.value[0],
    };

    int listHeight = getHeight();
    uint32_t prev[kMaxHeight + 1] = {0};
    uint32_t next[kMaxHeight + 1] = {0};
    prev[listHeight] = headOffset;
    for (int i = listHeight - 1; i >= 0; i--)
    {
        // Start from the node already found in the previous level
        NodeOffsetPair kp = findSpliceForLevel(key, prev[i + 1], i);
        prev[i] = std::get<0>(kp);
        next[i] = std::get<1>(kp);
        // just update
        if (prev[i] == next[i])
        {
            uint32_t v = arena->putVal(val);
            uint64_t encVal = Node::encodeValue(v, val.encodeSize());
            auto prevNode = arena->getNode(prev[i]);
            prevNode->setValue(arena, encVal);
            return;
        }
    }

    // need to create a new node
    int h = randomHeight();
    auto newNode = Node::newNode(arena, key, val, h);
    // update height via cas
    while (h > listHeight)
    {
        if (height.compare_exchange_strong(listHeight, h))
        {
            // Successfully increased skiplist‘s height.
            break;
        }
        // There may be other threads that modify the finish
        listHeight = getHeight();
    }

    // Insert from bottom to top according to the prev and next arrays obtained earlier
    for (int i = 0; i < h; i++)
    {
        for (;;)
        {
            // prev[i] == 0
            // If the new height at this point is higher than the original height, then prev[i] may be zero
            if (arena->getNode(prev[i]) == nullptr)
            {
                // must from headOffset
                NodeOffsetPair kp = findSpliceForLevel(key, headOffset, i);
                prev[i] = std::get<0>(kp);
                next[i] = std::get<1>(kp);
            }
            newNode->tower[i] = next[i];
            auto pnode = arena->getNode(prev[i]);
            if (pnode->casNextOffset(i, next[i], arena->getNodeOffset(newNode)))
            {
                // cas ok
                break;
            }
            // cas failed, Concurrency has occurred, with new values
            NodeOffsetPair kp = findSpliceForLevel(key, prev[i], i);
            prev[i] = std::get<0>(kp);
            next[i] = std::get<1>(kp);
            if (prev[i] == next[i])
            {
                // Equality can happen only on base level, Only two threads can be modifying level 0 at the same time,
                // if it was any other level, it would have been found to be equal when the prev and next arrays were
                // obtained earlier and could not have run here
                assert(i == 0);
            }
        }
    }
}
std::shared_ptr<struct Value> Skiplist::search(std::vector<std::byte> key)
{
    auto node = std::get<0>(findNear(key, false, true));
    if (node == nullptr)
    {
        return {};
    }

    std::vector<std::byte> nextKey = arena->getKey(node->keyOffset, node->keySize);
    if (CompareKey(key, nextKey) != 0)
    {
        return {};
    }

    ValueBytePair vbp = node->getValueOffset();
    return arena->getVal(std::get<0>(vbp), std::get<1>(vbp));
}
bool Skiplist::empty() { return findLast() == nullptr; }

std::shared_ptr<Node> Skiplist::getNext(std::shared_ptr<Node> node, int height) const
{
    return arena->getNode(node->getNextOffset(height));
}
std::shared_ptr<Node> Skiplist::getHead() const { return arena->getNode(headOffset); }

// The input "before" tells us where to start looking.
Skiplist::NodeOffsetPair Skiplist::findSpliceForLevel(std::vector<std::byte> key, uint32_t before, int level)
{
    for (;;)
    {
        auto beforeNode = arena->getNode(before);
        uint32_t next = beforeNode->getNextOffset(level);
        auto nextNode = arena->getNode(next);
        if (nextNode == nullptr)
        {
            // the last one
            return {before, next};
        }
        std::vector<std::byte> nextKey = nextNode->key(arena);
        int cmp = CompareKey(key, nextKey);
        if (cmp == 0)
        {
            return {next, next};
        }
        else if (cmp < 0)
        {
            // before.key < key < next.key. We are done for this level.
            return {before, next};
        }
        before = next;
    }
}

// TODO Refactoring, feature splitting
// findNear finds the node near to key.
// If less=true, it finds rightmost node such that node.key < key (if allowEqual=false) or
// node.key <= key (if allowEqual=true).
// If less=false, it finds leftmost node such that node.key > key (if allowEqual=false) or
// node.key >= key (if allowEqual=true).
// Returns the node found. The bool returned is true if the node has key equal to given key.
std::tuple<std::shared_ptr<Node>, bool> Skiplist::findNear(std::vector<std::byte> key, bool less, bool allowEqual) const
{
    auto node = getHead();
    int level = getHeight() - 1;
    for (;;)
    {
        // Assume x.key < key.
        auto next = getNext(node, level);
        if (next == nullptr)
        {
            // x.key < key < END OF LIST
            if (level > 0)
            {
                level--;
                continue;
            }
            // Level=0. Cannot descend further. Let's return something that makes sense.
            // Try to return node. Make sure it is not a head node.
            if (!less || node == getHead())
            {
                return {nullptr, false};
            }
            else
            {
                return {node, false};
            }
        }

        std::vector<std::byte> nextKey = next->key(arena);
        int cmp = CompareKey(key, nextKey);
        if (cmp > 0)
        {
            // x.key < next.key < key. We can continue to move right.
            node = next;
            continue;
        }
        if (cmp == 0)
        {
            if (allowEqual)
            {
                return {next, true};
            }
            if (!less)
            {
                // We want >, so go to base level to grab the next bigger note.
                // not exist repeated value
                return {getNext(next, 0), false};
            }
            // We want <. If not base level, we should go closer in the next level.
            if (level > 0)
            {
                level--;
                continue;
            }
            // On base level. Return x.
            if (node == getHead())
            {
                return {nullptr, false};
            }
            return {node, false};
        }

        // cmp < 0. In other words, x.key < key < next.
        if (level > 0)
        {
            level--;
            continue;
        }
        // At base level. Need to return something.
        if (!less)
        {
            return {next, false};
        }
        // Try to return node. Make sure it is not a head node.
        if (node == getHead())
        {
            return {nullptr, false};
        }
        return {node, false};
    }
}

std::shared_ptr<Node> Skiplist::findLast()
{
    auto node = getHead();
    int level = getHeight() - 1;
    for (;;)
    {
        auto next = getNext(node, level);
        if (next != nullptr)
        {
            node = next;
            continue;
        }
        if (level == 0)
        {
            if (node == getHead())
            {
                return nullptr;
            }
            return node;
        }
        level--;
    }
}

int Skiplist::getHeight() const { return height.load(); }

int Skiplist::randomHeight()
{
    int seed = time(0);
    static std::default_random_engine tmp(seed);

    int h = 1;
    while (tmp() % 100 / 100.0000 < 0.5)
    {
        h++;
    }

    return h;
}

}  // namespace sensekv