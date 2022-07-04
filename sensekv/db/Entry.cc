#include "Entry.hpp"

#include <array>
#include <cstdint>
#include <cstring>
#include <memory>

namespace sensekv
{
// ========== struct Node
std::shared_ptr<struct Node> Node::newNode(std::shared_ptr<Arena> arena,
                                           std::vector<std::byte> key,
                                           struct Value val,
                                           int h)
{
    uint32_t nodeOffset = arena->putNode(h);
    uint32_t keyOffset = arena->putKey(key);
    uint32_t keySize = key.size();
    uint32_t height = static_cast<uint16_t>(h);

    std::shared_ptr<Node> node = arena->getNode(nodeOffset);
    node->keyOffset = keyOffset;
    node->keySize = key.size();
    node->value = Node::encodeValue(arena->putVal(val), val.encodeSize());
    node->height = height;

    return node;
}

uint64_t Node::encodeValue(uint32_t valOffset, uint32_t valSize)
{
    return static_cast<uint64_t>(valOffset) << 32 | static_cast<uint64_t>(valSize);
}
ValueBytePair Node::decodeValue(uint64_t val)
{
    return ValueBytePair{static_cast<uint32_t>(val), static_cast<uint32_t>(val >> 32)};
}

ValueBytePair Node::getValueOffset() const
{
    // cas
    return Node::decodeValue(value.load());
}
void Node::setValue(std::shared_ptr<Arena> arena, uint64_t newValue)
{
    value.store(newValue);
    // The new value is already in memory in the skiplist add function
    // arena->putVal(struct Value value)
}
std::shared_ptr<struct Value> Node::getValue(std::unique_ptr<Arena> arena)
{
    ValueBytePair vbp = getValueOffset();
    return arena->getVal(std::get<0>(vbp), std::get<1>(vbp));
}

std::vector<std::byte> Node::key(std::shared_ptr<Arena> arena) const { return arena->getKey(keyOffset, keySize); }
uint32_t Node::getNextOffset(int height) const { return tower[height].load(); }
bool Node::casNextOffset(int height, uint32_t oldValue, uint32_t newValue)
{
    return tower[height].compare_exchange_strong(oldValue, newValue);
}

// ========= struct Value
uint32_t Value::encodeValue(std::byte bytes[])
{
    bytes[0] = meta;
    uint64_t ea = expiresAt;
    int k = 0;
    for (int i = sizeof(uint64_t); i >= 1; i--)
    {
        // encode value part
        uint8_t bit = ea >> (64 - 8 * k);
        k++;
        bytes[i] = std::byte{bit};
    }
    if (value != nullptr)
    {
        ::memcpy(&bytes[sizeof(uint64_t) + 1], value, size);
    }

    // return length of encode
    return sizeof(uint64_t) + 1 + size;
}
void Value::decodeValude(std::byte bytes[], int sz)
{
    struct Value val = {};
    val.meta = bytes[0];
    // decode uint64
    uint64_t* a = reinterpret_cast<uint64_t*>(&bytes[1]);
    expiresAt = *a;
    // start 9
    int len = sizeof(uint64_t) + 1;
    value = &bytes[len];
    size = sz - len;
}

uint32_t Value::encodeSize()
{
    // sizeof meta == 1
    return 1 + sizeof(uint64_t) + size;
}
int Value::sizeVarint(uint64_t x)
{
    // The number of significant digits can be preserved only uint64_t here, but in order to facilitate temporarily
    // counted eight directly
    return sizeof(uint64_t);
}

}  // namespace sensekv