#include "Entry.hpp"

#include <array>
#include <cstdint>
#include <cstring>
#include <memory>

namespace sensekv
{
// ========== struct Node
Node::Node(std::unique_ptr<Arena> arena, std::vector<std::byte> key, struct Value val, int h)
{
    uint32_t nodeOffset = arena->putNode(height);
    keyOffset = arena->putKey(key);
    keySize = key.size();
    value = encodeValue(arena->putVal(val), val.encodeSize());
    height = static_cast<uint16_t>(h);
}

uint64_t Node::encodeValue(uint32_t valOffset, uint32_t valSize) const
{
    return static_cast<uint64_t>(valOffset) << 32 | static_cast<uint64_t>(valSize);
}
ValueBytePair Node::decodeValue(uint64_t val) const
{
    return ValueBytePair { static_cast<uint32_t>(value), static_cast<uint32_t>(value >> 32) }
}

ValueBytePair Node::getValueOffset() const
{
    // cas
    return decodeValue(value.load());
}
void Node::setValue(std::unique_ptr<Arena> arena, uint64_t newValue)
{
    value.store(newValue);
    // arena->putVal(struct Value value)
}
std::shared_ptr<struct Value> Node::getValue(std::unique_ptr<Arena> arena)
{
    ValueBytePair vbp = getValueOffset();
    return arena->getVal(std::get<0>(vbp), std::get<1>(vbp));
}

std::vector<std::byte> Node::key(std::unique_ptr<Arena> arena) const { return arena->getKey(keyOffset, keySize); }
uint32_t Node::getNextOffset(int height) const { return tower[height].load(); }
uint32_t Node::casNextOffset(int height, uint32_t oldValue, uint32_t newValue)
{
    tower[height].compare_exchange_strong(oldValue, newValue);
}

// ========= struct Value
uint32_t Value::encodeValue(std::vector<std::byte>& bytes)
{
    bytes.resize(sizeof(uint64_t) + 1 + size);
    bytes[0] = meta;
    uint64_t ea = expiresAt;
    int k = 0;
    for (int i = sizeof(uint64_t); i >= 1; i--)
    {
        uint8_t bit = ea >> (64 - 8 * k);
        k++;
        bytes[i] = std::byte{bit};
    }
    ::memcpy(&bytes[sizeof(uint64_t) + 1], value, size);
}
void Value::decodeValude(std::vector<std::byte> bytes)
{
    struct Value val = {};
    val.meta = bytes[0];
    // decode uint64
    uint64_t* a = reinterpret_cast<uint64_t*>(&bytes[1]);
    expiresAt = *a;
    int len = sizeof(uint64_t) + 1;
    value = &bytes[len];
    size = bytes.size() - len;
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