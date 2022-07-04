#include "Arena.hpp"

#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>

namespace sensekv
{

Arena::~Arena()
{
    // TODO
    // delete buf;
}

std::shared_ptr<Arena> Arena::newArena(int64_t n)
{
    Arena* arena = new Arena();
    return std::shared_ptr<Arena>(arena);
}

uint32_t Arena::allocate(uint32_t sz)
{
    uint32_t offset = n;
    n.fetch_add(sz);

    if (!shouldGrow)
    {
        assert(n < len);
        return n;
    }

    if (kMaxNodeSize > len || n > len - kMaxNodeSize)
    {
        uint32_t growBy = len;
        if (growBy > (1 << 30))
        {
            growBy = (1 << 30);
        }
        if (growBy < sz)
        {
            growBy = sz;
        }
        std::byte* newBuf = reinterpret_cast<std::byte*>(::malloc(growBy + len));
        ::memcpy(newBuf, buf, offset);
        delete buf;
        buf = newBuf;
        len += growBy;
    }

    return n;
}

int64_t Arena::size() const { return n.load(); }

uint32_t Arena::putNode(int height)
{
    // Data has not been initialized, just allocation of space, the initialization performed by new node
    uint32_t unusedSize = (kMaxHeight - height) * OffsetSize;
    // Compute size of node
    uint32_t n = kMaxNodeSize - unusedSize + NodeAlign;
    n = allocate(n);

    // memeory order
    return (n + NodeAlign) & (~NodeAlign);
}
uint32_t Arena::putVal(struct Value value)
{
    uint32_t n = value.encodeSize();
    uint32_t offset = allocate(n);
    value.encodeValue(buf + offset);
    return offset;
}
uint32_t Arena::putKey(std::vector<std::byte> key)
{
    uint32_t keySz = key.size();
    uint32_t offset = allocate(keySz);
    ::memcpy(buf + offset, &key[0], keySz);
    return offset;
}

struct Node* Arena::getNode(uint32_t offset) const
{
    // tower[n] == 0
    if (offset == 0)
    {
        return nullptr;
    }
    // The space allocated to the node
    struct Node* node = reinterpret_cast<struct Node*>(&buf[offset]);
    return node;
}
std::vector<std::byte> Arena::getKey(uint32_t offset, uint16_t size) const
{
    return std::vector<std::byte>{buf + offset, buf + offset + size};
}
struct Value* Arena::getVal(uint32_t offset, uint32_t size) const
{
    struct Value* value = new Value();
    value->decodeValude(buf + offset, size);

    return value;
}
uint32_t Arena::getNodeOffset(struct Node* node) const
{
    if (node == nullptr)
    {
        return 0;
    }
    // node addr is allocated by arena
    uint32_t offset = reinterpret_cast<std::byte*>(node) - buf;
    return offset;
}

}  // namespace sensekv