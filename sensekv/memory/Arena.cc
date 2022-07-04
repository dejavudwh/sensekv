#include "Arena.hpp"

#include <algorithm>
#include <cstdint>

namespace sensekv
{

Arena::~Arena() { delete buf; }

std::unique_ptr<Arena> Arena::newArena(int64_t n)
{
    Arena* arena = new Arena();
    return std::unique_ptr<Arena>(arena);
}

uint32_t Arena::allocate(uint32_t sz)
{
    uint32_t offset = n.fetch_add(sz);
    if (!shouldGrow)
    {
        assert(offset < len);
        return offset;
    }

    if (offset > len - kMaxNodeSize)
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
        std::byte* newBuf = reinterpret_cast<std::byte*>(::malloc(offset - len));
        ::memcpy(newBuf, buf, len);
        buf = newBuf;
        len += growBy;
    }

    return offset - sz;
}

int64_t Arena::size() const 
{
    return n.load();
}

uint32_t Arena::putNode(int height) 
{
    uint32_t unusedSize = (kMaxNodeSize - height) * OffsetSize;
    uint32_t n = kMaxNodeSize - unusedSize + NodeAlign;
    n = allocate(n);

    // memeory order
    return (n + NodeAlign) & (~NodeAlign);
}

uint32_t Arena::putVal(struct Value value) {}

uint32_t Arena::putKey(std::byte bytes[]) {}

}  // namespace sensekv