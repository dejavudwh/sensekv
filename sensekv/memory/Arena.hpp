#pragma once

#include <cstdint>
#include <memory>

#include "KVStruct.hpp"
#include "Noncopyable.hpp"

namespace sensekv
{

using byte = unsigned char;

static constexpr int OffsetSize = sizeof(uint32_t);
// for 64-bit aligned
static constexpr int NodeAlign = sizeof(uint64_t) - 1;
static constexpr int MaxNodeSize = sizeof(struct PodNode);

class Arena : public Noncopyable
{
public:
    Arena() = delete;
    ~Arena();

    std::unique_ptr<Arena> newArena(int64_t n);

    uint32_t allocate(uint32_t sz);
    int64_t size() const;

    uint32_t putNode(int height);
    uint32_t putVal(struct Value value);
    uint32_t putKey(std::byte bytes[]);

private:
    uint32_t n = 0;
    bool shouldGrow = false;
    std::byte* buf = nullptr;
};

}  // namespace sensekv