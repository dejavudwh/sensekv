#pragma once

#include <cstddef>
#include <cstdint>

namespace sensekv
{

static constexpr int kMaxHeight = 20;

struct PodNode
{
    // value offset: 0-31 bit
    // value size  : 32-64 bit
    // for atomically loaded and stored
    uint64_t value = 0;

    // immutable
    uint32_t keyOffset = 0;
    uint16_t keySize = 0;

    // height of node in skiplist
    uint16_t height = 0;

    uint32_t tower[kMaxHeight] = {};
};

struct Value 
{
    std::byte* value = nullptr;
    // value expire time
    uint64_t expiresAt = 0;
};

struct Entry
{
    std::byte* key;
    std::byte* value;
    uint64_t expiresAt;
};

}  // namespace sensekv