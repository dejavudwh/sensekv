#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "Arena.hpp"

namespace sensekv
{

static constexpr int kMaxHeight = 20;

class Arena;

// == uint64 value
using ValueBytePair = std::tuple<uint32_t, uint32_t>;

struct Node
{
    // value offset: 0-31 bit
    // value size  : 32-64 bit
    // for atomically loaded and stored
    std::atomic<uint64_t> value = 0;

    // immutable
    uint32_t keyOffset = 0;
    uint16_t keySize = 0;

    // height of node in skiplist
    uint16_t height = 0;

    std::atomic<uint32_t> tower[kMaxHeight] = {0};

    Node(std::unique_ptr<Arena> arena, std::vector<std::byte> key, struct Value val, int h);
    // Node's data memory is managed by arena
    ~Node() = default;

    uint64_t encodeValue(uint32_t valOffset, uint32_t valSize) const;
    ValueBytePair decodeValue(uint64_t val) const;

    ValueBytePair getValueOffset() const;
    void setValue(std::unique_ptr<Arena> arena, uint64_t newValue);
    std::shared_ptr<struct Value> getValue(std::unique_ptr<Arena> arena);

    std::vector<std::byte> key(std::unique_ptr<Arena> arena) const;
    uint32_t getNextOffset(int height) const;
    uint32_t casNextOffset(int height, uint32_t oldValue, uint32_t newValue);
};

struct Value
{
    std::byte meta = {};
    // value expire time
    uint64_t expiresAt = 0;
    std::byte* value = nullptr;
    // This field is not serialized. Only for internal usage.
    uint64_t version = 0;
    int size = 0;

    uint32_t encodeValue(std::vector<std::byte>& bytes);
    void decodeValude(std::vector<std::byte> bytes);

    uint32_t encodeSize();
    int sizeVarint(uint64_t x);
};

static constexpr int OffsetSize = sizeof(uint32_t);
// for 64-bit aligned
static constexpr int NodeAlign = sizeof(uint64_t) - 1;
static constexpr int kMaxNodeSize = sizeof(struct Node);

struct Entry
{
    std::byte* key;
    std::byte* value;
    uint64_t expiresAt;
};

}  // namespace sensekv