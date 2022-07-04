#pragma once

#include <Arena.hpp>
#include <cstddef>
#include <cstdint>
#include <vector>
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
    uint64_t value = 0;

    // immutable
    uint32_t keyOffset = 0;
    uint16_t keySize = 0;

    // height of node in skiplist
    uint16_t height = 0;

    uint32_t tower[kMaxHeight] = {0};

    Node(std::unique_ptr<Arena> arena, std::vector<std::byte> key, struct Value, int height);
    // PodNode's memory is managed by arena
    ~Node();

    uint64_t encodeValue();
    ValueBytePair decodeValue();

    ValueBytePair getValueOffset() const;
    void setValue(std::unique_ptr<Arena> arena, uint64_t newValue);
    std::shared_ptr<struct Value> getValue(std::unique_ptr<Arena> arena);

    std::vector<std::byte> key(std::unique_ptr<Arena>) const;
    uint32_t getNextOffset(int height) const;
    uint32_t casNextOffset(int height, uint32_t oldValue, uint32_t newValue);
};

struct Value
{
    std::byte meta = {};
    std::byte* value = nullptr;
    // value expire time
    uint64_t expiresAt = 0;
    // This field is not serialized. Only for internal usage.
    uint64_t version = 0;

    uint32_t encodeValue(std::vector<std::byte> bytes);
    void decodeValude(std::vector<std::byte> bytes);

    uint32_t encodeSize();
    int sizeVarint(uint64_t x);
};

struct Entry
{
    std::byte* key;
    std::byte* value;
    uint64_t expiresAt;
};

}  // namespace sensekv