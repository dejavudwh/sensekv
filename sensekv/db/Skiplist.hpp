#pragma once

#include <assert.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <tuple>
#include <vector>

#include "Arena.hpp"
#include "Entry.hpp"

namespace sensekv
{
class Skiplist final
{
public:
    Skiplist(int64_t arenaSize);

    void add(struct Entry entry);
    struct Value* search(std::vector<std::byte> key);
    bool empty();

private:
    using NodeOffsetPair = std::tuple<uint32_t, uint32_t>;

    struct Node* getNext(struct Node* node, int height) const;
    struct Node* getHead() const;

    NodeOffsetPair findSpliceForLevel(std::vector<std::byte> bytes, uint32_t before, int level);

    std::tuple<struct Node*, bool> findNear(std::vector<std::byte> key, bool less, bool allowEqual) const;
    std::tuple<uint32_t, uint32_t> findSpliceForLevel(std::vector<std::byte> key, uint32_t before, int level) const;
    struct Node* findLast();

    int getHeight() const;

    int randomHeight();

private:
    std::shared_ptr<Arena> arena = nullptr;
    std::atomic<int32_t> height = 0;
    // in arena
    uint32_t headOffset = 0;
};

static int CompareKey(std::vector<std::byte> key1, std::vector<std::byte> key2);

}  // namespace sensekv