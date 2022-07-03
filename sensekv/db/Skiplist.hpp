#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "Arena.hpp"
#include "KVStruct.hpp"

namespace sensekv
{

class Node
{
public:
    Node(std::unique_ptr<Arena> arena, std::vector<std::byte> key, struct Value, int height);
    // PodNode's memory is managed by arena
    ~Node();

    std::tuple<uint32_t, uint32_t> getValueOffset() const;
    void setValue(std::unique_ptr<Arena> arena, uint64_t newValue);

    std::vector<std::byte> key(std::unique_ptr<Arena>) const;
    uint32_t getNextOffset(int height) const;
    uint32_t casNextOffset(int height, uint32_t oldValue, uint32_t newValue);

private:
    struct PodNode* podNode = nullptr;
};

class Skiplist final
{
public:
    Skiplist();

    void add(struct Entry entry);
    struct Value search(std::vector<std::byte> key);

private:
    std::shared_ptr<Node> getNext(std::shared_ptr<Node> node, int height) const;
    std::shared_ptr<Node> getHead() const;

    std::shared_ptr<Node> findNear(std::vector<std::byte> key, bool less, bool allowEqual) const;
    std::tuple<uint32_t, uint32_t> findSpliceForLevel(std::vector<std::byte> key, uint32_t before, int level) const;
    std::shared_ptr<Node> findLast();

    int getHeight() const;

    int randomHeight();

private:
    std::unique_ptr<Arena> arena = nullptr;
    int32_t height = 0;
    // in arena
    uint32_t headOffset = 0;
};

}  // namespace sensekv