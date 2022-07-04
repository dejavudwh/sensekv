#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "Arena.hpp"
#include "Entry.hpp"

namespace sensekv
{
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