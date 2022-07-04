#pragma once

#include <string.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>

#include "Entry.hpp"
#include "Noncopyable.hpp"

namespace sensekv
{

class Arena : public Noncopyable
{
public:
    ~Arena();

    std::unique_ptr<Arena> newArena(int64_t n);

    uint32_t allocate(uint32_t sz);
    int64_t size() const;

    uint32_t putNode(int height);
    uint32_t putVal(struct Value value);
    uint32_t putKey(std::vector<std::byte> key);

    std::shared_ptr<struct Node> getNode(uint32_t offset) const;
    std::vector<std::byte> getKey(uint32_t offset, uint16_t size) const;
    std::shared_ptr<struct Value> getVal(uint32_t offset, uint32_t size) const;
    uint32_t getNodeOffset(std::shared_ptr<struct Value> val) const;

private:
    Arena();

private:
    std::atomic<uint32_t> n = 0;
    bool shouldGrow = false;
    // the total length of buf
    uint32_t len = 0;
    std::byte* buf = nullptr;
};

}  // namespace sensekv