#include <iostream>

#include "Arena.hpp"

using namespace sensekv;
// class Arena : public Noncopyable
// {
// public:
//     ~Arena();

//     std::unique_ptr<Arena> newArena(int64_t n);

//     uint32_t allocate(uint32_t sz);

//     int64_t size() const;

//     uint32_t putNode(int height);
//     uint32_t putVal(struct Value value);
//     uint32_t putKey(std::vector<std::byte> key);

//     std::shared_ptr<struct Node> getNode(uint32_t offset) const;
//     std::vector<std::byte> getKey(uint32_t offset, uint16_t size) const;
//     std::shared_ptr<struct Value> getVal(uint32_t offset, uint32_t size) const;
//     uint32_t getNodeOffset(std::shared_ptr<struct Node> node) const;

// private:
//     Arena();

// private:
//     std::atomic<uint32_t> n = 0;
//     bool shouldGrow = false;
//     // the total length of buf
//     uint32_t len = 0;
//     std::byte* buf = nullptr;
// };

int main()
{
    auto arena = Arena::newArena(1000);
    std::vector<std::byte> key;
    for (int i = 0; i < 8; i++)
    {
        key.push_back(std::byte(i));
    }
    auto offset = arena->putKey(key);
    key = arena->getKey(offset, 8);
    for (auto k : key)
    {
        unsigned char* cc = (unsigned char*)&k;
        printf("key: %x", *cc);
    }
}