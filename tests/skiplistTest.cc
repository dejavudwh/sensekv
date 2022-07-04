#include <iostream>

#include "Arena.hpp"
#include "Entry.hpp"
#include "Skiplist.hpp"

using namespace sensekv;

int main()
{
    Skiplist sl(1000);
    std::vector<std::byte> key1 = {std::byte(1), std::byte(2), std::byte(3), std::byte(4)};
    std::vector<std::byte> value2 = {std::byte(1), std::byte(2), std::byte(3), std::byte(4)};
    ;
    struct Entry et1
    {
        .key = key1, .value = value2,
    };
    sl.add(et1);
    auto node = sl.search(key1);
    for (int i = 0; i < 4; i++)
    {
        unsigned char* cc = (unsigned char*)&node->value;
        printf("key: %x", *cc);
        cc += 8;
    }
}