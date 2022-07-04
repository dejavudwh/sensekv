#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <vector>

int main()
{
    // std::byte b{12};
    // std::vector<int> i{1, 2, 3, 4, 5, 6, 7};
    std::vector<std::byte> vb;
    for (int i = 0; i < 10; i++)
    {
        vb.push_back(std::byte{3});
    }

    uint64_t* a = reinterpret_cast<uint64_t*>(&vb[0]);
    std::cout << *a;
}