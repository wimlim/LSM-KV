#pragma once
#include "MurmurHash3.h"
#include <cstring>
#include <vector>
#include <memory>

class BloomFilter {
public:
    BloomFilter();
    void add(uint64_t key);
    bool contains(uint64_t key);
private:
    uint32_t hash[4];
    std::vector<char> bytes;
    static constexpr uint64_t kNumBits = 10240;
    static constexpr uint64_t kNumHashes = 4;
};