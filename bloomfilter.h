#pragma once
#include "MurmurHash3.h"
#include <cstring>
#include <vector>
#include <memory>

class BloomFilter {
public:
    BloomFilter() = default;
    BloomFilter(uint32_t n, uint32_t min, uint32_t max, const std::vector<char> &bytes);
    void add(uint64_t key);
    bool contains(uint64_t key);
protected:
    friend class SkipList;
    uint32_t keyNum;
    uint32_t minKey;
    uint32_t maxKey;
    uint32_t hash[4];
    std::vector<char> bytes;
    static constexpr uint64_t kNumBits = 10240;
    static constexpr uint64_t kNumHashes = 4;
};