#pragma once
#include "MurmurHash3.h"
#include <bitset>
#include <vector>
#include <memory>
#include <iostream>
#include <fstream>


class SSTable {
public:
    SSTable() : bits() {}

    SSTable(uint32_t l, uint32_t n, uint32_t min, uint32_t max, const std::vector<char> &bytes);

    void add(uint64_t key);

    void addKeySet(uint64_t key, uint32_t offset);
    void addKeySet(const char* buffer, uint64_t len);

    bool contains(uint64_t key);

    std::string get(uint64_t key);
protected:
    friend class MemTable;
    uint32_t level;
    uint32_t timeStamp;
    uint32_t keyNum;
    uint32_t minKey;
    uint32_t maxKey;
    uint32_t hash[4];
    std::bitset<10240 * 8> bits;
    std::vector<std::pair<uint64_t, uint32_t>> index;
    static std::string path;
    static constexpr uint64_t kNumBits = 10240 * 8;
};