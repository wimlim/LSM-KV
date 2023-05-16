#pragma once
#include <cmath>
#include <random>
#include <memory>
#include <vector>
#include <fstream>
#include <iostream>
#include "sstable.h"

class SkiplistNode {
public:
    uint64_t key;
    std::string val;
    std::vector<std::shared_ptr<SkiplistNode>> forward;
    SkiplistNode(uint64_t key, const std::string& val) : 
        key(key), val(val), forward(MAX_LEVEL, nullptr) {}
private:
    static constexpr int MAX_LEVEL = 32;
};

class MemTable {
public:
    //iniaitlize
    MemTable() : level(0), size(10272), head(new SkiplistNode(0, "")) {}

    void reset();

    void ins(uint64_t key, const std::string& val);

    const std::string get(uint64_t key) const;

    const int getSize() const;

    const void writeToDisk(const std::string& filename, uint64_t timeStamp, SSTable& filter) const;
private:
    uint16_t level;
    uint64_t size;
    std::shared_ptr<SkiplistNode> head;
    static constexpr float p = 1 / 2.71828182845904523536;
    static constexpr uint32_t MAX_LEVEL = 32;
    static constexpr uint32_t MAX_SIZE = 2 * 1024 * 1024;
    uint32_t randomLevel();
};