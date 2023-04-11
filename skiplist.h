#pragma once

#include <cmath>
#include <random>
#include <memory>
#include <vector>

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

class SkipList {
public:
    //iniaitlize
    SkipList() : level(0), size(10272), head(new SkiplistNode(0, "")) {}
    void reset();
    bool ins(uint64_t key, const std::string& val);
    bool del(uint64_t key);
    const std::string get(uint64_t key) const;
    const int getSize() const;
private:
    int level;
    int size;
    std::shared_ptr<SkiplistNode> head;
    static constexpr float p = 1 / 2.71828182845904523536;
    static constexpr int MAX_LEVEL = 32;
    static constexpr int MAX_SIZE = 2 * 1024 * 1024;
    int randomLevel();
};