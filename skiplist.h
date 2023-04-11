#include <cmath>
#include <random>
#include <memory>
#include <vector>

class SkiplistNode {
public:
    uint64_t key;
    std::string val;
    std::vector<std::shared_ptr<SkiplistNode>> forward;
    SkiplistNode(uint64_t key, std::string val = "", int level = MAX_LEVEL) : 
        key(key), val(val), forward(level) {}
private:
    static constexpr int MAX_LEVEL = 32;
};

class SkipList {
public:
    //iniaitlize
    SkipList() : level(0), head(new SkiplistNode(-1)) {}
    void insert(uint64_t key, std::string val);
    std::string search(uint64_t key);
private:
    int level;
    std::shared_ptr<SkiplistNode> head;
    static constexpr float p = 1 / M_E;
    static constexpr int MAX_LEVEL = 32;
    int randomLevel();
};