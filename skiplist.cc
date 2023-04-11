#include "skiplist.h"

void SkipList::insert(uint64_t key, std::string val) {
    std::vector<std::shared_ptr<SkiplistNode>> update(MAX_LEVEL);
    auto x = head;
    for (int i = level; i >= 0; --i) {
        while (x->forward[i] != nullptr && x->forward[i]->key < key)
            x = x->forward[i];
        update[i] = x;
    }
    x = x->forward[0];
    if (x == nullptr || x->key != key) {
        int new_level = randomLevel();
        if (new_level > level) {
            for (int i = level + 1; i <= new_level; ++i)
                update[i] = head;
            level = new_level;
        }
        x = std::make_shared<SkiplistNode>(key, new_level);
        for (int i = 0; i <= new_level; ++i) {
            x->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = x;
        }
    }
}

std::string SkipList::search(uint64_t key) {
    auto x = head;
    for (int i = level; i >= 0; --i) {
        while (x->forward[i] != nullptr && x->forward[i]->key < key)
            x = x->forward[i];
    }
    x = x->forward[0];
    if (x != nullptr && x->key == key)
        return x->val;
    else
        return "";
}

int SkipList::randomLevel() {
    static std::mt19937 generator(std::random_device{}());
    static std::uniform_real_distribution<float> distribution(0, 1);
    int lvl = 1;
    while (distribution(generator) < p && lvl < MAX_LEVEL)
        lvl++;
    return lvl;
}