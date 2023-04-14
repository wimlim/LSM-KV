#include "skiplist.h"

const int SkipList::getSize() const {
    return size;
}

void SkipList::reset() {
    auto x = head->forward[0];
    while (x != nullptr) {
        auto tmp = x->forward[0];
        x.reset();
        x = tmp;
    }
    size = 0;
    level = 0;
    for (int i = 0; i < MAX_LEVEL; i++)
        head->forward[i].reset();
}

bool SkipList::ins(uint64_t key, const std::string& val) {
    std::vector<std::shared_ptr<SkiplistNode>> update(MAX_LEVEL);
    auto x = head;
    for (int i = level; i >= 0; i--) {
        while (x->forward[i] != nullptr && x->forward[i]->key < key)
            x = x->forward[i];
        update[i] = x;
    }
    x = x->forward[0];
    //already in table
    if (x != nullptr && x->key == key) {
        if (size + 12 + val.size() - x->val.size() > MAX_SIZE)
            return false;
        x->val = val;
        return true;
    } 
    // not in table yet
    if (size + 12 + val.size() > MAX_SIZE)
        return false;
    int new_level = randomLevel();
    if (new_level > level) {
        for (int i = level + 1; i <= new_level; i++)
            update[i] = head;
        level = new_level;
    }
    auto new_node = std::make_shared<SkiplistNode>(key, val);
    for (int i = 0; i <= new_level; i++) {
        new_node->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = new_node;
    }
    size += 12 + val.size();
}

bool SkipList::del(uint64_t key) {
    std::vector<std::shared_ptr<SkiplistNode>> update(MAX_LEVEL);
    auto x = head;
    for (int i = level; i >= 0; i--) {
        while (x->forward[i] != nullptr && x->forward[i]->key < key)
            x = x->forward[i];
        update[i] = x;
    }
    x = x->forward[0];
    if (x != nullptr && x->key == key && x->val != "~DELETE~") {
        x->val = "~DELETE~";
        return true;
    } 
    return false;
}

const std::string SkipList::get(uint64_t key) const {
    auto x = head;
    for (int i = level; i >= 0; --i) {
        while (x->forward[i] != nullptr && x->forward[i]->key < key)
            x = x->forward[i];
    }
    x = x->forward[0];
    if (x != nullptr && x->key == key && x->val != "~DELETE~")
        return x->val;
    else
        return empty_string;
}

int SkipList::randomLevel() {
    static std::mt19937 generator(std::random_device{}());
    static std::uniform_real_distribution<float> distribution(0, 1);
    int lvl = 1;
    while (distribution(generator) < p && lvl < MAX_LEVEL)
        lvl++;
    return lvl;
}