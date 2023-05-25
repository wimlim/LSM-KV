#include "memtable.h"

const int MemTable::getSize() const {
    return size;
}

const void MemTable::writeToDisk(const std::string& filename, uint64_t timeStamp, SSTable& table) const {
    //transform skiplist to bloom filter
    auto x = head->forward[0];
    uint64_t num = 0;
    uint64_t key;
    uint64_t minkey = 0xffffffffffffffff;
    uint64_t maxkey = 0;
    while (x) {
        key = x->key;
        num++;
        minkey = key < minkey ? key : minkey;
        maxkey = key > maxkey ? key : maxkey;
        table.add(key);
        x = x->forward[0];
    }
    table.keyNum = num;
    table.minKey = minkey;
    table.maxKey = maxkey;
    table.level = 0;
    table.timeStamp = timeStamp;
    table.id = timeStamp;
    // write header
    std::ofstream outfile(filename, std::ios::binary | std::ios::out | std::ios::trunc);
    if (!outfile.is_open()) {
        std::cerr << "Error : memTable open file " << filename << " failed" << std::endl;
        return;
    }
    outfile.write(reinterpret_cast<char*>(&timeStamp), sizeof(uint64_t));
    outfile.write(reinterpret_cast<char*>(&num), sizeof(uint64_t));
    outfile.write(reinterpret_cast<char*>(&minkey), sizeof(uint64_t));
    outfile.write(reinterpret_cast<char*>(&maxkey), sizeof(uint64_t));
    // write bloom filter
    outfile.write(reinterpret_cast<char*>(&table.bits), sizeof(table.bits));
    // write key and offset
    x = head->forward[0];
    uint32_t offset = 10272 + num * 12;
    // printf the pos of outfile
    while (x) {
        key = x->key;
        table.addKeySet(key, offset);
        outfile.write(reinterpret_cast<char*>(&key), sizeof(uint64_t));
        outfile.write(reinterpret_cast<char*>(&offset), sizeof(uint32_t));
        offset += x->val.size();
        x = x->forward[0];
    }
    // write value
    x = head->forward[0];
    while (x) {
        outfile.write(x->val.data(), x->val.size());
        x = x->forward[0];
    }
    outfile.close();
}

void MemTable::reset() {
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

void MemTable::ins(uint64_t key, const std::string& val) {
    std::vector<std::shared_ptr<SkiplistNode>> update(MAX_LEVEL);
    auto x = head;
    for (int i = level; i >= 0; i--) {
        while (x->forward[i] != nullptr && x->forward[i]->key < key)
            x = x->forward[i];
        update[i] = x;
    }
    x = x->forward[0];
    // already in table
    if (x != nullptr && x->key == key) {
        x->val = val;
        return;
    } 
    // not in table yet
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

const std::string MemTable::get(uint64_t key) const {
    auto x = head;
    for (int i = level; i >= 0; --i) {
        while (x->forward[i] != nullptr && x->forward[i]->key < key)
            x = x->forward[i];
    }
    x = x->forward[0];
    if (x != nullptr && x->key == key) {
        return x->val;
    }
    else {
        return "";
    }
}

uint32_t MemTable::randomLevel() {
    static std::mt19937 generator(std::random_device{}());
    static std::uniform_real_distribution<float> distribution(0, 1);
    int lvl = 1;
    while (distribution(generator) < p && lvl < MAX_LEVEL)
        lvl++;
    return lvl;
}