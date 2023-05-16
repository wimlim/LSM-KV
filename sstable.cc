#include "sstable.h"

SSTable::SSTable(uint32_t l, uint32_t n, uint32_t min, uint32_t max, const std::vector<char> &b) :
            bits(), level(l), keyNum(n), minKey(min), maxKey(max) {
    for (uint32_t i = 0; i < b.size(); i++) {
        for (int j = 0; j < 8; j++) {
            if (b[i] & (1 << j)) {
                bits.set(i * 8 + j);
            }
        }
    }
}

void SSTable::add(uint64_t key) {
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    for (int i = 0; i < 4; i++) {
        hash[i] %= bits.size();
        bits.set(hash[i]);
    }
}

bool SSTable::contains(uint64_t key) {
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    for (int i = 0; i < 4; i++) {
        hash[i] %= bits.size();
        if (!bits.test(hash[i])) {
            return false;
        }
    }
    return true;
}

void SSTable::addKeySet(uint64_t key, uint32_t offset) {
    index.push_back(std::make_pair(key, offset));
}

void SSTable::addKeySet(const char* buffer, uint64_t len) {
    len = len * 12;
    uint64_t key;
    uint32_t offset;
    for (int i = 0; i < len; i += 12) {
        memcpy(&key, buffer + i, 8);
        memcpy(&offset, buffer + i + 8, 4);
        index.push_back(std::make_pair(key, offset));
    }
}

std::string SSTable::get(uint64_t key) {
    // divide the keys and find the offset
    uint32_t l = 0, r = index.size(), mid = (l + r) >> 1;
    uint32_t offset;
    while (l < r) {
        if (index[mid].first == key) {
            offset = index[mid].second;
            break;
        } else if (index[mid].first < key) {
            l = mid + 1;
        } else {
            r = mid;
        }
        mid = (l + r) >> 1;
    }
    if (l == r) {
        return "";
    }
    // read the value
    std::string value;
    std::ifstream infile(path + std::to_string(level) + "-" + std::to_string(timeStamp) + ".sst", std::ios::binary);
    if (!infile) {
        std::cerr << "open file error" << std::endl;
        return "";
    }
    infile.seekg(offset);
    infile.read((char*)&value, index[mid + 1].second - offset);
    infile.close();
    return value;
}