#include "sstable.h"

SSTable::SSTable(std::string path, uint32_t i, uint32_t t, uint32_t n, uint32_t min, uint32_t max, const std::vector<char> &b) :
            index(), bits(), pathname(path), id(i), timeStamp(t), keyNum(n), minKey(min), maxKey(max) {
    for (uint32_t i = 0; i < 1280; i++) {
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
    std::ifstream infile(pathname, std::ios::binary);
    if (!infile) {
        std::cerr << "ssTable open file error" << std::endl;
        return "";
    }
    std::string value;
    infile.seekg(offset);
    if (mid == (index.size() - 1)) {
        infile >> value;
    }
    else {
        uint32_t len = index[mid + 1].second - offset;
        char buffer[len];
        infile.read(buffer, len);
        value = std::string(buffer, len);
    }
    infile.close();
    return value;
}

void SSTable::getAll(std::vector<std::pair<uint64_t, std::string>> &set) {
    std::ifstream infile(pathname, std::ios::binary);
    if (!infile) {
        std::cerr << "ssTable open file error" << std::endl;
        return;
    }
    uint32_t offset = index[0].second;
    uint32_t len;
    infile.seekg(offset);
    for (int i = 0; i < index.size(); i++) {
        if (i == (index.size() - 1)) {
            std::string value;
            infile >> value;
            set.push_back(std::make_pair(index[i].first, value));
        }
        else {
            len = index[i + 1].second - offset;
            char buffer[len];
            infile.read(buffer, len);
            set.push_back(std::make_pair(index[i].first, std::string(buffer, len)));
        }
        offset += len;
    }
    infile.close();
}