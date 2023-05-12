#include "bloomfilter.h"

BloomFilter::BloomFilter(uint32_t n, uint32_t min, uint32_t max, const std::vector<char> &b) : 
            bytes(kNumBits / 8, 0), keyNum(n), minKey(min), maxKey(max) {
    bytes = b;
}

void BloomFilter::add(uint64_t key) {
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    for (int i = 0; i < kNumHashes; i++) {
        hash[i] %= kNumBits;
        bytes[hash[i] / 8] |= (1 << (hash[i] % 8));
    }
}

bool BloomFilter::contains(uint64_t key) {
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    for (int i = 0; i < 4; i++) {
        hash[i] %= kNumBits;
        if (!(bytes[hash[i] / 8] & (1 << (hash[i] % 8))))
            return false;
    }
    return true;
}