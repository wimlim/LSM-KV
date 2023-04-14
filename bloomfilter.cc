#include "bloomfilter.h"

BloomFilter::BloomFilter() : bytes(kNumBits / 8, false) {}

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
        if (!bytes[hash[i] / 8] & (1 << (hash[i] % 8)))
            return false;
    }
    return true;
}