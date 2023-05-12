#pragma once

#include "kvstore_api.h"
#include "skiplist.h"
#include "bloomfilter.h"
#include "utils.h"
#include <fstream>
#include <cstdint>
#include <algorithm>
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;

class KVStore : public KVStoreAPI {
private:
	SkipList memTable;
	std::vector<std::pair<uint32_t, BloomFilter>> bloomFilters;
	std::vector<std::vector<uint32_t>> fileNum;
	std::string direct;
	uint64_t timeStamp;
	uint64_t maxLevel;
	static constexpr uint32_t MAX_MEM_SIZE = 2 * 1024 * 1024;

public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
};
