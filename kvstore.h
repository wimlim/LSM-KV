#pragma once

#include "kvstore_api.h"
#include "memtable.h"
#include "sstable.h"
#include "utils.h"
#include <cstdint>
#include <algorithm>

enum levelMode {
	Tiering, Leveling
};

class KVStore : public KVStoreAPI {
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
private:
	void compaction();
	MemTable memTable;
	std::vector<std::pair<uint32_t, SSTable>> ssTables;
	std::vector<std::vector<uint32_t>> levelNode;
	uint32_t fileLimit[10] = {0};
	levelMode levelMode[10] = {Tiering};
	std::string direct;
	uint64_t timeStamp;
	static constexpr uint32_t MAX_MEM_SIZE = 2 * 1024 * 1024;
};
