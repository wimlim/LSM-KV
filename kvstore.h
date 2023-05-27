#pragma once

#include "kvstore_api.h"
#include "memtable.h"
#include "sstable.h"
#include "utils.h"
#include <cstdint>
#include <map>
#include <algorithm>

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
	int selectCompaction(int level, int l, int r, std::vector<uint32_t> &idlist, std::map<uint64_t, std::string> &keyset);
	void compactionLeveling(int level, int timestamp, const std::vector<uint32_t> &idlist, const std::map<uint64_t, std::string> &keyset);
	MemTable memTable;
	std::vector<std::vector<SSTable>> ssTables;
	uint32_t maxLevel;
	std::string direct;
	uint64_t timeStamp;
	static constexpr uint32_t MAX_MEM_SIZE = 2 * 1024 * 1024;
};
