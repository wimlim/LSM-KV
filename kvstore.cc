#include "kvstore.h"
#include "utils.h"


KVStore::KVStore(const std::string &dir): 
	KVStoreAPI(dir), timeStamp(0), direct(dir)
{
	utils::mkdir(dir.c_str());

}

KVStore::~KVStore()
{

}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	if (!memTable.ins(key, s)) {
		std::string filename = std::to_string(timeStamp++) + ".sst";
		std::ofstream out(filename, std::ios::out | std::ios::binary);

		memTable.reset();
		memTable.ins(key, s);
	}
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	
	return memTable.get(key);
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	return memTable.del(key);
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	namespace fs = std::experimental::filesystem;
	if (!fs::remove_all(direct))
		printf("Error: cannot remove directory %s\n");
	bloomFilters.clear();
	memTable.reset();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{	
}