#include "kvstore.h"
#include "utils.h"

/**
 * if there is no directory, create one
 * if there are files in the directory, load them into memory
**/
KVStore::KVStore(const std::string &dir): 
	KVStoreAPI(dir), timeStamp(0), maxLevel(0), direct(dir)
{
	if (!fs::exists(direct)) {
		fs::create_directory(direct);
		return;
	}
	uint32_t t, n, mink, maxk;
	std::vector<char> buffer(1280);
	// iterate all directories named Level*
	for (auto &level : fs::directory_iterator(direct)) {
		if (level.path().filename().string().substr(0, 5) == "Level") {
			// iterate all files in the directory
			for (auto &file : fs::directory_iterator(level.path())) {
				// load bloom filter
				if (file.path().extension() == ".sst") {
					std::ifstream infile(file.path().string(), std::ios::in | std::ios::binary);
					if (!infile.is_open()) {
						printf("Error: cannot open file %s\n", file.path().string().c_str());
						continue;
					}
					// read t, n, mink, maxk
					infile.read(t, 4);
					infile.read(n, 4);
					infile.read(mink, 4);
					infile.read(maxk, 4);
					timeStamp = timeStamp > t ? timeStamp : t;
					// read bloom filter
					infile.read(buffer.data(), 1280);
					bloomFilters.push_back(std::make_pair(t, BloomFilter(n, mink, maxk, buffer)));
					infile.close();
				}
			}
		}
	}
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
	if (memTable.getSize() + s.size() > MAX_MEM_SIZE) {
		std::string filename = direct + "Level0" +  std::to_string(timeStamp) + ".sst";
		std::ofstream out(filename, std::ios::out | std::ios::binary);
		bloomFilters.push_back(std::make_pair(timeStamp, BloomFilter()));
		memTable.writeToDisk(direct, timeStamp, bloomFilters.back().second);
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
	std::string res = memTable.get(key);
	if (res == empty_string) {

	}
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
	if (!fs::remove_all(direct))
		printf("Error: cannot remove directory %s\n");
	// delete all files and directs under direct
	for (auto &level : fs::directory_iterator(direct)) {
		fs::remove_all(level.path());
	}
	// reset bloomfilters
	bloomFilters.clear();
	// reset memTable
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