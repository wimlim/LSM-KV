#include "kvstore.h"

bool filter_cmp(const std::pair<uint64_t, SSTable> &a, const std::pair<uint64_t, SSTable> &b) {
    return a.first < b.first;
}
/**
 * if there is no directory, create one
 * if there are files in the directory, load them into memory
**/
KVStore::KVStore(const std::string &dir): KVStoreAPI(dir), timeStamp(0), maxLevel(0), direct(dir), levelNode(10)
{
    if (!utils::dirExists(direct)) {
        utils::mkdir(direct.c_str());
        return;
    }
    uint64_t t, n, mink, maxk;
    uint32_t l;
    std::vector<char> buffer(1280);
    // iterate all directories named Level*
    std::vector<std::string> levels;
	std::vector<std::string> files;
    utils::scanDir(direct, levels);
    for (auto &level : levels) {
        if (level.substr(0, 6) == "level-") {
            l = std::stoi(level.substr(6));
            std::string levelpath = direct + "\\" + level;
            utils::scanDir(levelpath, files);
            // iterate all files in the directory
            for (auto &file : files) {
				// calculate the files under the level
                levelNode[l].push_back(stoi(file.substr(0, file.length() - 4)));
                std::string filepath = levelpath + "\\" + file;
                // load bloom filter
                if (file.substr(file.length() - 4) == ".sst") {
                    std::ifstream infile(filepath, std::ios::in | std::ios::binary);
                    if (!infile.is_open()) {
                        printf("Error: cannot open file %s\n", filepath.c_str());
                        continue;
                    }
                    printf("%s open\n", filepath.c_str());
                    // read t, n, mink, maxk
                    infile.read(buffer.data(), 32);
                    memcpy(&t, buffer.data(), 8);
                    memcpy(&n, buffer.data() + 8, 8);
                    memcpy(&mink, buffer.data() + 16, 8);
                    memcpy(&maxk, buffer.data() + 24, 8);
                    printf("%d %d %d %d %d\n", l, t, n, mink, maxk);
                    timeStamp = timeStamp > t ? timeStamp : t;
                    // read bloom filter
                    infile.read(buffer.data(), 1280);
                    ssTables.push_back(std::make_pair(t, SSTable(l, n, mink, maxk, buffer)));
                    // read index
                    infile.read(buffer.data(), n * 12);
                    ssTables[t].second.addKeySet(buffer.data(), n);
                    infile.close();
                }
            }
        }
    }
    std::sort(ssTables.begin(), ssTables.end(), filter_cmp);
	timeStamp++;
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
	if (memTable.getSize() + s.size() + 12 > MAX_MEM_SIZE) {
		std::string filename = direct + "\\level-0\\" +  std::to_string(timeStamp) + ".sst";
		std::ofstream out(filename, std::ios::out | std::ios::binary | std::ios::trunc);
		ssTables.push_back(std::make_pair(timeStamp, SSTable()));
		memTable.writeToDisk(direct, timeStamp, ssTables.back().second);
        compaction();
		memTable.reset();
		memTable.ins(key, s);
	}
	else {
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
    if (res != "") {
        return res;
    }
    // iterate bloomfilter from end
    std::for_each(ssTables.rbegin(), ssTables.rend(), [](const auto& p) {
        if (p.second.contains(key)) {
            res = p.second.get(key);
            if (res == "~DELETED~") {
                return "";
            }
            else if (res != "") {
                return res;
            }
        }
    });
    return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    std::string res = get(key);
    if (res == "") {
        return false;
    }
    else {
        put(key, "~DELETED~");
        return true;
    }
    return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	// delete all files and directs under direct

	// reset bloomfilters
	ssTables.clear();
	// reset memTable
	memTable.reset();
}

void compaction() {

}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{	
}