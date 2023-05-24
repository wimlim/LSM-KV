#include "kvstore.h"

bool filter_cmp(const std::pair<uint64_t, SSTable> &a, const std::pair<uint64_t, SSTable> &b) {
    return a.first < b.first;
}
/**
 * if there is no directory, create one
 * if there are files in the directory, load them into memory
**/
KVStore::KVStore(const std::string &dir): KVStoreAPI(dir), timeStamp(-1), direct(dir), maxLevel(0)
{
    if (!utils::dirExists(direct)) {
        utils::mkdir(direct.c_str());
        timeStamp = 0;
        utils::mkdir((direct + "/level-0").c_str());
        return;
    }
    uint64_t t, n, mink, maxk;
    uint32_t l;
    std::vector<char> buffer(10240);
    // iterate all directories named Level*
    std::vector<std::string> levels;
	std::vector<std::string> files;
    utils::scanDir(direct, levels);
    for (auto &level : levels) {
        if (level.substr(0, 6) == "level-") {
            l = std::stoi(level.substr(6, level.size() - 6));
            std::string levelpath = direct + "/" + level;
            utils::scanDir(levelpath, files);
            // iterate all files in the directory
            for (auto &file : files) {
				// calculate the files under the level
                std::string filepath = levelpath + "/" + file;
                // load bloom filter
                if (file.substr(file.length() - 4) == ".sst") {
                    std::ifstream infile(filepath, std::ios::in | std::ios::binary);
                    if (!infile.is_open()) {
                        std::cerr << "Error: kvstore open file failed" << std::endl;
                        continue;
                    }
                    // read t, n, mink, maxk
                    infile.read(buffer.data(), 32);
                    memcpy(&t, buffer.data(), 8);
                    memcpy(&n, buffer.data() + 8, 8);
                    memcpy(&mink, buffer.data() + 16, 8);
                    memcpy(&maxk, buffer.data() + 24, 8);
                    timeStamp = timeStamp > t ? timeStamp : t;
                    // read bloom filter
                    infile.read(buffer.data(), 10240);
                    ssTables.push_back(SSTable(l, t, n, mink, maxk, buffer));
                    // read index
                    infile.read(buffer.data(), n * 12);
                    // print buffer
                    ssTables.back().addKeySet(buffer.data(), n);
                    infile.close();
                }
            }
        }
    }

    std::string level0 = direct + "/level-0";
    if (!utils::dirExists(level0)) {
        utils::mkdir(level0.c_str());
    }
	timeStamp++;
}

KVStore::~KVStore()
{
    std::string filename = direct + "/level-0/" +  std::to_string(timeStamp) + ".sst";
    ssTables.push_back(SSTable());
    memTable.writeToDisk(filename, timeStamp, ssTables.back());
    compaction();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	if (memTable.getSize() + s.size() + 12 > MAX_MEM_SIZE) {
        timeStamp++;
		std::string filename = direct + "/level-0/" +  std::to_string(timeStamp) + ".sst";
		ssTables.push_back(SSTable());
		memTable.writeToDisk(filename, timeStamp, ssTables.back());
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
    if (res == "~DELETED~") {
        return "";
    }
    if (res != "") {
        return res;
    }
    // iterate bloomfilter from end
    for (auto it = ssTables.rbegin(); it != ssTables.rend(); it++) {
        if (it->contains(key)) {
            res = it->get(direct, key).c_str();
            if (res == "~DELETED~") {
                return "";
            }
            else if (res != "") {
                return res;
            }
        }
    }
    return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    std::string res = get(key);
    if (res == "" || res == "~DELETED~") {
        return false;
    }
    else {
        put(key, "~DELETED~");
        return true;
    }
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    std::vector<std::string> files;
	// delete all files and directs under direct
    for (int i = 1; i < 10; i++) {
        std::string levelpath = direct + "/level-" + std::to_string(i);
        utils::scanDir(levelpath, files);
        for (auto &file : files) {
            std::string filepath = levelpath + "/" + file;
            utils::rmfile(filepath.c_str());
        }
        utils::rmdir(levelpath.c_str());
    }
	// reset bloomfilters
	ssTables.clear();
	// reset memTable
	memTable.reset();
}

void KVStore::compaction() {

}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{	
}