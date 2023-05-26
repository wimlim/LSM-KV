#include "kvstore.h"
/**
 * if there is no directory, create one
 * if there are files in the directory, load them into memory
**/
KVStore::KVStore(const std::string &dir): KVStoreAPI(dir), timeStamp(0), direct(dir), maxLevel(0), ssTables(20)
{
    if (!utils::dirExists(direct)) {
        utils::mkdir(direct.c_str());
        utils::mkdir((direct + "/level-0").c_str());
        return;
    }
    uint64_t t, n, mink, maxk;
    uint32_t l, id;
    std::vector<char> buffer(10240);
    // iterate all directories named Level*
    std::vector<std::string> levels;
	std::vector<std::string> files;
    utils::scanDir(direct, levels);
    for (auto &level : levels) {
        if (level.substr(0, 6) == "level-") {
            l = std::stoi(level.substr(6, level.size() - 6));
            maxLevel = maxLevel > l ? maxLevel : l;
            std::string levelpath = direct + "/" + level;
            utils::scanDir(levelpath, files);
            // iterate all files in the directory
            for (int i = 0; i < files.size(); i++) {
                std::string file = files[i];
                std::string filepath = levelpath + "/" + file;
                // load bloom filter
                printf("Loading %s\n", filepath.c_str());
                if (file.substr(file.length() - 4) == ".sst") {
                    id = std::stoi(file.substr(0, file.length() - 4));
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
                    ssTables[l].push_back(SSTable(filepath, id, t, n, mink, maxk, buffer));
                    // read index
                    infile.read(buffer.data(), n * 12);
                    ssTables[l].back().addKeySet(buffer.data(), n);
                    infile.close();
                }
            }
        }
    }
    for (int i = 0; i <= maxLevel; i++)
        std::sort(ssTables[i].begin(), ssTables[i].end(), [](const SSTable &a, const SSTable &b) {
            return a.timeStamp < b.timeStamp;
        });
    std::string level0 = direct + "/level-0";
    if (!utils::dirExists(level0)) {
        utils::mkdir(level0.c_str());
    }
}

KVStore::~KVStore()
{
    std::string filename = direct + "/level-0/" +  std::to_string(timeStamp) + ".sst";
    ssTables[0].push_back(SSTable());
    memTable.writeToDisk(filename, timeStamp, ssTables[0].back());
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
		ssTables[0].push_back(SSTable());
		memTable.writeToDisk(filename, timeStamp, ssTables[0].back());
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
    for (int i = 0; i <= maxLevel; i++) {
        for (auto it = ssTables[i].rbegin(); it != ssTables[i].rend(); it++) {
            if (it->contains(key)) {
                res = it->get(key).c_str();
                if (res == "~DELETED~") {
                    return "";
                }
                else if (res != "") {
                    return res;
                }
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
    if (ssTables[0].size() < 3) {
        return;
    }
    uint32_t minkey = 0xffffffff;
    uint32_t maxkey = 0;
    std::vector<SSTable> oldSSTables;
    // deal with level-0
    auto it = ssTables[0].begin();
    while (it != ssTables[0].end()) {
        minkey = minkey < it->minKey ? minkey : it->minKey;
        maxkey = maxkey > it->maxKey ? maxkey : it->maxKey;
        oldSSTables.push_back(*it);
        it = ssTables[0].erase(it);
    }
    // deal with level-1 from minkey to maxkey
    it = ssTables[1].begin();
    while (it != ssTables[1].end()) {
        if (it->minKey >= minkey || it->maxKey <= maxkey) {
            oldSSTables.push_back(*it);
            it = ssTables[1].erase(it);
        }
        else {
            it++;
        }
    }
    // merge sort oldSSTables
    std::sort(oldSSTables.begin(), oldSSTables.end(), [](const SSTable &a, const SSTable &b) {
        return a.timeStamp > b.timeStamp;
    });
    std::map<uint64_t, std::string> keySet;
    std::vector<std::pair<uint64_t, std::string>> tmpSet;
    // iterate oldSSTables
    for (auto &sst : oldSSTables) {
        sst.getAll(tmpSet);
        // iterate tmpSet
        for (auto &set : tmpSet) {
            if (keySet.find(set.first) == keySet.end()) {
                if (maxLevel == 0 && set.second == "~DELETED~") {
                    continue;
                }
                keySet[set.first] = set.second;
            }
        }
    }

    for (int i = 1; i <= maxLevel; i++) {
        if (ssTables[i].size() <= (2 << i)) {
            break;
        }

    }
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{	
}