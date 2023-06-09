#include "kvstore.h"
/**
 * if there is no directory, create one
 * if there are files in the directory, load them into memory
**/
KVStore::KVStore(const std::string &dir): KVStoreAPI(dir), timeStamp(0), direct(dir), maxLevel(0), ssTables(20)
{
    initConf();
    if (!utils::dirExists(direct)) {
        utils::mkdir(direct.c_str());
        utils::mkdir((direct + "/level-0").c_str());
        return;
    }
    uint64_t t, n, mink, maxk;
    uint32_t l, id;
    std::vector<char> buffer(MAX_MEM_SIZE);
    // iterate all directories named Level*
    std::vector<std::string> levels;
	std::vector<std::string> files;
    utils::scanDir(direct, levels);
    for (auto &level : levels) {
        if (level.substr(0, 6) == "level-") {
            l = std::stoi(level.substr(6, level.size() - 6));
            maxLevel = maxLevel > l ? maxLevel : l;
            std::string levelpath = direct + "/" + level;
            files.clear();
            utils::scanDir(levelpath, files);
            // iterate all files in the directory
            for (auto &file : files) {
                std::string filepath = levelpath + "/" + file;
                // load bloom filter
                if (file.substr(file.length() - 4) == ".sst") {
                    id = std::stoi(file.substr(0, file.length() - 4));
                    std::ifstream infile(filepath, std::ios::in | std::ios::binary);
                    if (!infile.is_open()) {
                        std::cerr << "Error: kvstore open file " << filepath << " failed" << std::endl;
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
    std::string level0 = direct + "/level-0";
    if (!utils::dirExists(level0)) {
        utils::mkdir(level0.c_str());
    }
}

void KVStore::initConf() {
    // open file default.conf
    std::ifstream infile("default.conf", std::ios::in);
    if (!infile.is_open()) {
        std::cerr << "Error: kvstore open file default.conf failed" << std::endl;
        return;
    }
    // read in levelLimit and LevelType
    int id;
    std::string type;
    std::string line;
    while (std::getline(infile, line)) {
        std::stringstream ss(line);
        ss >> id;
        ss >> levelLimit[id];
        ss >> type;
        if (type == "Tiering") levelType[id] = Tiering;
        else levelType[id] = Leveling;
    }
    for (int i = id + 1; i < 20; i++) {
        levelLimit[i] = levelLimit[i - 1] << 1;
        levelType[i] = Leveling;
    }
}

KVStore::~KVStore()
{
    timeStamp++;
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
    int time = 0;
    std::string tmp;
    // iterate bloomfilter from end
    for (int i = 0; i <= maxLevel; i++) {
        std::sort(ssTables[i].begin(), ssTables[i].end(), [](SSTable &a, SSTable &b) {
            return a.timeStamp > b.timeStamp;
        });
        for (auto it = ssTables[i].begin(); it != ssTables[i].end(); it++) {
            if (it->contains(key)) {
                if (it->timeStamp > time ) {
                    tmp = it->get(key);
                    if (tmp == "") {
                        continue;
                    }
                    res = tmp;
                    time = it->timeStamp;
                }
            }
        }
    }
    if (res != "~DELETED~") {
        return res;
    }
    else {
        return "";
    }
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
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    std::vector<std::string> files;
	// delete all files and directs under direct
    for (int i = 0; i <= maxLevel; i++) {
        std::string levelpath = direct + "/level-" + std::to_string(i);
        utils::scanDir(levelpath, files);
        for (auto &file : files) {
            std::string filepath = levelpath + "/" + file;
            utils::rmfile(filepath.c_str());
        }
        if (i)
            utils::rmdir(levelpath.c_str());
    }
    timeStamp = 0;
    maxLevel = 0;
    ssTables.clear();
	// reset memTable
	memTable.reset();
}

void KVStore::compaction() {
    if (ssTables[0].size() <= 2) {
        return;
    }
    std::vector<uint32_t> idlist;
    std::map<uint64_t, std::string> keySet;
    int maxtime;

    for (int i = 0; i <= maxLevel; i++) {
        int limit = levelLimit[i];
        if (ssTables[i].size() <= limit) {
            return;
        }
        if (levelType[i] == Leveling) {
            std::sort(ssTables[i].begin(), ssTables[i].end(), [](SSTable &a, SSTable &b) {
                return a.timeStamp > b.timeStamp;
            });
            // find the sstables of the same value right before limit
            while (limit && ssTables[i][limit].timeStamp == ssTables[i][limit - 1].timeStamp) {
                limit--;
            }
        }
        else {
            limit = 0;
        }
        maxtime = selectCompaction(i, limit, ssTables[i].size(), idlist, keySet);
        compactionLeveling(i + 1, maxtime, idlist, keySet);
    }
}

int KVStore::selectCompaction(int level, int l, int r, std::vector<uint32_t> &idlist, std::map<uint64_t, std::string> &keyset) {
    idlist.clear();
    keyset.clear();
    uint64_t minkey = 0xffffffffffffffff;
    uint64_t maxkey = 0;
    int maxtime = 0;
    std::vector<SSTable> oldSSTables;
    
    // deal with level-0
    auto it = ssTables[level].begin() + l;
    r = r - l;
    while (r--) {
        minkey = minkey < it->minKey ? minkey : it->minKey;
        maxkey = maxkey > it->maxKey ? maxkey : it->maxKey;
        maxtime = maxtime > it->timeStamp ? maxtime : it->timeStamp;
        idlist.push_back(it->id);
        oldSSTables.push_back(*it);
        it = ssTables[level].erase(it);
    }
    // deal with level-1 from minkey to maxkey
    int nxtlevel = level + 1;
    if (levelType[nxtlevel] == Leveling) {
        it = ssTables[nxtlevel].begin();
        while (it != ssTables[nxtlevel].end()) {
            if (it->minKey <= maxkey && it->maxKey >= minkey) {
                idlist.push_back(it->id);
                oldSSTables.push_back(*it);
                it = ssTables[nxtlevel].erase(it);
            }
            else {
                it++;
            }
        }
    }
    // merge sort oldSSTables
    std::sort(oldSSTables.begin(), oldSSTables.end(), [](const SSTable &a, const SSTable &b) {
        return a.timeStamp > b.timeStamp;
    });
    
    std::vector<std::pair<uint64_t, std::string>> tmpSet;
    // iterate oldSSTables
    for (auto &sst : oldSSTables) {
        sst.getAll(tmpSet);
        std::string filepath = sst.pathname;
        utils::rmfile((filepath).c_str());
        // iterate tmpSet
        for (auto &set : tmpSet) {
            if (keyset.find(set.first) == keyset.end()) {
                keyset[set.first] = set.second;
            }
        }
    }
    return maxtime;
}

void KVStore::compactionLeveling(int level, int timestamp, const std::vector<uint32_t> &idlist, const std::map<uint64_t, std::string> &keyset) {
    if (level > maxLevel) {
        maxLevel = level;
        utils::mkdir((direct + "/level-" + std::to_string(maxLevel)).c_str());
    }
    MemTable tmp;
    uint64_t key;
    std::string s;
    int cnt = 0;
    for (auto &set : keyset) {
        key = set.first;
        s = set.second;
        if (tmp.getSize() + s.size() + 12 > MAX_MEM_SIZE) {
            std::string filename = direct + "/level-" + std::to_string(level) + "/" +  std::to_string(idlist[cnt]) + ".sst";
            ssTables[level].push_back(SSTable());
            tmp.writeToDisk(filename, timeStamp, ssTables[level].back());
            ssTables[level].back().id = idlist[cnt];
            tmp.reset();
            tmp.ins(key, s);
            cnt++;
        }
        else {
            tmp.ins(key, s);
	    }
    }
    if (tmp.getSize() > 10272) {
        std::string filename = direct + "/level-" + std::to_string(level) + "/" +  std::to_string(idlist[cnt]) + ".sst";
        ssTables[level].push_back(SSTable());
        tmp.writeToDisk(filename, timeStamp, ssTables[level].back());
        ssTables[level].back().id = idlist[cnt];
        tmp.reset();
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