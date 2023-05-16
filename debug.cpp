#include "memtable.h"
#include "sstable.h"
#include "utils.h"
#include "kvstore.h"
#include <iostream>
#include <cstdio>

int main() {
    MemTable list;
    list.ins(1, "1");
    list.ins(2, "2");
    list.ins(3, "3");
    SSTable x;
    std::string path = "D:/UserData/Desktop/LSMKV/data/level-0/0.sst";
    list.writeToDisk(path, 0, x);
    KVStore store("D:/UserData/Desktop/LSMKV/data");
    std::cout << store.get(3).c_str() << std::endl;
}