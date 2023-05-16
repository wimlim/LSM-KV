#include "skiplist.h"
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
    x.addKeySet(1, 10);
    x.addKeySet(2, 20);
    x.addKeySet(3, 30);
    x.get(3);
    x.get(2);
    x.get(1);
    return 0;
    std::string path = "D:\\UserData\\Desktop\\LSMKV\\data\\level-0\\0.sst";
    list.writeToDisk(path, 0, x);
    KVStore store("D:\\UserData\\Desktop\\LSMKV\\data");
}