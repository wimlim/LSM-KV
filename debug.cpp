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
    KVStore store("./data");
    std::cout << store.get(1).c_str() << std::endl;
    store.put(1, "SE");
    std::cout << store.get(1).c_str() << std::endl;
    store.del(1);
    std::cout << store.get(1).c_str() << std::endl;
    std::cout << store.del(1) << std::endl;
}