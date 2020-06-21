#include"pm_ehash.h"
#include<math.h>
#include<string>
#include<fstream>

/**
 * @description: construct a new instance of PmEHash in a default directory
 * @param NULL
 * @return: new instance of PmEHash
 */
PmEHash::PmEHash() {
    std::fstream _file;
    std::string fname = PM_EHASH_DIRECTORY + "1.txt";
    _file.open(fname);
    if(_file) {
        //新建所需的数据文件

    } else {
        recover();
    }
}
/**
 * @description: persist and munmap all data in NVM
 * @param NULL 
 * @return: NULL
 */
PmEHash::~PmEHash() {
    selfDestory();
}

/**
 * @description: 插入新的键值对，并将相应位置上的位图置1
 * @param kv: 插入的键值对
 * @return: 0 = insert successfully, -1 = fail to insert(target data with same key exist)
 */
int PmEHash::insert(kv new_kv_pair) {
    uint64_t ret = 0;
    if(search(new_kv_pair.key, ret) == 0)
        return -1;
    pm_bucket* bucket = getFreeBucket(new_kv_pair.key);
    kv* freePlace = getFreeKvSlot(bucket);
    *freePlace = new_kv_pair;
    persist(freePlace);
    return 0;
}

/**
 * @description: 删除具有目标键的键值对数据，不直接将数据置0，而是将相应位图置0即可
 * @param uint64_t: 要删除的目标键值对的键
 * @return: 0 = removing successfully, -1 = fail to remove(target data doesn't exist)
 */
int PmEHash::remove(uint64_t key) {
    uint64_t num = hashFunc(key);
    pm_bucket* bucket = catalog.buckets_virtual_address[num];
    uint64_t r_val;
    if(search(key, r_val) == -1)
        return -1;
    size_t i;
    for(i = 0; i < BUCKET_SLOT_NUM; i++)
        if((bucket->slot)->key == key)
            break;
    bucket->bitmap[i] = 0;
    if(i == 0)
        mergeBucket(num);
    return 0;
}
/**
 * @description: 更新现存的键值对的值
 * @param kv: 更新的键值对，有原键和新值
 * @return: 0 = update successfully, -1 = fail to update(target data doesn't exist)
 */
int PmEHash::update(kv kv_pair) {
    uint64_t id = hashFunc(kv_pair.key);
    pm_bucket* bucket = catalog.buckets_virtual_address[id];
    uint64_t r_val;
    if(search(kv_pair.key, r_val) == -1)
        return -1;
    size_t i;
    for(i = 0; i < BUCKET_SLOT_NUM; i++)
        if((bucket->slot)->key == kv_pair.key)
            break;
    bucket->slot[i].value = kv_pair.value;
    return 0;
}
/**
 * @description: 查找目标键值对数据，将返回值放在参数里的引用类型进行返回
 * @param uint64_t: 查询的目标键
 * @param uint64_t&: 查询成功后返回的目标值
 * @return: 0 = search successfully, -1 = fail to search(target data doesn't exist) 
 */
int PmEHash::search(uint64_t key, uint64_t& return_val) {
    uint64_t b_id = hashFunc(key);
    pm_bucket* bucket = catalog.buckets_virtual_address[b_id];
    size_t i;
    for(i = 0; i < BUCKET_SLOT_NUM; i++)
        if((bucket->slot)->key == key)
            break;
    if(i == BUCKET_SLOT_NUM)
        return -1;
    uint64_t r_val = bucket->slot[i].value;
    return 0;
}

/**
 * @description: 用于对输入的键产生哈希值，然后取模求桶号(自己挑选合适的哈希函数处理)
 * @param uint64_t: 输入的键
 * @return: 返回键所属的桶号
 */
uint64_t PmEHash::hashFunc(uint64_t key) {
    uint64_t id = key % (1 << (metadata->global_depth));
    uint64_t b_id;
    int i = 0, j = 0;
    while (i != metadata->global_depth) {
        b_id += (uint64_t)pow(2, i);
        id = id / 2;
        i++;
        j++;
    }

    return b_id;
}

/**
 * @description: 获得供插入的空闲的桶，无空闲桶则先分裂桶然后再返回空闲的桶
 * @param uint64_t: 带插入的键
 * @return: 空闲桶的虚拟地址
 */
pm_bucket* PmEHash::getFreeBucket(uint64_t key) {
    uint64_t b_id = hashFunc(key);
    pm_bucket* bucket = catalog.buckets_virtual_address[b_id];
    size_t i;
    for(i = 0; i < BUCKET_SLOT_NUM; i++)
        if(bucket->bitmap[i] == 0)
            break;
    if(i == BUCKET_SLOT_NUM)
        splitBucket(key);
    return bucket;
}

/**
 * @description: 获得空闲桶内第一个空闲的位置供键值对插入
 * @param pm_bucket* bucket
 * @return: 空闲键值对位置的虚拟地址
 */
kv* PmEHash::getFreeKvSlot(pm_bucket* bucket) {
    size_t i;
    for(i = 0; i < BUCKET_SLOT_NUM; i++)
        if(bucket->bitmap[i] == 0)
            break;
    return (bucket->slot) + i;
}

/**
 * @description: 桶满后进行分裂操作，可能触发目录的倍增
 * @param uint64_t: 目标桶在目录中的序号
 * @return: NULL
 */
void PmEHash::splitBucket(uint64_t bucket_id) {
    pm_bucket* bucket = catalog.buckets_virtual_address[bucket_id];

    //局部深度自加1
    ++(bucket->local_depth);

    //如果局部深度大于全局深度，全局深度等于局部深度
    if(metadata->global_depth < bucket->local_depth)
        metadata->global_depth = bucket->local_depth;
    
    //桶分裂数据重组
    
}

/**
 * @description: 桶空后，回收桶的空间，并设置相应目录项指针
 * @param uint64_t: 桶号
 * @return: NULL
 */
void PmEHash::mergeBucket(uint64_t bucket_id) {
    
}

/**
 * @description: 对目录进行倍增，需要重新生成新的目录文件并复制旧值，然后删除旧的目录文件
 * @param NULL
 * @return: NULL
 */
void PmEHash::extendCatalog() {

}

/**
 * @description: 获得一个可用的数据页的新槽位供哈希桶使用，如果没有则先申请新的数据页
 * @param pm_address&: 新槽位的持久化文件地址，作为引用参数返回
 * @return: 新槽位的虚拟地址
 */
void* PmEHash::getFreeSlot(pm_address& new_address) {
    if(free_list.empty())
        allocNewPage();
    pm_bucket* bucket = free_list.front();
    if(vAddr2pmAddr.count(bucket) > 0)
        new_address = vAddr2pmAddr[bucket];
}

/**
 * @description: 申请新的数据页文件，并把所有新产生的空闲槽的地址放入free_list等数据结构中
 * @param NULL
 * @return: NULL
 */
void PmEHash::allocNewPage() {

}

/**
 * @description: 读取旧数据文件重新载入哈希，恢复哈希关闭前的状态
 * @param NULL
 * @return: NULL
 */
void PmEHash::recover() {

}

/**
 * @description: 重启时，将所有数据页进行内存映射，设置地址间的映射关系，空闲的和使用的槽位都需要设置 
 * @param NULL
 * @return: NULL
 */
void PmEHash::mapAllPage() {

}

/**
 * @description: 删除PmEHash对象所有数据页，目录和元数据文件，主要供gtest使用。即清空所有可扩展哈希的文件数据，不止是内存上的
 * @param NULL
 * @return: NULL
 */
void PmEHash::selfDestory() {

}