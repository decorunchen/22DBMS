#include"pm_ehash.h"
#include"data_page.h"
#include<math.h>
#include<string.h>
#include<fstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <libpmem.h>


/**
 * @description: construct a new instance of PmEHash in a default directory
 * @param NULL
 * @return: new instance of PmEHash
 */
PmEHash::PmEHash() {
    if(isEmpty()) {
        //新建元数据文件
        int is_pmem = 0;
        size_t mapped_len;
        char PATH[100];
        sprintf(PATH, "mkdir -p %s", PM_EHASH_DIRECTORY);
        system(PATH);
        sprintf(PATH, "%s/%s", PM_EHASH_DIRECTORY, META_NAME);      
        //创建pm_ehash_metadata文件
        metadata = (ehash_metadata*)pmem_map_file(PATH, sizeof(ehash_metadata), PMEM_FILE_CREATE, 0777, &mapped_len, &is_pmem);
        metadata->map_len = mapped_len;
        metadata->catalog_size = DEFAULT_CATALOG_SIZE;
        metadata->global_depth = 4;
        metadata->max_file_id = 1;
        pmem_persist(metadata, mapped_len);
        if (is_pmem)
            pmem_persist(metadata, mapped_len);
        else
            pmem_msync(metadata, mapped_len);
        pmem_unmap(metadata, mapped_len);  

        //新建目录文件
        sprintf(PATH, "%s/%s", PM_EHASH_DIRECTORY, CATALOG_NAME);  
        catalog.buckets_pm_address = (pm_address*)pmem_map_file(PATH, sizeof(pm_address) * DEFAULT_CATALOG_SIZE, PMEM_FILE_CREATE, 0777, &mapped_len, &is_pmem);
        catalog.map_len = mapped_len;
        for(int i = 0; i < DEFAULT_CATALOG_SIZE; i++){
            catalog.buckets_pm_address[i].fileId = 1;
            catalog.buckets_pm_address[i].offset = i;
        }
        if (is_pmem)
            pmem_persist(catalog.buckets_pm_address, mapped_len);
        else
            pmem_msync(catalog.buckets_pm_address, mapped_len);
        pmem_unmap(catalog.buckets_pm_address, mapped_len);

        //新建数据页1
        page_num = 1;
        sprintf(PATH, "%s/%d", PM_EHASH_DIRECTORY, page_num);
        pages = new data_page*[page_num + 1];
        pages[page_num] = (data_page*)pmem_map_file(PATH, sizeof(data_page), PMEM_FILE_CREATE, 0777, &mapped_len, &is_pmem); 
        pages[page_num]->map_len = mapped_len;
        pages[page_num]->bitmap = ~0; 
        for(int i = 0; i < DEFAULT_CATALOG_SIZE; i++){
            pages[page_num]->buckets[i].local_depth = 4;
            memset(pages[page_num]->buckets[i].bitmap, 0, sizeof(pages[page_num]->buckets[i].bitmap));
        }
        if (is_pmem)
            pmem_persist(pages[page_num], mapped_len);
        else
            pmem_msync(pages[page_num], mapped_len);
        pmem_unmap(pages[page_num], mapped_len);
        recover();
    } else {
        //恢复哈希文件
        recover();
    }
}
/**
 * @description: persist and munmap all data in NVM
 * @param NULL 
 * @return: NULL
 */
PmEHash::~PmEHash() {
    size_t map_len;
    page_num = metadata->max_file_id;
    map_len = metadata->map_len;
    pmem_persist(metadata, map_len);
    pmem_unmap(metadata, map_len);
    map_len = catalog.map_len;
    pmem_persist(catalog.buckets_pm_address, map_len);
    pmem_unmap(catalog.buckets_pm_address, map_len);
    for(int i = 1; i <= page_num; i++){
        map_len = pages[i]->map_len;
        pmem_persist(pages[i], map_len);
        pmem_unmap(pages[i], map_len);
    }
} 

/**
 * @description: 插入新的键值对，并将相应位置上的位图置1
 * @param kv: 插入的键值对
 * @return: 0 = insert successfully, -1 = fail to insert(target data with same key exist)
 */
int PmEHash::insert(kv new_kv_pair) {
    //判断哈希表中是否已经存在该键值对
    uint64_t ret = 0;
    if(search(new_kv_pair.key, ret) == 0)
        return -1;
    pm_bucket* bucket = getFreeBucket(new_kv_pair.key);
    kv* freePlace = getFreeKvSlot(bucket);
    *freePlace = new_kv_pair;
    //设置相应的bitmap
    size_t i;
    i = freePlace - bucket->slot;
    bucket->setKV(i, 1);
    return 0;
}
/**
 * @description: 寻找桶中具有目标键的键值id
 * @param pm_bucket: 所在桶
 * @param uint64_t: 要寻找的键
 * @return: 返回所需id -1 = fail to search(target data doesn't exist)
 */
int PmEHash::getKeyId(pm_bucket *bucket, uint64_t key)
{
    size_t i;
    for(i = 0; i < BUCKET_SLOT_NUM; i++)
        if(bucket->HasKV(i) && (bucket->slot)->key == key)
            break;
    if(i == BUCKET_SLOT_NUM)return -1;
    return i;
} 

/**
 * @description: 删除具有目标键的键值对数据，不直接将数据置0，而是将相应位图置0即可
 * @param uint64_t: 要删除的目标键值对的键
 * @return: 0 = removing successfully, -1 = fail to remove(target data doesn't exist)
 */
int PmEHash::remove(uint64_t key) {
    uint64_t b_id = hashFunc(key);
    pm_bucket* bucket = catalog.buckets_virtual_address[b_id];
    uint64_t r_val;
    if(search(key, r_val) == -1)
        return -1;
    
    //位图置0
    bucket->setKV(getKeyId(bucket, key),0);
    //桶是否空了
    uint64_t i;
    for(i = 0;i < BUCKET_SLOT_NUM; i++){
        if(bucket->HasKV(i)){
            return 0;
        }
    }
    //桶空删除
    mergeBucket(b_id);
    return 0;
} 
/**
 * @description: 更新现存的键值对的值
 * @param kv: 更新的键值对，有原键和新值
 * @return: 0 = update successfully, -1 = fail to update(target data doesn't exist)
 */
int PmEHash::update(kv kv_pair) {
    uint64_t b_id = hashFunc(kv_pair.key);
    pm_bucket* bucket = catalog.buckets_virtual_address[b_id];
    uint64_t r_val;
    if(search(kv_pair.key, r_val) == -1)
        return -1;
    size_t i;
    i = getKeyId(bucket, kv_pair.key);
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
    if(bucket == NULL)return -1;
    size_t i;
    for(i = 0; i < BUCKET_SLOT_NUM; i++){
        if(bucket->HasKV(i) && (bucket->slot + i)->key == key){
            break;
        }
    }
    if(i == BUCKET_SLOT_NUM)
        return -1;
    return_val = bucket->slot[i].value;
    return 0;
}

/**
 * @description: 用于对输入的键产生哈希值，然后取模求桶号(自己挑选合适的哈希函数处理)
 * @param uint64_t: 输入的键
 * @return: 返回键所属的桶号
 */
uint64_t PmEHash::hashFunc(uint64_t key) {
    key = (~key) + (key << 21);
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8); 
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4); 
    key = key ^ (key >> 28);
    key = key + (key << 31);
    uint64_t id = key % (1 << (metadata->global_depth));    //取后几位
    uint64_t b_id = id;

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
    if(bucket == NULL){
        catalog.buckets_virtual_address[b_id] = (pm_bucket*)getFreeSlot(catalog.buckets_pm_address[b_id]);
        bucket = catalog.buckets_virtual_address[b_id];
    }
    while(bucket->Full()){
        splitBucket(b_id);
        b_id = hashFunc(key);
        bucket = catalog.buckets_virtual_address[b_id];
    }
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
        if(bucket->HasKV(i) == 0)
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
    //如果局部深度大于全局深度，全局深度等于局部深度,并且目录倍增
    if(metadata->global_depth < bucket->local_depth) {
        metadata->global_depth = bucket->local_depth;
        extendCatalog();
    }
    //桶分裂数据重组
    uint64_t local_id = bucket_id & ((1 << (bucket->local_depth - 1)) - 1);
    uint64_t new_bucket_id = local_id | (1 << (bucket->local_depth - 1));
    //创建新桶
    pm_bucket *new_bucket = (pm_bucket*)getFreeSlot(catalog.buckets_pm_address[new_bucket_id]);
    new_bucket->local_depth = bucket->local_depth;
    memset(new_bucket->bitmap, 0, sizeof(new_bucket->bitmap));

    //改变其它catalog的指向
    int n = 1 << (metadata->global_depth - bucket->local_depth);
    uint64_t son_id;
    for(uint64_t i = 0; i < n; i++){
        son_id = new_bucket_id | (i << bucket->local_depth);
        catalog.buckets_pm_address[son_id] = catalog.buckets_pm_address[new_bucket_id];
        catalog.buckets_virtual_address[son_id] = new_bucket;
    }
    //重组分裂桶
    int num = 0;
    for(int i = 0; i< BUCKET_SLOT_NUM; i++){
        if(bucket->HasKV(i)){
            uint64_t id = hashFunc(bucket->slot[i].key) & ((1 << bucket->local_depth) - 1);
            if(id == new_bucket_id){
                bucket->setKV(i, 0);
                new_bucket->slot[num] = bucket->slot[i];
                new_bucket->setKV(num, 1);
                num++;
            }
        }
    }
} 

/**
 * @description: 桶空后，回收桶的空间，并设置相应目录项指针
 * @param uint64_t: 桶号
 * @return: NULL
 */
void PmEHash::mergeBucket(uint64_t bucket_id) {
    pm_bucket* bucket = catalog.buckets_virtual_address[bucket_id];
    pm_address addr = vAddr2pmAddr[bucket];
    if(vAddr2pmAddr.count(bucket) <= 0)
        return;
    recovery(addr);
    free_list.push(bucket);
    //设置相应目录项指针
    catalog.buckets_virtual_address[bucket_id] = NULL;
} 

/**
 * @description: 对目录进行倍增，需要重新生成新的目录文件并复制旧值，然后删除旧的目录文件
 * @param NULL
 * @return: NULL
 */
void PmEHash::extendCatalog() {
    char name[100];
    size_t map_len;
    int is_pmem;
    sprintf(name, "%s/%s", PM_EHASH_DIRECTORY, CATALOG_NAME);
    pmem_persist(catalog.buckets_pm_address, metadata->catalog_size);
    pmem_unmap(catalog.buckets_pm_address,metadata->catalog_size);
    metadata->catalog_size *= 2;
    catalog.buckets_pm_address = (pm_address*)pmem_map_file(name, sizeof(pm_address) * metadata->catalog_size, PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);
    catalog.map_len = map_len;
    memcpy((void*)(catalog.buckets_pm_address + metadata->catalog_size / 2), (void*)catalog.buckets_pm_address, metadata->catalog_size / 2 * sizeof(pm_address));
    //倍增虚拟地址
    pm_bucket **bucket_virtual_address = new pm_bucket*[metadata->catalog_size];
    for(int i = 0;i < metadata->catalog_size / 2; i++){
        bucket_virtual_address[i] = catalog.buckets_virtual_address[i];
        bucket_virtual_address[metadata->catalog_size / 2 + i] = catalog.buckets_virtual_address[i];
    }
    delete[] catalog.buckets_virtual_address;
    catalog.buckets_virtual_address = bucket_virtual_address;
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
    free_list.pop();
    if(vAddr2pmAddr.count(bucket) > 0){
        new_address = vAddr2pmAddr[bucket];
    }
    pages[new_address.fileId]->bitmap = pages[new_address.fileId]->bitmap | (1 << new_address.offset);
    return bucket;
} 

/**
 * @description: 申请新的数据页文件，并把所有新产生的空闲槽的地址放入free_list等数据结构中
 * @param NULL
 * @return: NULL
 */
void PmEHash::allocNewPage() {
    page_num = metadata->max_file_id + 1;             //数据页加1
    
    data_page **new_pages = new data_page*[page_num + 1];
	memcpy(new_pages, pages, sizeof(data_page*) * page_num);
	delete[] pages;
	pages = new_pages;

    pages[page_num] = persistFile();
    pages[page_num]->bitmap = 0;
    pages[page_num]->file_name = page_num;
    pm_address new_address;
    new_address.fileId = page_num;
    for(size_t i = 0; i < DATA_PAGE_SLOT_NUM; i++){
        new_address.offset = i;
        pmAddr2vAddr[new_address] = pages[page_num]->buckets + i;
        vAddr2pmAddr[pages[page_num]->buckets + i ]= new_address;
        free_list.push(pages[page_num]->buckets + i);
    }
    metadata->max_file_id++;
} 
/**
 * @description: 读取旧数据文件重新载入哈希，恢复哈希关闭前的状态
 * @param NULL
 * @return: NULL
 */
void PmEHash::recover() {
    char PATH[100];
    size_t map_len;
    int is_pmem;
    sprintf(PATH, "%s/%s", PM_EHASH_DIRECTORY, META_NAME);
    metadata = (ehash_metadata*)pmem_map_file(PATH, sizeof(ehash_metadata), PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);
    metadata->map_len = map_len;
    sprintf(PATH, "%s/%s", PM_EHASH_DIRECTORY, CATALOG_NAME);
    catalog.buckets_pm_address = (pm_address*)pmem_map_file(PATH, sizeof(pm_address) * metadata->catalog_size, PMEM_FILE_CREATE, 0777, &map_len, &is_pmem); 
    catalog.map_len = map_len;
    mapAllPage();
    catalog.buckets_virtual_address = new pm_bucket*[metadata->catalog_size];
    for(int i = 0; i < metadata->catalog_size; i++){
        catalog.buckets_virtual_address[i] = pmAddr2vAddr[catalog.buckets_pm_address[i]];
    }
} 

/**
 * @description: 重启时，将所有数据页进行内存映射，设置地址间的映射关系，空闲的和使用的槽位都需要设置 
 * @param NULL
 * @return: NULL
 */
void PmEHash::mapAllPage() {
    char PATH[100];
    size_t map_len;
    int is_pmem;
    pm_address addr;
    pages = new data_page*[metadata->max_file_id + 1];
    for(int i = 1; i <= metadata->max_file_id; i++){
        sprintf(PATH, "%s/%d", PM_EHASH_DIRECTORY, i);
        pages[i] = (data_page*)pmem_map_file(PATH, sizeof(data_page), PMEM_FILE_CREATE, 0777, &map_len, &is_pmem);
        pages[i]->map_len = map_len;
        addr.fileId = i;
        for(int j = 0; j < DATA_PAGE_SLOT_NUM; j++){
            addr.offset = j;
            pmAddr2vAddr[addr] = pages[i]->buckets + j;
            vAddr2pmAddr[pages[i]->buckets + j ]= addr;
            if (((pages[i]->bitmap >> j) & 1) == 0) {
				free_list.push(pages[i]->buckets + j);
			}
        }
    } 
} 

/**
 * @description: 删除PmEHash对象所有数据页，目录和元数据文件，主要供gtest使用。即清空所有可扩展哈希的文件数据，不止是内存上的
 * @param NULL
 * @return: NULL
 */
void PmEHash::selfDestory() {
    char PATH[100];
    size_t map_len;
    page_num = metadata->max_file_id;
    sprintf(PATH, "%s/%s", PM_EHASH_DIRECTORY, META_NAME);
    map_len = metadata->map_len;
    pmem_unmap(metadata, map_len);
    std::remove(PATH);
    sprintf(PATH, "%s/%s", PM_EHASH_DIRECTORY, CATALOG_NAME);
    map_len = catalog.map_len;
    pmem_unmap(catalog.buckets_pm_address, map_len);
    std::remove(PATH);
    for(int i = 1; i <= page_num; i++) {
        sprintf(PATH, "%s/%d", PM_EHASH_DIRECTORY, i);
        map_len = pages[i]->map_len;
        pmem_unmap(pages[i], map_len);
        std::remove(PATH);
    }
    sprintf(PATH, "rm -r %s", PM_EHASH_DIRECTORY);
    system(PATH);
} 

/**
 * @description: 判断是否应该初始化哈希表
 * @param NULL
 * @return: 1需要初始化 0不需要
 */
 int PmEHash::isEmpty() {
     std::FILE* file = fopen("../data/pm_ehash_metadata", "r");
     if(file == NULL)
        return 1;
    return 0;
 }