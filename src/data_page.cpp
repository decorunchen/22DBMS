#include"pm_ehash.h"
#include<libpmem.h>

// 数据页表的相关操作实现都放在这个源文件下，如PmEHash申请新的数据页和删除数据页的底层实现

/**
 * @decription: 移除指定slot
 * @param 下标, 数据页号
 * @return NULL
 */
void recovery(uint64_t index, uint64_t pnum) {
    if(ISPAGE[pnum] != 0)
        return;
    for(size_t i = index; i < n - 1; i++)
        pages[pnum].buckets[i] = pages[pnum].buckets[i + 1];
}

void persist(void* address) {
    pmem_persisit(address, sizeof(kv));
}

/**
 * @decription: 新建一个数据页
 * @param 文件名
 * @return: 新建的文件句柄
 */
data_page* persistFile() {
    pages[page_num] = data_page();
    pages[page_num]->file_name = fname;
    pages[page_num]->buckets = new pm_bucket[DATA_PAGE_SLOT_NUM];
    int is_pmem = 0;
    size_t mapped_len;
    std::string PATH = PM_EHASH_DIRECTORY + std::to_string(page_num);
    char* file = pmem_map_file(PATH, DATA_PAGE_SLOT_NUM * sizeof(pm_bucket), PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
    if(file == NULL)
        return NULL；
    if (is_pmem)
        pmem_persist(file, mapped_len);
    else
        pmem_msync(file, mapped_len);
    
    map_list.push(mapped_len);

    ISPAGE[page_num] = 1;
    page_num++;
    
    return file;
}