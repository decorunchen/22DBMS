#include"pm_ehash.h"
#include"data_page.h"
#include<libpmem.h>

// 数据页表的相关操作实现都放在这个源文件下，如PmEHash申请新的数据页和删除数据页的底层实现
int page_num = 1;
data_page **pages;         //data_page.h中的全局变量

/**
 * @decription: 移除指定slot
 * @param 下标, 数据页号
 * @return NULL
 */
void recovery(pm_address addr) {
    pages[addr.fileId]->bitmap = pages[addr.fileId]->bitmap & ~(1 << addr.offset);
}

void persist(void* address) {
    pmem_persist(address, sizeof(kv));
}

/**
 * @decription: 新建一个数据页
 * @param 文件名
 * @return: 新建的文件句柄
 */
data_page* persistFile() {
    int is_pmem = 0;
    size_t mapped_len;
    char PATH[100];
    sprintf(PATH, "%s/%d", PM_EHASH_DIRECTORY, page_num);
    data_page *file = (data_page*)pmem_map_file(PATH, sizeof(data_page), PMEM_FILE_CREATE, 0777, &mapped_len, &is_pmem);
    file->map_len = mapped_len;
    return file;
}