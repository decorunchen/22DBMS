#include"pm_ehash.h"
#include<libpmem.h>

// 数据页表的相关操作实现都放在这个源文件下，如PmEHash申请新的数据页和删除数据页的底层实现

void persist(void* address) {
    pmem_persisit(address, sizeof(kv));
}