#ifndef DATA_PAGE
#define DATA_PAGE

#define DATA_PAGE_SLOT_NUM 16
#define PAGE_NUM 100000
// use pm_address to locate the data in the page
int page_num = 0;
data_page pages[PAGE_NUM];
int ISPAGE[PAGE_NUM] = {0};

// uncompressed page format design to store the buckets of PmEHash
// one slot stores one bucket of PmEHash
typedef struct data_page {
    // fixed-size record design
    // uncompressed page format
    uint64_t file_name;
    pm_bucket buckets[DATA_PAGE_SLOT_NUM];
} data_page;

void persist(void* address);
data_page* persistFile();
void recovery(uint64_t index, uint64_t pnum);

#endif