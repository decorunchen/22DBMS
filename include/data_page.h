#ifndef DATA_PAGE
#define DATA_PAGE

#define DATA_PAGE_SLOT_NUM 16
// use pm_address to locate the data in the page
extern int page_num;

// uncompressed page format design to store the buckets of PmEHash
// one slot stores one bucket of PmEHash

typedef struct data_page {
    // fixed-size record design
    // uncompressed page format
    uint64_t file_name;
    pm_bucket buckets[DATA_PAGE_SLOT_NUM];
    uint16_t bitmap;
    size_t map_len;
} data_page;

extern data_page **pages;

void persist(void* address);
data_page* persistFile();
void recovery(pm_address);

#endif