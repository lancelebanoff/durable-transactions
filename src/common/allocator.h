#ifndef ALLOCATOR_H
#define ALLOCATOR_H




#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <malloc.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h> /* For O_* constants */
#include <string.h>
#include <errno.h>

#include "assert.h"

#include <cstdint>
#include <atomic>

template<typename DataType>
class Allocator 
{
public:
    Allocator(uint64_t totalBytes, uint64_t threadCount, uint64_t typeSize)
    {
        isMemMapped = false;
        m_totalBytes = totalBytes;
        m_threadCount = threadCount;
        m_typeSize = typeSize;
        m_ticket = 0;
        m_pool = (char*)memalign(m_typeSize, totalBytes);

        // pmem_map_file(PATH, PMEM_LEN, PMEM_FILE_CREATE,
        //     0666, &mapped_len, &is_pmem)
        
    //     m_pool = mmap(NULL, m_typeSize, PROT_READ | PROT_WRITE,
    // MAP_SHARED, fd, 0);


        ASSERT(m_pool, "Memory pool initialization failed.");
    }

    // void init(const char* name, uint64_t totalBytes) {


 
    // }

    Allocator(const char* name, uint64_t totalBytes, uint64_t threadCount, uint64_t typeSize)
    {
           
        // init(name, totalBytes);
        printf("Allocator was called with name %s\n\r", name);

        isMemMapped = true;
        bool new_mem = true;
    	mode_t perms = S_IRUSR | S_IWUSR;
        int flags = O_RDWR | O_CREAT | O_EXCL;

        int fd = shm_open(name, flags, perms);
        int current_size = totalBytes;

    	if(fd == -1) {
            if(EEXIST == errno){
                new_mem = false;
                flags = O_RDWR;
                fd = shm_open(name, flags, perms);
                if(fd == -1) {
                    printf("ERROR: shm_open error: %d: %s\n\r", errno, strerror(errno));
                }else{
                    struct stat sb;
                    fstat( fd , &sb );
                    current_size = sb.st_size;
			    }
            }else {
                printf("ERROR: shm_open error: %d: %s\n\r", errno, strerror(errno));
            }

        }
        if(new_mem)  {
            if(ftruncate(fd, current_size) == -1) {
                printf("ERROR: ftruncate error: %d: %s\n\r", errno, strerror(errno));
            }            
        }

        m_pool = (char*)mmap(NULL, current_size, PROT_READ | PROT_WRITE,
			MAP_SHARED, fd, 0);

        if(MAP_FAILED == m_pool) {
            printf("ERROR: mmap error! (%d): %s, size=%ld, name=%s, requested size=%d\n\r",errno,strerror(errno), current_size, name, current_size);
        }
        if(new_mem)
            memset(m_pool, 0, current_size);
        shm_name = name;
        shm_fd = fd;  

        m_totalBytes = totalBytes;
        m_threadCount = threadCount;
        m_typeSize = typeSize;
        m_ticket = 0;
        printf("Allocator was completed with name %s\n\r", name);

        // m_pool = (char*)memalign(m_typeSize, totalBytes);

        // pmem_map_file(PATH, PMEM_LEN, PMEM_FILE_CREATE,
        //     0666, &mapped_len, &is_pmem)
        
    //     m_pool = mmap(NULL, m_typeSize, PROT_READ | PROT_WRITE,
    // MAP_SHARED, fd, 0);


        ASSERT(m_pool, "Memory pool initialization failed.");
    }    

    ~Allocator()
    {
        if(isMemMapped) {
            printf("Desctructor was called with name %s\n\r", shm_name);
            // int rc = munmap(m_pool, m_totalBytes);
            // if(rc) {
            //     printf("ERROR: failed to free the shared memory(%s)  %d: %s\n\r", shm_name, errno, strerror(errno));
            // }
            // rc = shm_unlink(shm_name);
            // if(rc){
            //     printf("ERROR: failed to free the shared memory(%s)  %d: %s\n\r", shm_name, errno, strerror(errno));
            // }
            int rc = close(shm_fd);
            if(rc){
                printf("ERROR: failed to free the shared memory(%s)  %d: %s\n\r", shm_name, errno, strerror(errno));
            }            
        }else {
            free(m_pool);
        }
     
        
    }

    //Every thread need to call init once before any allocation
    void Init()
    {
        uint64_t threadId = __sync_fetch_and_add(&m_ticket, 1);
        ASSERT(threadId < m_threadCount, "ThreadId specified should be smaller than thread count.");

        m_base = m_pool + threadId * m_totalBytes / m_threadCount;

        m_freeIndex = 0;
    }

    void Uninit()
    { }

    DataType* Alloc()
    {
        ASSERT(m_freeIndex < m_totalBytes / m_threadCount, "out of capacity.");
        char* ret = m_base + m_freeIndex;
        m_freeIndex += m_typeSize;
        return (DataType*)ret;
    }

private:
    bool isMemMapped;
    const char* shm_name;
    int shm_fd;

    char* m_pool;
    uint64_t m_totalBytes;      //number of elements T in the pool
    uint64_t m_threadCount;
    uint64_t m_ticket;
    uint64_t m_typeSize;

    static __thread char* m_base;
    static __thread uint64_t m_freeIndex;
};

template<typename T>
__thread char* Allocator<T>::m_base;

template<typename T>
__thread uint64_t Allocator<T>::m_freeIndex;

#endif /* end of include guard: ALLOCATOR_H */
