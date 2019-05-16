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
#include "../logging.h"

#include <cstdint>
#include <atomic>

extern pmem_durableds_logger logger;

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

    Allocator()
    {
        logger.pmem_durableds_dlog("empty constructor was called! siseof(DataType)=", sizeof(DataType));
    }    

    // void init(const char* name, uint64_t totalBytes) {


 
    // }

    void reload_mem(const char* name)
    {
        isMemMapped = true;
    	mode_t perms = S_IRUSR | S_IWUSR;

        int current_size = 0;

        int flags = O_RDWR;
        int fd = shm_open(name, flags, perms);
        if(fd == -1) {
            logger.pmem_durableds_elog("ERROR: shm_open with name ",name, " error: ",errno,": ", strerror(errno));
            return;
        }else{
            struct stat sb;
            fstat( fd , &sb );
            current_size = sb.st_size;
        }

        m_pool = (char*)mmap(m_pool, current_size, PROT_READ | PROT_WRITE,
			MAP_SHARED, fd, 0);

        if(MAP_FAILED == m_pool) {
            logger.pmem_durableds_elog("ERROR: mmap error! (",errno,"): ", strerror(errno), ", size=", current_size, ", name=", name, "requested size=", current_size);
            return;
        }
        shm_name = name;
        shm_fd = fd;  

        m_totalBytes = current_size;
        m_ticket = 0;
        logger.pmem_durableds_dlog("reload_mem was completed with name=", name);
    }

    bool load_existing_mem(const char* name, uint64_t threadCount, uint64_t typeSize)
    {
        bool new_mem = true;
        isMemMapped = true;
    	mode_t perms = S_IRUSR | S_IWUSR;

        int current_size = 0;

        int flags = O_RDWR;
        int fd = shm_open(name, flags, perms);
        if(fd == -1) {
            logger.pmem_durableds_elog("ERROR: shm_open with name ",name, " error: ",errno,": ", strerror(errno));
            return false;
        }else{
            struct stat sb;
            fstat( fd , &sb );
            current_size = sb.st_size;
        }

        m_pool = (char*)mmap(NULL, current_size, PROT_READ | PROT_WRITE,
			MAP_SHARED, fd, 0);

        if(MAP_FAILED == m_pool) {
            logger.pmem_durableds_elog("ERROR: mmap error! (",errno,"): ", strerror(errno), ", size=", current_size, ", name=", name, "requested size=", current_size);
            return false;
        }
        shm_name = name;
        shm_fd = fd;  

        m_totalBytes = current_size;
        m_threadCount = threadCount;
        m_typeSize = typeSize;
        m_ticket = 0;
    }

    Allocator(const char* name, uint64_t totalBytes, uint64_t threadCount, uint64_t typeSize)
    {
           
        // init(name, totalBytes);
        logger.pmem_durableds_dlog("Allocator was called with name ", name, ", totalBytes=", totalBytes, ", typeSize=", typeSize);

        isMemMapped = true;
        bool new_mem = true;
    	mode_t perms = S_IRUSR | S_IWUSR;
        int flags = O_RDWR | O_CREAT | O_EXCL;

        int fd = shm_open(name, flags, perms);
        int current_size = totalBytes;

    	if(fd == -1) {
            if(EEXIST == errno){
                logger.pmem_durableds_elog("memory with name ", name, " already exists");
                load_existing_mem(name, threadCount, typeSize);
            }else {
                logger.pmem_durableds_elog("ERROR: shm_open with name ",name, " error: ",errno,": ", strerror(errno));
            }

        }else {
            logger.pmem_durableds_dlog("creating memory with name ", name);
            if(new_mem)  {
                if(ftruncate(fd, current_size) == -1) {
                    logger.pmem_durableds_elog("ERROR: ftruncate error: ", errno, ": ", strerror(errno));
                }            
            }

            m_pool = (char*)mmap(NULL, current_size, PROT_READ | PROT_WRITE,
                MAP_SHARED, fd, 0);

            if(MAP_FAILED == m_pool) {
                logger.pmem_durableds_elog("ERROR: mmap error! (",errno,"): ", strerror(errno), ", size=", current_size, ", name=", name, "requested size=", current_size);
            }
            if(new_mem)
                memset(m_pool, 0, current_size);
            shm_name = name;
            shm_fd = fd;  

            m_totalBytes = totalBytes;
            m_threadCount = threadCount;
            m_typeSize = typeSize;
            m_ticket = 0;
        }
        
        logger.pmem_durableds_dlog("Allocator was completed with name ", name, ", m_pool=", m_pool);
        ASSERT(m_pool, "Memory pool initialization failed.");
    }    

    ~Allocator()
    {
        if(isMemMapped) {
            logger.pmem_durableds_dlog("Desctructor was called with name ", shm_name);
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
                logger.pmem_durableds_elog("ERROR: failed to free the shared memory(", shm_name, ")  ", errno, ": ", strerror(errno));
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

    void print()
    {
        printf("%s\n", shm_name);
        printf("\tm_pool=%p\n", m_pool);
        printf("\tm_totalBytes=%lu\n", m_totalBytes);
        printf("\tm_threadCount=%lu\n", m_threadCount);
        printf("\tm_ticket=%lu\n", m_ticket);
        printf("\tm_typeSize=%lu\n", m_typeSize);
        printf("\tm_base=%p\n", m_base);
        printf("\tm_freeIndex=%lu\n\r", m_freeIndex);        
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
