#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/resource.h>

#define MB (1024 * 1024)

// Function to get current memory usage
void print_memory_info() {
    // Try to get memory info from /proc/self/status
    FILE *status = fopen("/proc/self/status", "r");
    if (status != NULL) {
        char line[128];
        while (fgets(line, 128, status) != NULL) {
            if (strncmp(line, "VmSize:", 7) == 0 || 
                strncmp(line, "VmRSS:", 6) == 0 || 
                strncmp(line, "VmPeak:", 7) == 0) {
                printf("%s", line);
            }
        }
        fclose(status);
    } else {
        // If /proc is not available, use getrusage as fallback
        struct rusage usage;
        if (getrusage(RUSAGE_SELF, &usage) == 0) {
            printf("Memory usage: %ld KB\n", usage.ru_maxrss);
        } else {
            printf("Could not determine memory usage\n");
        }
    }
}

// Memory allocation test
int main() {
    printf("Memory Isolation Test (PID=%d)\n", getpid());
    printf("--------------------\n");
    
    printf("Initial memory usage:\n");
    print_memory_info();
    
    // Store allocated memory chunks
    void *memory_chunks[100] = {NULL};
    int total_allocated = 0;
    
    // Try to allocate memory in chunks
    for (int i = 0; i < 100; i++) {
        int chunk_size = 5 * MB; // 5MB chunks
        
        printf("\nTrying to allocate %dMB chunk #%d...\n", chunk_size / MB, i + 1);
        fflush(stdout);
        
        // Allocate memory
        void *chunk = malloc(chunk_size);
        if (chunk == NULL) {
            printf("ALLOCATION FAILED: Could not allocate %dMB for chunk #%d\n", chunk_size / MB, i + 1);
            printf("This is expected if memory limits are enforced correctly\n");
            fflush(stdout);
            break;
        }
        
        // Actually use the memory to ensure it's allocated
        memset(chunk, 1, chunk_size);
        memory_chunks[i] = chunk;
        total_allocated += chunk_size;
        
        printf("SUCCESS: Allocated %dMB chunk #%d\n", chunk_size / MB, i + 1);
        printf("Total allocated: %dMB\n", total_allocated / MB);
        printf("Current memory usage:\n");
        print_memory_info();
        fflush(stdout);
        
        // Sleep briefly to allow monitoring
        sleep(1);
    }
    
    printf("\nMemory allocation test completed\n");
    printf("Final memory usage:\n");
    print_memory_info();
    fflush(stdout);
    
    // Clean up allocated memory
    for (int i = 0; i < 100; i++) {
        if (memory_chunks[i] != NULL) {
            free(memory_chunks[i]);
        }
    }
    
    printf("\nCleaned up memory\n");
    printf("Final memory usage after cleanup:\n");
    print_memory_info();
    fflush(stdout);
    
    // Sleep for a moment to allow monitoring
    printf("\nSleeping for 10 seconds before exit...\n");
    fflush(stdout);
    sleep(10);
    
    printf("Memory test complete\n");
    fflush(stdout);
    
    return 0;
} 
