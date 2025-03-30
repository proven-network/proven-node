#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int main() {
    printf("Filesystem Isolation Test\n");
    printf("------------------------\n");
    
    // Try to access the allowed file
    printf("Trying to access allowed file:\n");
    int allowed_fd = open("/allowed.txt", O_RDONLY);
    if (allowed_fd >= 0) {
        char buffer[1024];
        ssize_t bytes_read = read(allowed_fd, buffer, sizeof(buffer) - 1);
        if (bytes_read > 0) {
            buffer[bytes_read] = '\0';
            printf("%s\n", buffer);
            printf("SUCCESS: Could read allowed.txt (expected)\n");
        } else {
            printf("ERROR: Could not read content from allowed.txt\n");
            perror("read");
        }
        close(allowed_fd);
    } else {
        printf("ERROR: Could not find allowed.txt\n");
        perror("open");
    }
    
    // Try to access the system root
    printf("\nTrying to access system root:\n");
    DIR *dir = opendir("/");
    if (dir != NULL) {
        struct dirent *entry;
        printf("Files in /: ");
        while ((entry = readdir(dir)) != NULL) {
            printf("%s, ", entry->d_name);
        }
        printf("\n");
        closedir(dir);
    } else {
        printf("ERROR: Could not access system root\n");
        perror("opendir");
    }
    
    // Try to access /etc/passwd
    printf("\nTrying to access /etc/passwd:\n");
    int passwd_fd = open("/etc/passwd", O_RDONLY);
    if (passwd_fd >= 0) {
        char buffer[1024];
        ssize_t bytes_read = read(passwd_fd, buffer, sizeof(buffer) - 1);
        if (bytes_read > 0) {
            buffer[bytes_read] = '\0';
            printf("ISOLATION FAILURE: Read /etc/passwd: %s\n", buffer);
        } else {
            printf("ISOLATION FAILURE: Could open but not read /etc/passwd\n");
        }
        close(passwd_fd);
    } else {
        printf("SUCCESS: Could not access /etc/passwd (expected)\n");
        perror("open");
    }
    
    // Try to create a file in root directory
    printf("\nTrying to create a file in root directory:\n");
    int write_fd = open("/test_write_access.txt", O_WRONLY | O_CREAT, 0644);
    if (write_fd >= 0) {
        printf("ISOLATION FAILURE: Could write to root directory\n");
        close(write_fd);
    } else {
        printf("ISOLATION SUCCESS: Could not write to root directory (expected)\n");
        perror("open");
    }
    
    printf("\nFilesystem isolation test complete\n");
    
    return 0;
} 
