#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int main() {
    printf("Filesystem Isolation Test (Chroot Environment)\n");
    printf("--------------------------------------------\n\n");
    
    // Try to access the allowed file
    printf("Trying to access allowed file...\n");
    int allowed_fd = open("/allowed.txt", O_RDONLY);
    if (allowed_fd < 0) {
        printf("ERROR: Could not find allowed.txt\n");
        perror("open");
        return 1;
    }

    char buffer[1024];
    ssize_t bytes_read = read(allowed_fd, buffer, sizeof(buffer) - 1);
    if (bytes_read <= 0) {
        printf("ERROR: Could not read content from allowed.txt\n");
        perror("read");
        close(allowed_fd);
        return 1;
    }

    buffer[bytes_read] = '\0';
    printf("File contents: %s\n", buffer);
    printf("SUCCESS: Could read allowed.txt (expected)\n\n");
    close(allowed_fd);
    
    // List contents of root directory (which is our chroot environment)
    printf("Listing contents of chroot environment root:\n");
    DIR *dir = opendir("/");
    if (dir == NULL) {
        printf("ERROR: Could not access root directory\n");
        perror("opendir");
        return 1;
    }

    struct dirent *entry;
    printf("Files in /: ");
    while ((entry = readdir(dir)) != NULL) {
        printf("%s, ", entry->d_name);
    }
    printf("\n\n");
    closedir(dir);
    
    // Try to access /etc/passwd (should still be restricted even in chroot)
    printf("Trying to access /etc/passwd...\n");
    int passwd_fd = open("/etc/passwd", O_RDONLY);
    if (passwd_fd >= 0) {
        char passwd_buffer[1024];
        ssize_t passwd_bytes = read(passwd_fd, passwd_buffer, sizeof(passwd_buffer) - 1);
        if (passwd_bytes > 0) {
            passwd_buffer[passwd_bytes] = '\0';
            printf("ISOLATION FAILURE: Read /etc/passwd: %s\n", passwd_buffer);
            close(passwd_fd);
            return 1;
        } else {
            printf("ISOLATION FAILURE: Could open but not read /etc/passwd\n");
            close(passwd_fd);
            return 1;
        }
    } else {
        printf("SUCCESS: Could not access /etc/passwd (expected)\n");
    }
    
    printf("\nFilesystem isolation test complete\n");
    
    return 0;
} 
