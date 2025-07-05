#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <dirent.h>

/* Path to the counter file */
#define COUNTER_FILE "/data/counter.txt"
#define MAX_VALUE_LEN 32

/**
 * Function to list directory contents
 */
void list_directory(const char* path) {
    DIR *dir;
    struct dirent *entry;

    printf("[DEBUG] Listing contents of directory: %s\n", path);
    dir = opendir(path);
    if (dir == NULL) {
        printf("[ERROR] Failed to open directory %s: %s\n", path, strerror(errno));
        return;
    }

    while ((entry = readdir(dir)) != NULL) {
        printf("[DEBUG] Found: %s\n", entry->d_name);
    }

    closedir(dir);
}

/**
 * Function to check if a file exists and is accessible
 */
void check_file(const char* path) {
    struct stat st;
    printf("[DEBUG] Checking file: %s\n", path);
    if (stat(path, &st) == 0) {
        printf("[DEBUG] File exists, size: %ld bytes, permissions: %o\n",
               st.st_size, st.st_mode & 0777);
    } else {
        printf("[ERROR] File check failed: %s\n", strerror(errno));
    }
}

/**
 * Read the current counter value from file
 */
int read_counter(void) {
    FILE *fp;
    char value_str[MAX_VALUE_LEN];
    int value = 0;

    printf("[DEBUG] Attempting to read counter from: %s\n", COUNTER_FILE);
    check_file(COUNTER_FILE);

    fp = fopen(COUNTER_FILE, "r");
    if (fp == NULL) {
        if (errno == ENOENT) {
            printf("[INFO] Counter file does not exist, creating with initial value 0\n");
            fp = fopen(COUNTER_FILE, "w");
            if (fp == NULL) {
                printf("[ERROR] Failed to create counter file: %s\n", strerror(errno));
                return 0;
            }
            fprintf(fp, "0\n");
            fclose(fp);
            return 0;
        } else {
            printf("[ERROR] Failed to open counter file: %s\n", strerror(errno));
            return 0;
        }
    }

    if (fgets(value_str, sizeof(value_str), fp) != NULL) {
        value = atoi(value_str);
        printf("[DEBUG] Read counter value: %d\n", value);
    }

    fclose(fp);
    return value;
}

/**
 * Write the counter value to file
 */
int write_counter(int value) {
    FILE *fp;

    printf("[DEBUG] Writing counter value %d to: %s\n", value, COUNTER_FILE);
    fp = fopen(COUNTER_FILE, "w");
    if (fp == NULL) {
        printf("[ERROR] Failed to open counter file for writing: %s\n", strerror(errno));
        return -1;
    }

    fprintf(fp, "%d\n", value);
    fclose(fp);
    printf("[DEBUG] Successfully wrote counter value\n");
    return 0;
}

/**
 * Main function
 */
int main(int argc, char *argv[]) {
    printf("[INFO] Isolated Counter Starting\n");
    printf("[DEBUG] Current working directory: %s\n", getcwd(NULL, 0));

    // List contents of root directory
    printf("\n[DEBUG] Root directory contents:\n");
    list_directory("/");

    // List contents of /data directory
    printf("\n[DEBUG] /data directory contents:\n");
    list_directory("/data");

    /* Read current counter value */
    int counter = read_counter();
    printf("[INFO] Current counter value: %d\n", counter);

    /* Increment counter */
    counter++;
    printf("[INFO] Counter incremented to: %d\n", counter);

    /* Write new value */
    if (write_counter(counter) < 0) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
