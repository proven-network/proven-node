#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <errno.h>

// Log a message with the specified level
void log_message(const char* level, const char* message) {
    time_t now;
    struct tm* timeinfo;
    char timestamp[20];

    time(&now);
    timeinfo = localtime(&now);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", timeinfo);

    printf("%s: [%s] %s\n", level, timestamp, message);
    fflush(stdout); // Ensure output is flushed immediately
}

// Helper functions for each log level
void log_trace(const char* message) {
    log_message("TRACE", message);
}

void log_debug(const char* message) {
    log_message("DEBUG", message);
}

void log_info(const char* message) {
    log_message("INFO", message);
}

void log_warn(const char* message) {
    log_message("WARN", message);
}

void log_error(const char* message) {
    log_message("ERROR", message);
}

int main(int argc, char* argv[]) {
    // Print a welcome message
    log_info("Log parsing example starting");

    // Add a sleep right at the beginning to ensure we can capture the logs
    sleep(1);

    // Simulate application startup
    log_debug("Initializing application");
    log_trace("Checking environment variables");

    // Emit some logs at different levels
    log_info("Application initialized successfully");
    log_debug("Connected to database");

    // Generate some warning and error logs
    log_warn("Resource usage approaching threshold");

    // Simple log examples at each level with no formatting
    log_info("INFO message 1");
    log_debug("DEBUG message 1");
    log_trace("TRACE message 1");
    log_warn("WARN message 1");
    log_error("ERROR message 1");

    log_info("INFO message 2");
    log_debug("DEBUG message 2");
    log_trace("TRACE message 2");
    log_warn("WARN message 2");
    log_error("ERROR message 2");

    // Final logs
    log_info("Processing complete");
    log_debug("Cleaning up resources");
    log_info("Application shutting down");

    return 0;
}
