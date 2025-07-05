#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <time.h>

/* Port to listen on */
#define PORT 8080

/* Default startup delay in seconds */
#define DELAY_SECONDS 5

/**
 * HTTP 200 OK response with plain text content
 * This is what will be sent back to all clients
 */
const char *response =
"HTTP/1.1 200 OK\r\n"
"Content-Type: text/plain\r\n"
"Connection: close\r\n"
"\r\n"
"Hello from isolated HTTP server!";

/* Volatile flag for signal handling */
volatile sig_atomic_t running = 1;

/**
 * Signal handler for clean shutdown
 */
void handle_signal(int signal) {
    printf("\n[SIGNAL] Received signal %d, shutting down\n", signal);
    running = 0;
}

/**
 * Main function
 */
int main(int argc, char *argv[]) {
    int server_fd, client_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);
    int opt = 1;
    char buffer[1024] = {0};
    int startup_delay = DELAY_SECONDS;

    /* Parse command line arguments for custom delay */
    if (argc > 1) {
        startup_delay = atoi(argv[1]);
        if (startup_delay <= 0) {
            startup_delay = DELAY_SECONDS;
        }
    }

    /* Set up signal handling */
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    printf("\n┌───────────────────────────────────────┐\n");
    printf("│ Isolated HTTP Server - Readiness Check │\n");
    printf("└───────────────────────────────────────┘\n\n");
    printf("[STARTUP] Delaying server startup for %d seconds...\n", startup_delay);

    /* Simulate a slow startup with a delay */
    for (int i = 0; i < startup_delay; i++) {
        printf("[STARTUP] Initializing, please wait... %d/%d\n", i+1, startup_delay);
        sleep(1);
    }

    printf("\n[SERVER] Starting HTTP server on port %d\n", PORT);

    /* Create socket */
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("[ERROR] Failed to create socket");
        return EXIT_FAILURE;
    }

    /* Set socket options */
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("[ERROR] Failed to set socket options");
        return EXIT_FAILURE;
    }

    /* Configure server address */
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    /* Bind socket to port */
    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("[ERROR] Failed to bind socket");
        return EXIT_FAILURE;
    }

    /* Start listening */
    if (listen(server_fd, 3) < 0) {
        perror("[ERROR] Failed to listen");
        return EXIT_FAILURE;
    }

    printf("[SERVER] Ready and listening on port %d\n", PORT);
    printf("[SERVER] Waiting for connections...\n\n");

    /* Configure timeout for select() */
    struct timeval timeout;
    timeout.tv_sec = 1;
    timeout.tv_usec = 0;

    /* Main server loop */
    int request_count = 0;
    while (running) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(server_fd, &read_fds);

        int activity = select(server_fd + 1, &read_fds, NULL, NULL, &timeout);

        if (!running) {
            break;
        }

        if (activity < 0) {
            perror("[ERROR] Select error");
            continue;
        }

        if (activity == 0) {
            /* Timeout, no incoming connections */
            continue;
        }

        if (FD_ISSET(server_fd, &read_fds)) {
            if ((client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len)) < 0) {
                perror("[ERROR] Accept failed");
                continue;
            }

            /* Log client connection */
            char client_ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
            request_count++;
            printf("[REQUEST #%d] Client connected: %s:%d\n",
                   request_count, client_ip, ntohs(client_addr.sin_port));

            /* Read request (not parsing it for this simple example) */
            read(client_fd, buffer, sizeof(buffer) - 1);

            /* Log first line of the request */
            char *first_line = strtok(buffer, "\r\n");
            if (first_line) {
                printf("[REQUEST #%d] %s\n", request_count, first_line);
            }

            /* Send response */
            write(client_fd, response, strlen(response));
            printf("[RESPONSE #%d] Sent 200 OK (Content length: %ld bytes)\n\n",
                   request_count, strlen(response) - strlen("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n"));

            /* Close connection */
            close(client_fd);
        }
    }

    printf("[SERVER] Shutting down gracefully\n");
    close(server_fd);
    return EXIT_SUCCESS;
}
