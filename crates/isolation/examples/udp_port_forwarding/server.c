#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <fcntl.h>
#include <errno.h>

/* Port to listen on */
#define PORT 8080
#define BUFFER_SIZE 1024

/* Volatile flag for signal handling */
volatile sig_atomic_t running = 1;

/**
 * Signal handler for clean shutdown
 */
void handle_signal(int signal) {
    printf("\n[SIGNAL] Received signal %d, shutting down\n", signal);
    fflush(stdout);
    running = 0;
}

/**
 * Main function
 */
int main(int argc, char *argv[]) {
    int sockfd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);
    char buffer[BUFFER_SIZE];
    ssize_t n;

    printf("\n┌───────────────────────────────────────┐\n");
    printf("│ UDP Port Forward Echo Server          │\n");
    printf("└───────────────────────────────────────┘\n\n");
    fflush(stdout);

    printf("[SERVER] Starting UDP echo server on port %d\n", PORT);
    fflush(stdout);

    /* Create socket */
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        perror("[ERROR] Failed to create socket");
        return EXIT_FAILURE;
    }

    /* Configure server address */
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    /* Bind socket to port */
    if (bind(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("[ERROR] Failed to bind socket");
        close(sockfd);
        return EXIT_FAILURE;
    }

    /* Set up socket timeout */
    struct timeval tv;
    tv.tv_sec = 1;  /* 1 second timeout */
    tv.tv_usec = 0;
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        perror("[ERROR] Failed to set socket timeout");
        close(sockfd);
        return EXIT_FAILURE;
    }

    printf("[SERVER] Ready and listening on UDP port %d\n", PORT);
    printf("[SERVER] Waiting for packets...\n\n");
    fflush(stdout);

    /* Set up signal handling */
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    /* Main server loop */
    int request_count = 0;
    while (running) {
        /* Receive packet */
        n = recvfrom(sockfd, buffer, BUFFER_SIZE - 1, 0, (struct sockaddr *)&client_addr, &client_len);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                /* Timeout, just check running flag and continue */
                continue;
            } else if (running) { /* Don't print error if we are shutting down */
                perror("[ERROR] recvfrom failed");
                fflush(stdout);
            }
            continue; /* Continue checking the running flag */
        }

        buffer[n] = '\0'; /* Null-terminate the received data */
        request_count++;

        /* Log client info and received data */
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        printf("[REQUEST #%d] Received %zd bytes from %s:%d\n",
               request_count, n, client_ip, ntohs(client_addr.sin_port));
        printf("[REQUEST #%d] Data: %s\n", request_count, buffer);
        fflush(stdout);

        /* Echo packet back to client */
        ssize_t sent_bytes = sendto(sockfd, buffer, n, 0, (struct sockaddr *)&client_addr, client_len);
        if (sent_bytes < 0) {
            perror("[ERROR] sendto failed");
            fflush(stdout);
        } else {
             printf("[RESPONSE #%d] Echoed %zd bytes back to %s:%d\n\n",
                   request_count, sent_bytes, client_ip, ntohs(client_addr.sin_port));
             fflush(stdout);
        }
    }

    printf("[SERVER] Shutting down gracefully\n");
    fflush(stdout);
    close(sockfd);
    return EXIT_SUCCESS;
}
