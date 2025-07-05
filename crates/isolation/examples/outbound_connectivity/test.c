#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <signal.h>

#define HOST "example.com"
#define PORT 80
#define BUFFER_SIZE 4096

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
 * Makes an HTTP request to example.com and prints the response
 */
int make_http_request() {
    struct hostent *host;
    struct sockaddr_in server_addr;
    int sock, bytes_received;
    char request[100];
    char response[BUFFER_SIZE];

    /* Get host by name */
    printf("[CLIENT] Resolving hostname %s...\n", HOST);
    fflush(stdout);

    host = gethostbyname(HOST);
    if (host == NULL) {
        herror("[ERROR] Failed to resolve hostname");
        return EXIT_FAILURE;
    }

    printf("[CLIENT] Hostname resolved to %s\n",
           inet_ntoa(*(struct in_addr*)host->h_addr_list[0]));
    fflush(stdout);

    /* Create socket */
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("[ERROR] Failed to create socket");
        return EXIT_FAILURE;
    }

    /* Set up server address structure */
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    server_addr.sin_addr = *((struct in_addr *)host->h_addr_list[0]);
    memset(&(server_addr.sin_zero), 0, 8);

    /* Connect to server */
    printf("[CLIENT] Connecting to %s:%d...\n", HOST, PORT);
    fflush(stdout);

    if (connect(sock, (struct sockaddr *)&server_addr, sizeof(struct sockaddr)) < 0) {
        perror("[ERROR] Failed to connect to server");
        close(sock);
        return EXIT_FAILURE;
    }

    printf("[CLIENT] Connected successfully!\n");
    fflush(stdout);

    /* Prepare HTTP request */
    sprintf(request, "GET / HTTP/1.0\r\nHost: %s\r\n\r\n", HOST);

    /* Send HTTP request */
    printf("[CLIENT] Sending HTTP request...\n");
    fflush(stdout);

    if (send(sock, request, strlen(request), 0) < 0) {
        perror("[ERROR] Failed to send HTTP request");
        close(sock);
        return EXIT_FAILURE;
    }

    printf("[CLIENT] Request sent, waiting for response...\n");
    fflush(stdout);

    /* Receive response */
    memset(response, 0, BUFFER_SIZE);
    bytes_received = recv(sock, response, BUFFER_SIZE - 1, 0);
    if (bytes_received < 0) {
        perror("[ERROR] Failed to receive response");
        close(sock);
        return EXIT_FAILURE;
    }

    /* Display response */
    printf("\n┌───────────────────────────────────────┐\n");
    printf("│ HTTP Response from %s          │\n", HOST);
    printf("└───────────────────────────────────────┘\n\n");

    /* Just print the response headers (first few lines) */
    char *line = strtok(response, "\r\n");
    int line_count = 0;
    while (line != NULL && line_count < 10) {
        printf("%s\n", line);
        line = strtok(NULL, "\r\n");
        line_count++;
    }

    if (bytes_received >= BUFFER_SIZE - 1) {
        printf("...(response truncated)...\n");
    }

    printf("\n[CLIENT] Successfully received %d bytes\n", bytes_received);
    fflush(stdout);

    /* Close socket */
    close(sock);
    return EXIT_SUCCESS;
}

/**
 * Main function
 */
int main(int argc, char *argv[]) {
    printf("\n┌───────────────────────────────────────┐\n");
    printf("│ Outbound Connectivity Test            │\n");
    printf("└───────────────────────────────────────┘\n\n");
    fflush(stdout);

    /* Set up signal handling */
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    int result = make_http_request();

    if (result == EXIT_SUCCESS) {
        printf("[CLIENT] Outbound connectivity test completed successfully!\n");
    } else {
        printf("[CLIENT] Outbound connectivity test failed!\n");
    }

    return result;
}
