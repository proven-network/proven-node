CC = gcc
CFLAGS = -Wall -Wextra -O2
TARGET = counter

all: $(TARGET)

$(TARGET): counter.c
	$(CC) $(CFLAGS) -o $@ $^

clean:
	rm -f $(TARGET) 
