CC = gcc
CFLAGS = -Wall -Wextra

TARGET = server
SRC = server.c

all:
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET)

clean:
	rm -f $(TARGET)
