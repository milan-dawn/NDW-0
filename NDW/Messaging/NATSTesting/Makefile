
# NATSTesting/Makefile

DEBUG ?= 0
CC = gcc

ifeq ($(DEBUG), 1)
    CFLAGS = -g -fPIC -I../core/include -I/usr/local/include
else
    CFLAGS = -fPIC -I../core/include -I/usr/local/include
endif

LDFLAGS = -L/usr/local/lib -L../core/build -lcjson -lnats -lpthread -lm -lcoremessaging

TARGETS = NATSTest JSQueueTest JSPullTest

NATSTest_SRC = NATSTest.c
JSQueueTest_SRC = JSQueueTest.c
JSPullTest_SRC = JSPullTest.c

NATSTest_OBJ = $(NATSTest_SRC:.c=.o)
JSQueueTest_OBJ = $(JSQueueTest_SRC:.c=.o)
JSPullTest_OBJ = $(JSPullTest_SRC:.c=.o)

all: $(TARGETS)

NATSTest: $(NATSTest_OBJ)
	$(CC) $(NATSTest_OBJ) -o $@ $(LDFLAGS)

JSQueueTest: $(JSQueueTest_OBJ)
	$(CC) $(JSQueueTest_OBJ) -o $@ $(LDFLAGS)

JSPullTest: $(JSPullTest_OBJ)
	$(CC) $(JSPullTest_OBJ) -o $@ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o $(TARGETS)

