
CC = gcc
CFLAGS = -I/usr/local/include/cjson -I../core/include -Wall -g
LDFLAGS = -rdynamic -L../core/build -lcoremessaging -lcjson

TEST_HARNESS = TestHarness.o

.PHONY: all clean

all: TestTest.out PubSub.out SyncSub.out JSPush.out JSPoll.out GetRespReq.out AsyncQ_NATS.out validate_args.out

TestTest.out: TestTest.c $(TEST_HARNESS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

PubSub.out: PubSub.c $(TEST_HARNESS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

SyncSub.out: SyncSub.c $(TEST_HARNESS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

JSPush.out: JSPush.c $(TEST_HARNESS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

JSPoll.out: JSPoll.c $(TEST_HARNESS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

GetRespReq.out: GetRespReq.c
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

AsyncQ_NATS.out: AsyncQ_NATS.c
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

validate_args.out: validate_args.c
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f TestHarness.o TestTest.out PubSub.out SyncSub.out JSPush.out JSPoll.out GetRespReq.out AsyncQ_NATS.out

