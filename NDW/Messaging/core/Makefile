ifeq ($(DEBUG), 1)
    CFLAGS = -g -Wall -Wextra -fPIC -Iinclude -I/usr/local/include
else
    CFLAGS = -O2 -Wall -Wextra -fPIC -Iinclude -I/usr/local/include
endif

LDFLAGS = -shared -L/usr/local/lib -lcjson -lnats -lpthread -lm -ldl
TARGET = build/libcoremessaging.so

SRC = $(wildcard src/*.c)
OBJ = $(patsubst src/%.c,build/%.o,$(SRC))

BUILD_MODE_FILE = build/build_mode

.PHONY: all check_mode clean

all:
	@$(MAKE) check_mode
	@$(MAKE) $(TARGET)

check_mode:
	@if [ -d build ]; then \
		if [ -f $(BUILD_MODE_FILE) ]; then \
			if ! grep -q "DEBUG=$(DEBUG)" $(BUILD_MODE_FILE); then \
				echo "Build mode changed. Cleaning..."; \
				$(MAKE) clean; \
			fi \
		fi \
	else \
		mkdir build; \
	fi
	@echo "DEBUG=$(DEBUG)" > $(BUILD_MODE_FILE)

$(TARGET): $(OBJ)
	$(CC) $(OBJ) -o $@ $(LDFLAGS)

build/%.o: src/%.c
	@if [ ! -d build ]; then mkdir build; fi
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf build

