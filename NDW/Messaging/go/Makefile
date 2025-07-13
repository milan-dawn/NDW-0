# Define your app directories
APPS = ndw test_utils pubsub testharness

# Output binary name will match folder name
#GOFLAGS = -gcflags=all=-N -l

# CGO flags (adjust as needed)
CGO_CFLAGS := -I$(NDW_MSG_ROOT)/core/include
CGO_LDFLAGS := -L$(NDW_MSG_ROOT)/core/build -lcoremessaging

export CGO_CFLAGS
export CGO_LDFLAGS

# Automatically tidy modules before build
all: tidy $(APPS)

tidy:
	@echo "📦 Running go mod tidy..."
	@go mod tidy

# Rule for each app
$(APPS):
	@echo "🔧 Building $@..."
	@cd $@ && go build $(GOFLAGS) -o $@

clean:
	@echo "🧹 Cleaning executables..."
	@for app in $(APPS); do rm -f $$app/$$app; done

realclean: clean
	@echo "🧼 Cleaning Go build cache and intermediates..."
	@go clean -cache -modcache -i -r

.PHONY: all clean realclean tidy $(APPS)

