
set -x

echo "NDW_MSG_ROOT = $NDW_MSG_ROOT"

C_SHARED_LIB=$NDW_MSG_ROOT/core/build/libcoremessaging.so
if test -f $C_SHARED_LIB; then
    ls -ltr $C_SHARED_LIB
else
    echo  "*** ERROR *** C_SHARED_LIB: $C_SHARED_LIB does NOT exists!"
    exit 0 
fi

export LD_LIBRARY_PATH=$NDW_MSG_ROOT/core/build:$LD_LIBRARY_PATH
echo "LD_LIBRARY_PATH = "$LD_LIBRARY_PATH"  "

export PATH=/usr/local/go/bin:$PATH

export NDW_VERBOSE="3"

# Default Parameters to run (stress) test code.
export NDW_DEBUG_MSG_HEADERS="2"
export NDW_APP_CONFIG_FILE="$NDW_MSG_ROOT/go/GO_NATS_Registry.JSON"
export NDW_APP_DOMAINS="DomainA"
export NDW_APP_ID=777
export NDW_CAPTURE_LATENCY=1

export CGO_CFLAGS="-I$NDW_MSG_ROOT/core/include"
export CGO_LDFLAGS="-L$NDW_MSG_ROOT/core/build -lcoremessaging"

