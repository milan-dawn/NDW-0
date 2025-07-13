
#
# Set LD_LIBRARY_PATH to use libcoremassing.so
#
export LD_LIBRARY_PATH=$NDW_MSG_ROOT/core/build:$LD_LIBRARY_PATH

echo "env.sh LD_LIBRARY_PATH = $LD_LIBRARY_PATH"

#export NDW_VERBOSE="2"
export NDW_VERBOSE="3"

# Default Parameters to run (stress) test code.
export NDW_DEBUG_MSG_HEADERS="2"
export NDW_APP_CONFIG_FILE="./Registry.JSON"
export NDW_APP_DOMAINS="DomainA"
export NDW_APP_ID=777
export NDW_CAPTURE_LATENCY=1

