
set -x
#make -B -f Makefile DEBUG=1

make -B -f Makefile DEBUG=1 >see 2>&1
vi + see

ls -ltr *.so
set +x
echo
echo
