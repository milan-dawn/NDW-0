
set -x

export MEDIA=/media/subrata/BC62-E39C1

export DESTINATION=$MEDIA"/NDW/"
if [ ! -d $DESTINATION ]
then
    echo "Destination Directory<"$DESTINATION"> does NOT exists!"
    exit 1
fi

#find . -name "*.java" -print | cpio -pdvm $DESTINATION 
#find . -name "*.py" -print | cpio -pdvm $DESTINATION
find . -name "*.sh" -print | cpio -pdvm $DESTINATION
find . -name "*.h" -print | cpio -pdvm $DESTINATION
find . -name "*.cpp" -print | cpio -pdvm $DESTINATION
find . -name "*.c" -print | cpio -pdvm $DESTINATION
find . -name "*.JSON" -print | cpio -pdvm $DESTINATION
find . -name "*.txt" -print | cpio -pdvm $DESTINATION
find . -name "*.go" -print | cpio -pdvm $DESTINATION
find . -name "*Makefile" -print | cpio -pdvm $DESTINATION
find . -name "*.xls" -print | cpio -pdvm $DESTINATION
find . -name "*.docx" -print | cpio -pdvm $DESTINATION
find . -name "*.pdf" -print | cpio -pdvm $DESTINATION
find . -name "*.mod" -print | cpio -pdvm $DESTINATION

sync $MEDIA
sync
echo "DESTINATION= $DESTINATION"
echo


