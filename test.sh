#!/bin/sh 

DIR=/tmp
FILE_PREFIX=a
BS=1024
COUNT=1048576
#COUNT=1024
SIZE=1g

SRC_FILE=${DIR}/${FILE_PREFIX}_src.txt
DST_FILE=${DIR}/${FILE_PREFIX}_dst.txt
QCOW2_FILE=${DIR}/${FILE_PREFIX}.qcow2
QCOW2_MIRROR_FILE=${DIR}/${FILE_PREFIX}_mirror.qcow2
SRC_MIRROR_FILE=${DIR}/${FILE_PREFIX}_src_mirror.txt

rm -f $SRC_FILE
rm -f $DST_FILE
rm -f $QCOW2_FILE
rm -f $QCOW2_MIRROR_FILE
rm -f $SRC_MIRROR_FILE

echo "======== Test raw2qcow and qcow2raw ==========="
time dd if=/dev/random of=$SRC_FILE bs=$BS count=$COUNT
sleep 1

time bin/qcow2util dd -i $SRC_FILE -o $QCOW2_FILE -O qcow2 -f raw
sleep 1
#exit
time bin/qcow2util dd -o $DST_FILE -i $QCOW2_FILE -O raw -f qcow2
sleep 1

CK_SRC=`cksum $SRC_FILE | awk '{print $1}'`
CK_DST=`cksum $DST_FILE | awk '{print $1}'`

if [ $CK_SRC = $CK_DST ]; then 
   echo "check successfully"
else 
   echo "check failed"
fi

echo "======== Test qcow2qcow  ==========="

time bin/qcow2util dd -i $QCOW2_FILE -o $QCOW2_MIRROR_FILE  -O qcow2
sleep 1

CK_SRC=`cksum $QCOW2_FILE | awk '{print $1}'`
CK_DST=`cksum $QCOW2_MIRROR_FILE | awk '{print $1}'`

if [ $CK_SRC = $CK_DST ]; then
   echo "check successfully"
else 
   echo "check failed"
fi

echo "======== Test raw2raw  ==========="

time bin/qcow2util dd -i $SRC_FILE -o $SRC_MIRROR_FILE  -O raw
sleep 1

CK_SRC=`cksum $SRC_FILE | awk '{print $1}'`
CK_DST=`cksum $SRC_MIRROR_FILE | awk '{print $1}'`

if [ $CK_SRC = $CK_DST ]; then
   echo "check successfully"
else 
   echo "check failed"
fi
