#!/bin/sh

LIST="drivers/md/md.h
      drivers/md/raid0.h
      drivers/nvme/host/nvme.h"
BASEDIR=`cd \`dirname $0\`/..; pwd`
TEMPDIR=`mktemp -d`
cd $TEMPDIR || (echo "failed on mktemp -d"; exit 1)

if ! yumdownloader --source kernel; then
  echo "failed on yumdownloader --source kernel"
  rm -rf $TEMPDIR
  exit 1
fi

KERN_SRPM=`ls kernel-*.src.rpm | head -1`
KERN_VERSION=`rpm -q --queryformat='%{version}' -p "${KERN_SRPM}"`
if [ $? -ne 0 ]; then
  echo "failed on rpm -q --queryformat='%{version}' \"${KERN_SRPM}\"";
  rm -rf $TEMPDIR
  exit 1
fi

KERN_RELEASE=`rpm -q --queryformat='%{release}' -p "${KERN_SRPM}"`
if [ $? -ne 0 ]; then
  echo "failed on rpm -q --queryformat='%{release}' \"${KERN_SRPM}\"";
  rm -rf $TEMPDIR
  exit 1
fi

if ! rpm2cpio ${KERN_SRPM} | cpio -idu; then
  echo "failed on rpm2cpio ${KERN_SRPM} | cpio -idu"
  rm -rf $TEMPDIR
  exit 1
fi

if ! tar Jxf linux-${KERN_VERSION}-${KERN_RELEASE}.tar.xz; then
  echo "failed on tar Zxf linux-${KERN_VERSION}-${KERN_RELEASE}.tar.xz"
  rm -rf $TEMPDIR
  exit 1
fi

UPDATED=0
for x in $LIST
do
  diff -q linux-${KERN_VERSION}-${KERN_RELEASE}/$x \
          ${BASEDIR}/kmod/`basename $x | sed 's/\.h/\.rhel7\.h/g'` || UPDATED=1
done

if [ $UPDATED -ne 0 ]; then
  mkdir -p ${BASEDIR}/kmod/${KERN_RELEASE}
  for x in $LIST
  do
    cp linux-${KERN_VERSION}-${KERN_RELEASE}/$x \
       ${BASEDIR}/kmod/${KERN_RELEASE}
    ln -sf ${KERN_RELEASE}/`basename $x` \
           ${BASEDIR}/kmod/`basename $x | sed 's/\.h/\.rhel7\.h/g'`
  done
fi
rm -rf $TEMPDIR

exit 0
