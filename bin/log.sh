#!/bin/bash -xe

TIME=`TZ=Asia/Tokyo date +"%I%M%S"`
NOTE="$1"
VERSION=`bash -c "cd /home/isucon; git describe --dirty --always --tags"`

if [ -z "${NOTE}" ];then
	echo "argument not found"
	exit 1
fi

FILENAME="${TIME}_${NOTE}_${VERSION}.log"

sudo mv -v /var/log/nginx/access.log "/var/log/nginx/${FILENAME}"
sudo service nginx reload


