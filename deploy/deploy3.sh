#!/bin/bash -xe

set -eux
cd $(dirname $0)

git reset --hard && git checkout master && git pull

#mysql service
sudo cp -r ../webapp/config/mysql.conf.d/* /etc/mysql/mysql.conf.d/
sudo systemctl restart mysql.service